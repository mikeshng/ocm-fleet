package extensionresource

import (
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/stolostron/ocm-fleet/server/internal/transport/dynamicapi"
)

// RegisterHTTP registers REST/JSON routes for the dynamic service on the
// given HTTP mux. Routes address this collection at its canonical
// /apis/{service}/{version}/{collection} prefix:
//
//	POST   {prefix}                 -> Create
//	GET    {prefix}/{id}            -> Get
//	GET    {prefix}                 -> List
//	DELETE {prefix}/{id}            -> Delete
//	POST   {prefix}/{id}:resume     -> Resume
//
// A record whose name carries parent collections (for example
// "hubs/{hub}/managedClusters/{name}") is read at its resource name,
// /apis/{service}/{version}/{resource name}; the activator mounts every
// collection of a service under that root (see [routeTable]).
//
// The conn is a gRPC client connection to the server hosting the service.
//
// For dynamic (hot-swappable) registration, prefer [dynamicapi.DynamicHTTPMux]
// which uses handler indirection to support atomic replace and deregister.
func RegisterHTTP(mux *http.ServeMux, svc *RegisteredService, conn *grpc.ClientConn) {
	prefix := svc.Config.CanonicalHTTPPrefix()
	handler := BuildHTTPHandler(svc, conn, prefix)

	// Register both the exact path and the subtree pattern so that
	// /v1/{collection} (list, create) and /v1/{collection}/{id} (get,
	// delete) are both routed here instead of falling through to the
	// grpc-gateway catch-all on /v1/.
	mux.HandleFunc(prefix, handler)
	mux.HandleFunc(prefix+"/", handler)
}

// BuildHTTPHandler creates the HTTP handler function for one collection
// of a managed resource service without registering it on any mux. The
// path below prefix is a bare resource id, so the handler addresses the
// record "collection/id". Nested resource names are served by the
// service-root route table, not by this handler.
func BuildHTTPHandler(svc *RegisteredService, conn *grpc.ClientConn, prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, prefix)

		switch {
		case r.Method == http.MethodPost && (rest == "" || rest == "/"):
			if svc.Descriptors.CreateRequest == nil {
				http.NotFound(w, r)
				return
			}
			handleHTTPCreate(w, r, conn, svc)
		case r.Method == http.MethodPost && strings.HasSuffix(rest, ":resume"):
			if svc.Descriptors.ResumeRequest == nil {
				http.NotFound(w, r)
				return
			}
			id := strings.TrimPrefix(rest, "/")
			id = strings.TrimSuffix(id, ":resume")
			if !requireBareID(w, id) {
				return
			}
			handleHTTPResume(w, r, conn, svc, svc.Config.Collection()+id)
		case r.Method == http.MethodGet && (rest == "" || rest == "/"):
			handleHTTPList(w, r, conn, svc)
		case r.Method == http.MethodGet && len(rest) > 1:
			id := strings.TrimPrefix(rest, "/")
			if !requireBareID(w, id) {
				return
			}
			handleHTTPGet(w, r, conn, svc, svc.Config.Collection()+id)
		case r.Method == http.MethodDelete && len(rest) > 1:
			if svc.Descriptors.DeleteRequest == nil {
				http.NotFound(w, r)
				return
			}
			id := strings.TrimPrefix(rest, "/")
			if !requireBareID(w, id) {
				return
			}
			handleHTTPDelete(w, r, conn, svc, svc.Config.Collection()+id)
		default:
			http.NotFound(w, r)
		}
	}
}

// requireBareID reports whether id addresses one record directly under
// this collection and, when it does not, answers the request as invalid.
// A resource name — a slash-separated path such as
// "hubs/{hub}/managedClusters/{name}" — is read at its own URL below the
// service root, never appended to another collection.
func requireBareID(w http.ResponseWriter, id string) bool {
	switch {
	case id == "":
		dynamicapi.HTTPError(w, codes.InvalidArgument, "resource id is required")
	case strings.Contains(id, "/"):
		dynamicapi.HTTPError(w, codes.InvalidArgument,
			"expected a resource id, got a resource name: "+id)
	default:
		return true
	}
	return false
}

func handleHTTPCreate(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		dynamicapi.HTTPError(w, codes.InvalidArgument, "read body: "+err.Error())
		return
	}

	// Parse the request body as the resource message (spec + optional fields).
	resource := dynamicpb.NewMessage(svc.Descriptors.Resource)
	if err := protojson.Unmarshal(body, resource); err != nil {
		dynamicapi.HTTPError(w, codes.InvalidArgument, "parse body: "+err.Error())
		return
	}

	// Extract the ID from the query parameter (AIP pattern: ?{singular}_id=xxx)
	lower := strings.ToLower(svc.Config.Singular[:1]) + svc.Config.Singular[1:]
	id := r.URL.Query().Get(lower + "_id")
	if id == "" {
		dynamicapi.HTTPError(w, codes.InvalidArgument, lower+"_id query parameter is required")
		return
	}

	createReq := dynamicpb.NewMessage(svc.Descriptors.CreateRequest)
	idField := svc.Descriptors.CreateRequest.Fields().ByNumber(1)
	resourceField := svc.Descriptors.CreateRequest.Fields().ByNumber(2)
	createReq.Set(idField, stringValue(id))
	createReq.Set(resourceField, messageValue(resource))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/Create" + svc.Config.Singular
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, createReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func handleHTTPGet(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, name string) {
	getReq := dynamicpb.NewMessage(svc.Descriptors.GetRequest)
	nameField := svc.Descriptors.GetRequest.Fields().ByName("name")
	getReq.Set(nameField, stringValue(name))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/Get" + svc.Config.Singular
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, getReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func handleHTTPList(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService) {
	listReq := dynamicpb.NewMessage(svc.Descriptors.ListRequest)

	// Query parameters use the API's camelCase spelling, as the
	// transcoded routes do (AIP-127). A snake_case spelling is refused
	// rather than ignored: a client that sent one would otherwise get a
	// silently unpaged answer.
	for _, name := range []string{"page_size", "page_token"} {
		if r.URL.Query().Has(name) {
			dynamicapi.HTTPError(w, codes.InvalidArgument, "unknown query parameter "+name+": list takes pageSize and pageToken")
			return
		}
	}
	if v := r.URL.Query().Get("pageSize"); v != "" {
		if field := svc.Descriptors.ListRequest.Fields().ByName("page_size"); field != nil {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				dynamicapi.HTTPError(w, codes.InvalidArgument, "invalid pageSize: "+err.Error())
				return
			}
			listReq.Set(field, protoreflect.ValueOfInt32(int32(n)))
		}
	}
	if v := r.URL.Query().Get("pageToken"); v != "" {
		if field := svc.Descriptors.ListRequest.Fields().ByName("page_token"); field != nil {
			listReq.Set(field, protoreflect.ValueOfString(v))
		}
	}

	resp := dynamicpb.NewMessage(svc.Descriptors.ListResponse)
	method := "/" + svc.Config.GRPCServiceName() + "/List" + svc.Config.Plural
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, listReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func handleHTTPDelete(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, name string) {
	deleteReq := dynamicpb.NewMessage(svc.Descriptors.DeleteRequest)
	nameField := svc.Descriptors.DeleteRequest.Fields().ByName("name")
	deleteReq.Set(nameField, stringValue(name))

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/Delete" + svc.Config.Singular
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, deleteReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func handleHTTPResume(w http.ResponseWriter, r *http.Request, conn *grpc.ClientConn, svc *RegisteredService, name string) {
	resumeReq := dynamicpb.NewMessage(svc.Descriptors.ResumeRequest)
	nameField := svc.Descriptors.ResumeRequest.Fields().ByName("name")
	resumeReq.Set(nameField, stringValue(name))

	// Parse optional request body for user_signature / valid_until.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		dynamicapi.HTTPError(w, codes.InvalidArgument, "read body: "+err.Error())
		return
	}
	if len(body) > 0 {
		if err := protojson.Unmarshal(body, resumeReq); err != nil {
			dynamicapi.HTTPError(w, codes.InvalidArgument, "parse body: "+err.Error())
			return
		}
		// Re-set name since body unmarshal might have cleared it.
		resumeReq.Set(nameField, stringValue(name))
	}

	resp := dynamicpb.NewMessage(svc.Descriptors.Resource)
	method := "/" + svc.Config.GRPCServiceName() + "/Resume" + svc.Config.Singular
	if err := conn.Invoke(dynamicapi.GRPCContext(r), method, resumeReq, resp); err != nil {
		dynamicapi.GRPCHTTPError(w, err)
		return
	}

	dynamicapi.WriteJSON(w, http.StatusOK, resp)
}

func stringValue(s string) protoreflect.Value {
	return protoreflect.ValueOfString(s)
}

func messageValue(m *dynamicpb.Message) protoreflect.Value {
	return protoreflect.ValueOfMessage(m)
}
