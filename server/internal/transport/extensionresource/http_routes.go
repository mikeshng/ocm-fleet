package extensionresource

import (
	"net/http"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/transport/dynamicapi"
)

// httpService serves one activated extension service over HTTP. The
// collection-level methods answer the collection address; the named
// methods answer one record by its resource name.
type httpService struct {
	svc  *RegisteredService
	conn *grpc.ClientConn
}

func (s *httpService) create(w http.ResponseWriter, r *http.Request) {
	if s.svc.Descriptors.CreateRequest == nil {
		http.NotFound(w, r)
		return
	}
	handleHTTPCreate(w, r, s.conn, s.svc)
}

func (s *httpService) list(w http.ResponseWriter, r *http.Request) {
	handleHTTPList(w, r, s.conn, s.svc)
}

func (s *httpService) get(w http.ResponseWriter, r *http.Request, name string) {
	handleHTTPGet(w, r, s.conn, s.svc, name)
}

func (s *httpService) delete(w http.ResponseWriter, r *http.Request, name string) {
	if s.svc.Descriptors.DeleteRequest == nil {
		http.NotFound(w, r)
		return
	}
	handleHTTPDelete(w, r, s.conn, s.svc, name)
}

func (s *httpService) resume(w http.ResponseWriter, r *http.Request, name string) {
	if s.svc.Descriptors.ResumeRequest == nil {
		http.NotFound(w, r)
		return
	}
	handleHTTPResume(w, r, s.conn, s.svc, name)
}

// routeTable dispatches the URLs below one service root,
// /apis/{service}/{version}, to the extension services registered for
// its collections.
//
// A URL is the service root followed by either a collection
// (/{collection}, List or Create) or a resource name
// (/{resource name}, Get or Delete): the design's
// /apis/{service}/{version}/{resource path}. The collection that serves
// a resource name is the collection the name itself carries, so
// /apis/ocm.open-cluster-management.io/v1alpha/hubs/{hub}/managedClusters/{name}
// reaches the managed-clusters service without repeating or guessing at
// a collection segment. Mounting each collection at its own prefix
// instead would give /hubs/{hub}/... to the hubs collection, because a
// subtree is longer than the service root.
type routeTable struct {
	root string

	mu       sync.RWMutex
	services map[string]*httpService // keyed by collection id
}

func newRouteTable(root string) *routeTable {
	return &routeTable{root: root, services: make(map[string]*httpService)}
}

// set adds or atomically replaces the service serving collection.
func (t *routeTable) set(collection string, s *httpService) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.services[collection] = s
}

// remove drops the service serving collection. The other collections of
// the same root keep serving.
func (t *routeTable) remove(collection string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.services, collection)
}

// empty reports whether no collection is registered under the root.
func (t *routeTable) empty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.services) == 0
}

func (t *routeTable) service(collection domain.CollectionID) *httpService {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.services[string(collection)]
}

func (t *routeTable) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, t.root), "/")
	if rest == "" {
		http.NotFound(w, r)
		return
	}

	if !strings.Contains(rest, "/") {
		// The collection itself: List (GET) or Create (POST, AIP-133).
		s := t.service(domain.CollectionID(rest))
		if s == nil {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.list(w, r)
		case http.MethodPost:
			s.create(w, r)
		default:
			http.NotFound(w, r)
		}
		return
	}

	switch r.Method {
	case http.MethodGet:
		t.serveNamed(w, r, rest, (*httpService).get)
	case http.MethodDelete:
		t.serveNamed(w, r, rest, (*httpService).delete)
	case http.MethodPost:
		name, method := cutHTTPMethod(rest)
		if method != "resume" {
			http.NotFound(w, r)
			return
		}
		t.serveNamed(w, r, name, (*httpService).resume)
	default:
		http.NotFound(w, r)
	}
}

// serveNamed parses a resource name and calls op on the service whose
// collection the name carries. A malformed name is refused as invalid
// argument instead of being looked up.
func (t *routeTable) serveNamed(
	w http.ResponseWriter, r *http.Request, resourcePath string,
	op func(*httpService, http.ResponseWriter, *http.Request, string),
) {
	name, err := domain.ParseResourceName(resourcePath)
	if err != nil {
		dynamicapi.HTTPError(w, codes.InvalidArgument, err.Error())
		return
	}
	s := t.service(name.CollectionID())
	if s == nil {
		http.NotFound(w, r)
		return
	}
	op(s, w, r, string(name))
}

// cutHTTPMethod splits the custom method (AIP-136) written after a
// resource path: "clusters/c1:resume" becomes ("clusters/c1", "resume").
// A colon outside the last path segment belongs to the path, not to a
// method.
func cutHTTPMethod(resourcePath string) (name, method string) {
	i := strings.LastIndex(resourcePath, ":")
	if i < 0 || strings.Contains(resourcePath[i:], "/") {
		return resourcePath, ""
	}
	return resourcePath[:i], resourcePath[i+1:]
}
