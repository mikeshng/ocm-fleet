package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func objectKey(obj map[string]any) string {
	meta, _ := obj["metadata"].(map[string]any)
	kind, _ := obj["kind"].(string)
	name, _ := meta["name"].(string)
	return kind + "/" + name
}

func objectKeys(objs []map[string]any) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, objectKey(o))
	}
	return out
}

func agentOptions(profile string) AccessPieceOptions {
	return AccessPieceOptions{
		Profile:        profile,
		Mode:           AgentModeJob,
		Image:          "registry.test/enrollment-agent:1",
		ServerURL:      "https://fleet.test:8085",
		ServerCA:       []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
		ConnectAddress: "fleet-internal:8085",
		BundleToken:    "eb-0123456789ab.s3cr3t",
		TokenLifetime:  time.Hour,
	}
}

func TestBuildAccessPiece_Profiles(t *testing.T) {
	common := []string{
		"Namespace/ocm-fleet-system", "ServiceAccount/ocm-fleet-access",
		"Role/ocm-fleet-access-token", "RoleBinding/ocm-fleet-access-token",
	}
	cases := []struct {
		profile, clusterRole string
		want                 []string
		roleRef              string
	}{
		{AccessProfileClusterAdmin, "", []string{"ClusterRoleBinding/ocm-fleet-access-cluster-admin"}, "cluster-admin"},
		{AccessProfileWorkloads, "", []string{"ClusterRoleBinding/ocm-fleet-access-edit", "ClusterRole/ocm-fleet-access-cluster", "ClusterRoleBinding/ocm-fleet-access-cluster"}, "edit"},
		{AccessProfileReadOnly, "", []string{"ClusterRoleBinding/ocm-fleet-access-view", "ClusterRole/ocm-fleet-access-cluster", "ClusterRoleBinding/ocm-fleet-access-cluster"}, "view"},
		{AccessProfileCustom, "my-role", []string{"ClusterRoleBinding/ocm-fleet-access-custom"}, "my-role"},
		{"", "", []string{"ClusterRoleBinding/ocm-fleet-access-edit", "ClusterRole/ocm-fleet-access-cluster", "ClusterRoleBinding/ocm-fleet-access-cluster"}, "edit"},
	}
	for _, tc := range cases {
		t.Run("profile "+tc.profile, func(t *testing.T) {
			opts := agentOptions(tc.profile)
			opts.ClusterRole = tc.clusterRole
			piece, err := BuildAccessPiece(opts)
			if err != nil {
				t.Fatalf("BuildAccessPiece: %v", err)
			}
			got := objectKeys(piece.Access)
			want := append(append([]string{}, common...), tc.want...)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("access objects = %v, want %v", got, want)
			}
			// The first cluster role binding carries the profile's role
			// and binds exactly the access account.
			binding := piece.Access[4]
			roleRef, _ := binding["roleRef"].(map[string]any)
			if roleRef["name"] != tc.roleRef || roleRef["kind"] != "ClusterRole" {
				t.Errorf("roleRef = %v, want ClusterRole %q", roleRef, tc.roleRef)
			}
			subjects, _ := binding["subjects"].([]any)
			if len(subjects) != 1 {
				t.Fatalf("subjects = %v", subjects)
			}
			subject, _ := subjects[0].(map[string]any)
			if subject["kind"] != "ServiceAccount" || subject["name"] != AccessServiceAccount || subject["namespace"] != AccessNamespace {
				t.Errorf("subject = %v", subject)
			}
		})
	}
}

func TestBuildAccessPiece_ClusterRoleNeverReadsSecrets(t *testing.T) {
	for _, profile := range []string{AccessProfileWorkloads, AccessProfileReadOnly} {
		piece, err := BuildAccessPiece(agentOptions(profile))
		if err != nil {
			t.Fatalf("BuildAccessPiece: %v", err)
		}
		for _, obj := range piece.Access {
			if obj["kind"] != "ClusterRole" {
				continue
			}
			rules, _ := obj["rules"].([]any)
			writesNamespaces := false
			for _, r := range rules {
				rule, _ := r.(map[string]any)
				groups, _ := rule["apiGroups"].([]any)
				resources, _ := rule["resources"].([]any)
				verbs, _ := rule["verbs"].([]any)
				for _, g := range groups {
					for _, res := range resources {
						if g == "" && (res == "secrets" || res == "*") {
							t.Errorf("profile %s: generated cluster role grants %v on core %v", profile, verbs, res)
						}
						if g == "*" {
							t.Errorf("profile %s: generated cluster role uses a wildcard API group", profile)
						}
					}
				}
				for _, v := range verbs {
					if v == "create" {
						writesNamespaces = true
					}
					if v == "delete" || v == "*" {
						t.Errorf("profile %s: generated cluster role grants %v", profile, v)
					}
				}
			}
			if writesNamespaces != (profile == AccessProfileWorkloads) {
				t.Errorf("profile %s: namespace creation granted = %v", profile, writesNamespaces)
			}
		}
	}
}

func TestBuildAccessPiece_AgentObjects(t *testing.T) {
	t.Run("job", func(t *testing.T) {
		piece, err := BuildAccessPiece(agentOptions(AccessProfileWorkloads))
		if err != nil {
			t.Fatalf("BuildAccessPiece: %v", err)
		}
		want := "ServiceAccount/ocm-fleet-enrollment-agent,Role/ocm-fleet-enrollment-agent,RoleBinding/ocm-fleet-enrollment-agent,Secret/ocm-fleet-enrollment,Job/ocm-fleet-enroll"
		if got := strings.Join(objectKeys(piece.Agent), ","); got != want {
			t.Errorf("agent objects = %s, want %s", got, want)
		}
		raw, _ := json.Marshal(piece.Agent)
		for _, arg := range []string{"--once", "--server-url=https://fleet.test:8085", "--connect-address=fleet-internal:8085",
			"--token-lifetime=1h0m0s", "--service-account=ocm-fleet-access", "--bundle-token-file=/etc/ocm-fleet-enrollment/token",
			"--server-ca-file=/etc/ocm-fleet-enrollment/ca.crt"} {
			if !strings.Contains(string(raw), arg) {
				t.Errorf("agent arguments lack %q", arg)
			}
		}
		if strings.Contains(string(raw), "--cluster-name") {
			t.Error("a cluster name argument was written although none was given")
		}
		// The agent may only request the access account's token.
		role := piece.Agent[1]
		rules, _ := role["rules"].([]any)
		rule, _ := rules[0].(map[string]any)
		if len(rules) != 1 || strings.Join(toStrings(rule["resources"]), ",") != "serviceaccounts/token" ||
			strings.Join(toStrings(rule["resourceNames"]), ",") != AccessServiceAccount || strings.Join(toStrings(rule["verbs"]), ",") != "create" {
			t.Errorf("agent role rules = %v", rules)
		}
	})
	t.Run("deployment", func(t *testing.T) {
		opts := agentOptions(AccessProfileWorkloads)
		opts.Mode = AgentModeDeployment
		opts.ClusterName = "{{CLUSTER_NAME}}"
		opts.AccessAnnotations = map[string]string{"keep": "yes"}
		piece, err := BuildAccessPiece(opts)
		if err != nil {
			t.Fatalf("BuildAccessPiece: %v", err)
		}
		last := piece.Agent[len(piece.Agent)-1]
		if objectKey(last) != "Deployment/ocm-fleet-enrollment-agent" {
			t.Fatalf("last agent object = %s", objectKey(last))
		}
		raw, _ := json.Marshal(last)
		if strings.Contains(string(raw), "--once") || !strings.Contains(string(raw), "--cluster-name={{CLUSTER_NAME}}") ||
			!strings.Contains(string(raw), "readinessProbe") {
			t.Errorf("deployment = %s", raw)
		}
		for _, obj := range piece.Access {
			meta, _ := obj["metadata"].(map[string]any)
			ann, _ := meta["annotations"].(map[string]any)
			if ann["keep"] != "yes" {
				t.Errorf("%s lacks the access annotation", objectKey(obj))
			}
		}
		for _, obj := range piece.Agent {
			meta, _ := obj["metadata"].(map[string]any)
			if _, ok := meta["annotations"]; ok {
				t.Errorf("%s carries the access annotation", objectKey(obj))
			}
		}
	})
}

func toStrings(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, e := range list {
		s, _ := e.(string)
		out = append(out, s)
	}
	return out
}

func TestBuildAccessPiece_SecretOnlyInTheAgentSecret(t *testing.T) {
	piece, err := BuildAccessPiece(agentOptions(AccessProfileWorkloads))
	if err != nil {
		t.Fatalf("BuildAccessPiece: %v", err)
	}
	access, _ := json.Marshal(piece.Access)
	if strings.Contains(string(access), "s3cr3t") {
		t.Error("the bundle secret is in the lasting access objects")
	}
	holders := 0
	for _, obj := range piece.Agent {
		raw, _ := json.Marshal(obj)
		if strings.Contains(string(raw), "s3cr3t") {
			holders++
			if obj["kind"] != "Secret" {
				t.Errorf("the bundle secret is in %s", objectKey(obj))
			}
		}
	}
	if holders != 1 {
		t.Errorf("the bundle secret is in %d objects, want 1", holders)
	}
}

func TestBuildAccessPiece_AccessOnlyAndValidation(t *testing.T) {
	piece, err := BuildAccessPiece(AccessPieceOptions{Profile: AccessProfileReadOnly, AccessOnly: true})
	if err != nil {
		t.Fatalf("BuildAccessPiece access only: %v", err)
	}
	if len(piece.Agent) != 0 || len(piece.Access) == 0 {
		t.Errorf("access only: %d access, %d agent objects", len(piece.Access), len(piece.Agent))
	}
	for name, mutate := range map[string]func(o *AccessPieceOptions){
		"no image":        func(o *AccessPieceOptions) { o.Image = "" },
		"no server":       func(o *AccessPieceOptions) { o.ServerURL = "" },
		"no bundle token": func(o *AccessPieceOptions) { o.BundleToken = "" },
		"unknown profile": func(o *AccessPieceOptions) { o.Profile = "everything" },
		"unknown mode":    func(o *AccessPieceOptions) { o.Mode = "cron" },
	} {
		opts := agentOptions(AccessProfileWorkloads)
		mutate(&opts)
		if _, err := BuildAccessPiece(opts); !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("%s: error = %v, want ErrInvalidArgument", name, err)
		}
	}
}

func TestRenderManifestYAML_RoundTrips(t *testing.T) {
	piece, err := BuildAccessPiece(agentOptions(AccessProfileWorkloads))
	if err != nil {
		t.Fatalf("BuildAccessPiece: %v", err)
	}
	out, err := RenderManifestYAML(piece.All())
	if err != nil {
		t.Fatalf("RenderManifestYAML: %v", err)
	}
	docs := bytes.Split(out, []byte("\n---\n"))
	if len(docs) != len(piece.All()) {
		t.Fatalf("%d YAML documents for %d objects", len(docs), len(piece.All()))
	}
	for i, doc := range docs {
		var obj map[string]any
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			t.Fatalf("document %d does not parse: %v", i, err)
		}
		if obj["apiVersion"] == nil || obj["kind"] == nil {
			t.Errorf("document %d lacks apiVersion or kind", i)
		}
		if _, ok := obj["status"]; ok {
			t.Errorf("document %d carries a status", i)
		}
		if meta, _ := obj["metadata"].(map[string]any); meta != nil {
			if _, ok := meta["creationTimestamp"]; ok {
				t.Errorf("document %d carries a creation time", i)
			}
		}
	}
}

func TestHelpers(t *testing.T) {
	t.Run("ValidateClusterID", func(t *testing.T) {
		for _, ok := range []string{"ext", "hub1.ext", "a-b.c-d", "c1"} {
			if err := ValidateClusterID(ok); err != nil {
				t.Errorf("ValidateClusterID(%q) = %v", ok, err)
			}
		}
		for _, bad := range []string{"", "Ext", "a/b", "-a", "a-", "a..b", ".a", "a b", strings.Repeat("a", 201)} {
			if err := ValidateClusterID(bad); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("ValidateClusterID(%q) = %v, want ErrInvalidArgument", bad, err)
			}
		}
	})
	t.Run("ServiceAccountFromUsername", func(t *testing.T) {
		sa, ok := ServiceAccountFromUsername("system:serviceaccount:ns:name")
		if !ok || sa.Namespace != "ns" || sa.Name != "name" || sa.String() != "ns/name" {
			t.Errorf("got %+v, %v", sa, ok)
		}
		for _, bad := range []string{"kubernetes-admin", "system:serviceaccount:ns", "system:serviceaccount::name", "system:serviceaccount:ns:a:b"} {
			if _, ok := ServiceAccountFromUsername(bad); ok {
				t.Errorf("ServiceAccountFromUsername(%q) succeeded", bad)
			}
		}
		if parsed, ok := ParseServiceAccountRef("ns/name"); !ok || parsed != sa {
			t.Errorf("ParseServiceAccountRef = %+v, %v", parsed, ok)
		}
		if _, ok := ParseServiceAccountRef("name"); ok {
			t.Error("ParseServiceAccountRef accepted a bare name")
		}
	})
	t.Run("TokenLifetime", func(t *testing.T) {
		iat := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		gotIat, gotExp := TokenLifetime(makeToken("x", iat, iat.Add(time.Hour)))
		if !gotIat.Equal(iat) || !gotExp.Equal(iat.Add(time.Hour)) {
			t.Errorf("TokenLifetime = %v, %v", gotIat, gotExp)
		}
		for _, bad := range []string{"", "opaque", "a.b.c", "a.b"} {
			if i, e := TokenLifetime(bad); !i.IsZero() || !e.IsZero() {
				t.Errorf("TokenLifetime(%q) = %v, %v, want zero", bad, i, e)
			}
		}
	})
	t.Run("CA", func(t *testing.T) {
		a, b := testCA(t, "a"), testCA(t, "b")
		if !SameCA(a, a) || SameCA(a, b) || !SameCA(append(append([]byte{}, b...), a...), a) {
			t.Error("SameCA does not compare by shared certificate")
		}
		if SameCA(a, []byte("junk")) || SameCA(nil, nil) {
			t.Error("SameCA matched unparseable input")
		}
		fps, err := CAFingerprints(append(append([]byte{}, a...), a...))
		if err != nil || len(fps) != 1 || !strings.HasPrefix(fps[0], "sha256:") {
			t.Errorf("CAFingerprints = %v, %v", fps, err)
		}
	})
	t.Run("APIServerURL", func(t *testing.T) {
		if err := ValidateAPIServerURL("https://ext:6443"); err != nil {
			t.Errorf("valid URL refused: %v", err)
		}
		for _, bad := range []string{"", "ext:6443", "http://ext:6443", "https://", "https://u:p@ext", "https://ext/path", "https://ext?x=1"} {
			if err := ValidateAPIServerURL(bad); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("ValidateAPIServerURL(%q) = %v", bad, err)
			}
		}
		if NormalizeAPIServerURL(" https://ext:6443/ ") != "https://ext:6443" {
			t.Error("NormalizeAPIServerURL does not trim")
		}
	})
	t.Run("SearchTerms", func(t *testing.T) {
		got := SearchTerms("hub1.ext", map[string]string{"cloud": "aws", "empty": ""}, "default", "")
		want := []string{"aws", "cloud=aws", "default", "ext", "hub1", "hub1.ext"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("SearchTerms = %v, want %v", got, want)
		}
	})
	t.Run("RightsProfile", func(t *testing.T) {
		for want, r := range map[string]ClusterRights{
			AccessProfileClusterAdmin: {ClusterAdmin: true, Workloads: true},
			AccessProfileWorkloads:    {Workloads: true, ReadCluster: true},
			AccessProfileReadOnly:     {ReadCluster: true},
			"none":                    {},
		} {
			if got := r.Profile(); got != want {
				t.Errorf("%+v.Profile() = %q, want %q", r, got, want)
			}
		}
	})
}

// --- HTTP ---

type httpHarness struct {
	*enrollHarness
	mux *http.ServeMux
}

func newHTTPHarness(t *testing.T, endpoint EnrollmentEndpoint) *httpHarness {
	t.Helper()
	h := newEnrollHarness(t)
	api := NewEnrollmentHTTP(h.svc, endpoint, func(context.Context) string { return "ops" }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	// The dynamic inventory API's routes for the same collections.
	for _, prefix := range []string{"/clusters", "/enrollmentBundles"} {
		dyn := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
		mux.Handle(EnrollmentAPIPrefix+prefix, dyn)
		mux.Handle(EnrollmentAPIPrefix+prefix+"/", dyn)
	}
	api.Register(
		func(pattern string, handler http.Handler) { mux.Handle(pattern, handler) },
		func(pattern string, handler http.Handler) { mux.Handle(pattern, handler) },
	)
	return &httpHarness{enrollHarness: h, mux: mux}
}

var testEndpoint = EnrollmentEndpoint{ServerURL: "https://fleet.test:8085", AgentImage: "registry.test/agent:1"}

func (h *httpHarness) do(method, path, bearer string, body any) (int, map[string]any) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, EnrollmentAPIPrefix+path, reader)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rr := httptest.NewRecorder()
	h.mux.ServeHTTP(rr, req)
	out := map[string]any{}
	if rr.Body.Len() > 0 && strings.HasPrefix(rr.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			h.t.Fatalf("response is not JSON: %v: %s", err, rr.Body.String())
		}
	}
	return rr.Code, out
}

func TestHTTP_RoutesCoexistWithTheDynamicAPI(t *testing.T) {
	h := newHTTPHarness(t, testEndpoint)
	// Reads stay with the dynamic API (the stand-in answers 418).
	for _, path := range []string{"/clusters", "/clusters/ext", "/enrollmentBundles", "/enrollmentBundles/eb-0123456789ab"} {
		if code, _ := h.do(http.MethodGet, path, "", nil); code != http.StatusTeapot {
			t.Errorf("GET %s = %d, want the dynamic API's answer", path, code)
		}
	}
	if code, _ := h.do(http.MethodPost, "/clusters/ext:unknownVerb", "", nil); code != http.StatusNotFound {
		t.Errorf("unknown verb = %d, want 404", code)
	}
	if code, _ := h.do(http.MethodPost, "/clusters/ext", "", nil); code != http.StatusNotFound {
		t.Errorf("POST without a verb = %d, want 404", code)
	}
}

func TestHTTP_EnrollWithdrawForget(t *testing.T) {
	h := newHTTPHarness(t, testEndpoint)
	api, ca, token := h.addCluster("ext", time.Hour)

	code, body := h.do(http.MethodPost, "/clusters:enroll", "", map[string]any{
		"clusterId": "ext", "apiServer": api, "caCert": string(ca), "token": token,
		"labels": map[string]string{"env": "dev"}, "grantPlatformDelivery": true,
	})
	if code != http.StatusOK {
		t.Fatalf("enroll = %d %v", code, body)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), token) {
		t.Error("the enroll response holds the credential")
	}
	cluster, _ := body["cluster"].(map[string]any)
	obs, _ := cluster["observation"].(map[string]any)
	if cluster["name"] != "clusters/ext" || obs["state"] != "enrolled" || obs["enrolledBy"] != "ops" || obs["deliveryCredential"] != "platform" {
		t.Errorf("enroll response = %v", body)
	}

	code, body = h.do(http.MethodPost, "/clusters:enroll", "", map[string]any{"clusterId": "ext2", "apiServer": api, "caCert": string(ca), "token": token})
	if code != http.StatusConflict || body["reason"] != "cluster-already-known" {
		t.Errorf("second id for the same cluster = %d %v", code, body)
	}
	details, _ := body["details"].(map[string]any)
	if details["target"] != "k8s-ext" {
		t.Errorf("details = %v", body["details"])
	}
	code, body = h.do(http.MethodPost, "/clusters:enroll", "", map[string]any{"clusterId": "x", "apiServer": api, "caCert": string(ca), "token": "nope"})
	if code != http.StatusUnprocessableEntity || body["reason"] != "credential-rejected" {
		t.Errorf("rejected credential = %d %v", code, body)
	}
	code, body = h.do(http.MethodPost, "/clusters:enroll", "", map[string]any{"clusterId": "Bad"})
	if code != http.StatusBadRequest || body["reason"] != "invalid-argument" {
		t.Errorf("invalid id = %d %v", code, body)
	}
	code, body = h.do(http.MethodPost, "/clusters:enroll", "", map[string]any{"clusterId": "x", "unknownField": 1})
	if code != http.StatusBadRequest {
		t.Errorf("unknown field = %d %v", code, body)
	}

	if code, body = h.do(http.MethodPost, "/clusters/ext:renewCredential", "", nil); code != http.StatusOK {
		t.Errorf("renew = %d %v", code, body)
	}
	if code, body = h.do(http.MethodDelete, "/clusters/ext", "", nil); code != http.StatusConflict || body["reason"] != "still-enrolled" {
		t.Errorf("forget while enrolled = %d %v", code, body)
	}
	h.seedDeployment("named", domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"k8s-ext"}})
	code, body = h.do(http.MethodPost, "/clusters/ext:withdraw", "", nil)
	if code != http.StatusConflict || body["reason"] != "deployments-resolve-to-target" {
		t.Fatalf("withdraw with deployments = %d %v", code, body)
	}
	if code, body = h.do(http.MethodPost, "/clusters/nobody:withdraw", "", map[string]any{"reason": "x"}); code != http.StatusNotFound {
		t.Errorf("withdraw unknown = %d %v", code, body)
	}
	code, body = h.do(http.MethodPost, "/clusters/ext:withdraw", "", map[string]any{"reason": "gone", "leaveDeployments": true})
	if code != http.StatusOK {
		t.Fatalf("withdraw leaving deployments = %d %v", code, body)
	}
}

func TestHTTP_BundleManifestAndPresent(t *testing.T) {
	h := newHTTPHarness(t, testEndpoint)
	api, ca, token := h.addCluster("ext2", time.Hour)

	code, body := h.do(http.MethodPost, "/enrollmentBundles", "", map[string]any{
		"clusterId": "ext2", "apiServer": api, "profile": "read-only", "tokenLifetimeSeconds": 3600,
	})
	if code != http.StatusOK {
		t.Fatalf("create bundle = %d %v", code, body)
	}
	manifest, _ := body["manifest"].(string)
	name, _ := body["name"].(string)
	id := strings.TrimPrefix(name, "enrollmentBundles/")
	if !bundleIDPattern.MatchString(id) || !strings.Contains(manifest, "kind: Job") || !strings.Contains(manifest, "registry.test/agent:1") {
		t.Fatalf("create bundle response: name %q, manifest %d bytes", name, len(manifest))
	}
	// The bearer value is in the manifest and nowhere else in the response.
	var bearer string
	for _, doc := range strings.Split(manifest, "\n---\n") {
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("manifest document does not parse: %v", err)
		}
		if obj["kind"] == "Secret" {
			data, _ := obj["stringData"].(map[string]any)
			bearer, _ = data["token"].(string)
		}
	}
	if !strings.HasPrefix(bearer, id+".") {
		t.Fatalf("the manifest's secret holds %q", bearer)
	}
	delete(body, "manifest")
	rest, _ := json.Marshal(body)
	if strings.Contains(string(rest), strings.TrimPrefix(bearer, id+".")) {
		t.Error("the bundle secret appears outside the manifest")
	}

	present := map[string]any{"caCert": string(ca), "token": token, "serviceAccount": map[string]string{"namespace": AccessNamespace, "name": AccessServiceAccount}}
	if code, body = h.do(http.MethodPost, "/enrollments:present", "", present); code != http.StatusUnauthorized || body["reason"] != "bundle-missing" {
		t.Errorf("present without a bundle = %d %v", code, body)
	}
	if code, body = h.do(http.MethodPost, "/enrollments:present", id+".wrong", present); code != http.StatusUnauthorized {
		t.Errorf("present with a wrong secret = %d %v", code, body)
	}
	code, body = h.do(http.MethodPost, "/enrollments:present", bearer, present)
	if code != http.StatusOK || body["cluster"] != "clusters/ext2" || body["target"] != "k8s-ext2" || body["state"] != "enrolled" || body["renewAfterSeconds"] != float64(1200) {
		t.Fatalf("present = %d %v", code, body)
	}

	if code, body = h.do(http.MethodDelete, "/enrollmentBundles/"+id, "", nil); code != http.StatusOK {
		t.Errorf("revoke = %d %v", code, body)
	}
	if code, _ = h.do(http.MethodDelete, "/enrollmentBundles/"+id, "", nil); code != http.StatusNotFound {
		t.Errorf("second revoke = %d, want 404", code)
	}
	if code, _ = h.do(http.MethodPost, "/enrollments:present", bearer, present); code != http.StatusUnauthorized {
		t.Errorf("present with a revoked bundle = %d, want 401", code)
	}
}

func TestHTTP_BundleNeedsAConfiguredEndpoint(t *testing.T) {
	h := newHTTPHarness(t, EnrollmentEndpoint{})
	api, _, _ := h.addCluster("ext2", time.Hour)
	code, body := h.do(http.MethodPost, "/enrollmentBundles", "", map[string]any{"clusterId": "ext2", "apiServer": api})
	if code != http.StatusBadRequest {
		t.Fatalf("create bundle without an endpoint = %d %v", code, body)
	}
	collections, err := h.records.ListCollections(context.Background(), EnrollmentBundleResourceType)
	if err != nil || len(collections) != 0 {
		t.Errorf("a bundle was recorded although no manifest could be generated (%v, %v)", collections, err)
	}
}

func TestHTTP_AccessManifest(t *testing.T) {
	h := newHTTPHarness(t, EnrollmentEndpoint{})
	req := httptest.NewRequest(http.MethodGet, EnrollmentAPIPrefix+"/accessManifest?profile=cluster-admin", nil)
	rr := httptest.NewRecorder()
	h.mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("accessManifest = %d %s", rr.Code, rr.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	manifest, _ := body["manifest"].(string)
	if !strings.Contains(manifest, "name: cluster-admin") || strings.Contains(manifest, "kind: Job") || strings.Contains(manifest, "kind: Secret") {
		t.Errorf("manifest = %s", manifest)
	}
	req = httptest.NewRequest(http.MethodGet, EnrollmentAPIPrefix+"/accessManifest?profile=everything", nil)
	rr = httptest.NewRecorder()
	h.mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("unknown profile = %d, want 400", rr.Code)
	}
}

func TestHTTP_PresentIsRateLimitedPerBundle(t *testing.T) {
	h := newHTTPHarness(t, testEndpoint)
	present := map[string]any{"caCert": "x", "token": "y"}
	bad := "eb-0123456789ab.wrong"
	for i := 0; i < 20; i++ {
		if code, _ := h.do(http.MethodPost, "/enrollments:present", bad, present); code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i, code)
		}
	}
	code, body := h.do(http.MethodPost, "/enrollments:present", bad, present)
	if code != http.StatusTooManyRequests || body["reason"] != "rate-limited" {
		t.Fatalf("attempt after the limit = %d %v", code, body)
	}
	// Another bundle id is not affected.
	if code, _ := h.do(http.MethodPost, "/enrollments:present", "eb-ffffffffffff.wrong", present); code != http.StatusUnauthorized {
		t.Errorf("another bundle = %d, want 401", code)
	}
}

func TestHTTP_OneFailingClusterDoesNotShutOutItsBundle(t *testing.T) {
	h := newHTTPHarness(t, testEndpoint)
	api, ca, token := h.addCluster("ext", time.Hour)
	h.svc.RegisterVerifier("registry", &fakeVerifier{clusters: map[string]VerifiedCluster{
		"hub1/ext": {ClusterID: "hub1.ext", APIServer: api, CACert: ca},
	}})
	_, bearer, err := h.svc.CreateBundle(context.Background(), CreateBundleRequest{
		Scope: BundleScope{Verifier: "registry", VerifierScope: "hub1"}, Actor: "ops",
	})
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}

	// A cluster the verifier does not know keeps presenting itself
	// with the bundle's valid secret.
	stranger := map[string]any{"clusterName": "stranger", "caCert": string(ca), "token": token}
	for i := 0; i < 20; i++ {
		if code, body := h.do(http.MethodPost, "/enrollments:present", bearer, stranger); code != http.StatusForbidden {
			t.Fatalf("attempt %d = %d %v, want 403", i, code, body)
		}
	}
	if code, body := h.do(http.MethodPost, "/enrollments:present", bearer, stranger); code != http.StatusTooManyRequests {
		t.Fatalf("the failing cluster is not limited: %d %v", code, body)
	}
	// Every other cluster under the same bundle is unaffected.
	code, body := h.do(http.MethodPost, "/enrollments:present", bearer, map[string]any{"clusterName": "ext", "caCert": string(ca), "token": token})
	if code != http.StatusOK || body["cluster"] != "clusters/hub1.ext" {
		t.Fatalf("another cluster under the same bundle = %d %v, want it enrolled", code, body)
	}
}

func TestHTTP_OversizedBodyIsRefused(t *testing.T) {
	h := newHTTPHarness(t, testEndpoint)
	big := map[string]any{"clusterId": "ext", "caCert": strings.Repeat("x", maxRequestBodyBytes)}
	if code, _ := h.do(http.MethodPost, "/clusters:enroll", "", big); code != http.StatusBadRequest {
		t.Fatalf("oversized body = %d, want 400", code)
	}
}

func TestBuildAccessPiece_LeavesOutEmptyFields(t *testing.T) {
	piece, err := BuildAccessPiece(AccessPieceOptions{AccessOnly: true})
	if err != nil {
		t.Fatalf("BuildAccessPiece: %v", err)
	}
	for _, obj := range piece.Access {
		for _, field := range []string{"status", "spec"} {
			if v, ok := obj[field]; ok {
				t.Errorf("%s %v carries an empty %q: %v", obj["kind"], obj["metadata"], field, v)
			}
		}
		if meta, _ := obj["metadata"].(map[string]any); meta["creationTimestamp"] != nil {
			t.Errorf("%s carries a creation time", obj["kind"])
		}
	}
}

func TestFailureLimiter_WindowExpires(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := newFailureLimiter(2, time.Minute)
	l.now = func() time.Time { return now }
	l.fail("k")
	l.fail("k")
	if !l.blocked("k") || l.blocked("other") {
		t.Fatal("limiter does not block the failing key only")
	}
	now = now.Add(61 * time.Second)
	if l.blocked("k") {
		t.Fatal("limiter still blocks after the window")
	}
	if len(l.fails) != 0 {
		t.Errorf("expired entries are kept: %v", l.fails)
	}
}

func TestWriteEnrollmentError_InternalErrorsAreNotDetailed(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	WriteEnrollmentError(rr, req, slog.New(slog.NewTextHandler(io.Discard, nil)), errors.New("sql: connection refused at 10.0.0.1"))
	if rr.Code != http.StatusInternalServerError || strings.Contains(rr.Body.String(), "10.0.0.1") {
		t.Errorf("internal error response = %d %s", rr.Code, rr.Body.String())
	}
}
