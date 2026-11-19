# OCM Fleet all-in-one image

Single container (`quay.io/stolostron/ocm-fleet`) with API, baked-in UI, and
peer Dex under s6-overlay. Documented and tested with **podman**. To launch
this image through the Podman compose stack, see
[deploy/podman/](../podman/README.md).

Sandbox for demos, bootstrapping, and testing — not a production deployment.
Packaging owns PID 1, peer Dex (default), sandbox PKI, and the ordinary
`resource-manager serve` argv. The server stays IdP-agnostic.

## Build

### Podman

```bash
npx nx run image:aio
```

That builds `Dockerfile` / `Dockerfile.local` / `Dockerfile.web`, then assembles
via `Dockerfile.aio`. To reassemble from already-built component tags:

```bash
podman build -f Dockerfile.aio \
  --build-arg SERVER_IMAGE=localhost/ocm-fleet-server-local:latest \
  --build-arg WEB_IMAGE=localhost/ocm-fleet-web:latest \
  -t localhost/ocm-fleet:latest \
  -t quay.io/stolostron/ocm-fleet:latest .
```

## Quick start (Dex-on)

### Podman

Bare run needs **no** OIDC flags. Packaging starts peer Dex behind the AIO TLS
edge at `https://ocm-fleet.localhost:8085/idp` and fills AuthMethod/UI
defaults for `serve`. Foreground is the demo-friendly launch: startup stays
quiet until the public endpoint is ready, then prints the ready message, an
**OCM FLEET** block-art banner, and the URL and demo credentials. Press Ctrl+C to stop.

```bash
podman run \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  quay.io/stolostron/ocm-fleet:latest
```

Detached is the same image with `-d`; `podman logs` still shows the welcome
block after readiness (no terminal color codes).

Open https://ocm-fleet.localhost:8085 — the exact path `/` redirects
to `/app/`. The welcome banner is printed only after `https://ocm-fleet.localhost:8085/readyz`
returns success. Demo credentials appear only with built-in Dex. Errors still
stream beneath the banner if something fails later.

Routing on this origin is prefix-based, not a catch-all into the SPA:

1. **Edge** — `/idp`… goes to peer Dex (Dex-on only). Every other path is
   forwarded to the resource manager server.
2. **Resource manager server** — serves the paths below. Only exact `/` and `/app` redirect
   to `/app/`. Unknown paths return 404; they are not rewritten into `/app`.

| Path                         | Serves                     |
| ---------------------------- | -------------------------- |
| `/idp`…                      | Peer Dex (Dex-on only)     |
| `/app`…                      | SPA and static assets      |
| `/api`, `/v1alpha`, `/apis`… | Resource manager HTTP APIs |
| `/livez`, `/readyz`          | Health probes              |
| `/` or `/app` (exact)        | 302 to `/app/`             |

The sandbox certificate is intentionally browser-untrusted (private CA). In an
unmanaged desktop Chrome, Firefox, or Safari profile that allows overrides,
accept the top-level warning (Advanced → Proceed / Accept the Risk). That is
the only certificate interstitial for the SPA, API, WebSockets, and peer Dex:
they share this origin.

Host CLIs (`curl`, `fleetctl`, Go clients) still need a scoped `--cacert` /
`--oidc-ca-file`; they do not pick up the browser exception.

`.localhost` names can resolve to both `127.0.0.1` and `::1`. The documented
command publishes IPv4 loopback only. If a supported platform does not fall
back to IPv4, add a second `[::1]:8085:8085` publish for that platform — never
replace the scoped bind with `0.0.0.0` or `::`.

### Demo users (Dex-on)

Public sandbox fixtures (not production credentials). Login identifier is the
email:

| Persona   | Email                 | Password        | `preferred_username` | Group |
| --------- | --------------------- | --------------- | -------------------- | ----- |
| Operator  | `ops@ocm-fleet.local` | `ocm-fleet-ops` | `ops-user`           | `ops` |
| Developer | `dev@ocm-fleet.local` | `ocm-fleet-dev` | `dev-user`           | `dev` |

## Packaging defaults

AIO fills omitted AuthMethod/UI fields. These are **packaging** defaults, not
`resource-manager serve` binary defaults. The [OpenShift deployment example](../openshift/README.md)
uses a separate deployment-specific configuration surface; do not mix the two.

**Required from you** — everything else may be left unset.

| Mode             | You must set                                     |
| ---------------- | ------------------------------------------------ |
| Dex-on (default) | nothing                                          |
| Dex-off          | `OIDC_ISSUER_URL`                                |
| Kind             | a live engine socket mounted at `CONTAINER_HOST` |

**Optional — packaging fills if omitted.** Set an env var only to override.

| Setting                 | Env                                 | If omitted                                                                                                                                                                                       |
| ----------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Issuer                  | `OIDC_ISSUER_URL`                   | Peer Dex `https://ocm-fleet.localhost:8085/idp` (Dex-on). Setting this switches to Dex-off; there is no packaging issuer in that mode.                                                           |
| UI client               | `OIDC_UI_CLIENT_ID`                 | `ocm-fleet-console`                                                                                                                                                                              |
| UI scope                | `OIDC_UI_SCOPE`                     | Dex-on: `openid profile email groups audience:server:client_id:resource-manager`. Dex-off: `openid profile email`                                                                                |
| Resource audience       | `OIDC_RESOURCE_AUDIENCE`            | `resource-manager`                                                                                                                                                                               |
| OIDC CA                 | `OIDC_CA_FILE`                      | sandbox CA on Dex-on. Dex-off: unset unless discovery/TLS needs non-system trust                                                                                                                 |
| Log level               | `LOG_LEVEL`                         | `error`. One control for the resource manager server, peer Dex, and s6 lifecycle verbosity (`debug`, `info`, `warn`, `error`)                                                                    |
| Addons                  | `OCM_RM_SERVER_ADDONS`              | `kind,kubernetes,ocm`                                                                                                                                                                            |
| Container socket        | `CONTAINER_HOST`                    | image default `unix:///var/run/docker.sock`. Override only if the mount path differs                                                                                                             |
| Kind loopback forward   | `KIND_LOOPBACK_FORWARD_TO`          | Dex-on + live socket: `ocm-fleet:8085`. Set empty to disable. Not written on Dex-off                                                                                                             |
| Enrollment endpoint     | `OCM_RM_ENROLLMENT_SERVER_URL`      | Live socket: `https://ocm-fleet.localhost:8085`. Otherwise unset, and no enrollment manifest is generated                                                                                        |
| Enrollment CA           | `OCM_RM_ENROLLMENT_CA_FILE`         | sandbox CA with the defaulted endpoint. With an explicit endpoint: unset, which means the clusters' system roots                                                                                 |
| Enrollment dial address | `OCM_RM_ENROLLMENT_CONNECT_ADDRESS` | `ocm-fleet:8085` with the defaulted endpoint. With an explicit endpoint: unset, and the endpoint's host is resolved                                                                              |
| Enrollment agent image  | `OCM_RM_ENROLLMENT_AGENT_IMAGE`     | unset, and no enrollment manifest is generated                                                                                                                                                   |
| Cluster egress allow    | `OCM_RM_CLUSTER_EGRESS_ALLOW`       | unset: the server refuses to reach a cluster or hub at loopback, link-local (cloud metadata included) or another range a cluster is not at; a comma-separated CIDR list permits an address there |

The enrollment settings are explained under
[Existing clusters and OCM hubs](#existing-clusters-and-ocm-hubs).

`LOG_LEVEL` is the only AIO logging control. It is passed to the resource manager
(`--log-level`), rendered into peer Dex's logger, and mapped to s6-overlay
verbosity. Restore diagnostic output without rebuilding:

```bash
podman run \
  -e LOG_LEVEL=debug \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  quay.io/stolostron/ocm-fleet:latest
```

## External issuer (Dex-off)

Presence of `OIDC_ISSUER_URL` skips peer Dex and forwards that issuer into the
same serve bootstrap path. The AIO UI/API edge stays
`https://ocm-fleet.localhost:8085`; do not proxy the external IdP
under `/idp`. Register that origin, `/app/auth/callback`, and
`/app/silent-renew.html` on the external client. Packaging still fills omitted
fields above, but UI scope becomes portable OIDC (`openid profile email`)
instead of Dex-specific cross-client audience scopes. Configure API audiences
through the external provider's supported mechanism; OIDC scopes alone do not
establish the resource audience. Pass `OIDC_CA_FILE` only when discovery/TLS needs non-system
trust. Issuer URL shape and CA readability/PEM are
validated by `resource-manager serve`. Packaging fills omitted AuthMethod/UI defaults
and forwards the resolved serve argv. The welcome banner still waits for public
`/readyz`, then shows the console URL and external-IdP text without demo
Dex credentials.

### Podman

Minimal:

```bash
podman run -d \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -e OIDC_ISSUER_URL=https://idp.example.com/tenant/opaque-id \
  quay.io/stolostron/ocm-fleet:latest
```

All OIDC overrides (values shown are packaging defaults except issuer/CA). This example assumes `provider-ca.crt` exists in
the current host directory; omit the mount and `OIDC_CA_FILE` for system-trusted
issuers:

```bash
podman run -d \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -e OIDC_ISSUER_URL=https://idp.example.com/tenant/opaque-id \
  -v "$PWD/provider-ca.crt:/run/provider-ca.crt:ro" \
  -e OIDC_CA_FILE=/run/provider-ca.crt \
  -e OIDC_UI_CLIENT_ID=ocm-fleet-console \
  -e OIDC_UI_SCOPE='openid profile email' \
  -e OIDC_RESOURCE_AUDIENCE=resource-manager \
  quay.io/stolostron/ocm-fleet:latest
```

AuthMethod settings apply only to an empty store. Use a separate fresh store to test another issuer; `fleetctl auth setup` only configures local CLI login.

## With kind

### Podman

Privileged + host container socket (full control of the host engine —
local/dev only). Create the `kind` network once if needed:
`podman network create kind`.

When a live unix socket is present at `CONTAINER_HOST`, packaging writes
`/run/ocm-fleet/kind.env`:

- `KIND_EXPERIMENTAL_DOCKER_NETWORK=kind` unless already set (including
  intentionally empty to disable)
- On Dex-on only: `KIND_LOOPBACK_FORWARD_TO=ocm-fleet:8085` so kind
  control-plane nodes run a loopback TCP proxy (`127.0.0.1:8085` →
  `ocm-fleet:8085`, the AIO TLS edge). The kind addon also installs a
  kube-apiserver `hostAliases` mapping for the OIDC issuer hostname
  (via a kubeadm patch directory extra-mounted into control-plane nodes)
  so that name resolves to `127.0.0.1` before first start. A TCP proxy
  binary is run as a systemd unit on the node (not iptables DNAT) so
  Fedora and macOS behave the same. Podman DNS resolves the `ocm-fleet`
  alias, so AIO restarts keep working. Override with a different
  `host:port`, or set the variable empty to disable both the proxy and
  the hostAliases overlay.

Join `--network kind:alias=ocm-fleet`. Without that alias the default
destination host does not resolve from kind nodes.

On Linux, rootless Podman does not listen on `/var/run/docker.sock` (that
path is Docker, or a macOS `podman machine` helper symlink). The API socket
is `$XDG_RUNTIME_DIR/podman/podman.sock`, and only after the user systemd
unit is running:

```bash
export PODMAN_SOCKET=$XDG_RUNTIME_DIR/podman/podman.sock
systemctl --user enable podman.socket
systemctl --user restart podman.socket
```

```bash
podman run -d \
  --privileged \
  -p 127.0.0.1:8085:8085 \
  -p 127.0.0.1:50051:50051 \
  -v /tmp:/tmp \
  -v ${PODMAN_SOCKET:-/var/run/docker.sock}:/var/run/docker.sock \
  --network kind:alias=ocm-fleet \
  quay.io/stolostron/ocm-fleet:latest
```

## Existing clusters and OCM hubs

A walkthrough from a fresh clone, with every command written out: [existing-clusters-walkthrough.md](existing-clusters-walkthrough.md).

A short demo with two hubs, in eight steps with screenshots: [poc/shim](../../poc/shim/README.md).

Every request with its body, answer, and refusal reasons: [existing-clusters-api.md](../../docs/existing-clusters-api.md).

The `ocm` addon connects Open Cluster Management hubs with a read-only
credential and shows their clusters. The `kubernetes` addon enrolls clusters
that this container did not create; an enrolled cluster is then deployed to and
indexed like one it created. The resource manager writes nothing to a hub, and
nothing to a cluster before it is enrolled: each step returns a manifest that
the hub's or the cluster's administrator applies.

Several hubs can be connected to one container, each under an id of its own. A
cluster of a hub is named `<hub id>.<name on the hub>`, so the same name on two
hubs is two clusters, and each hub keeps its own read-only account, add-on, and
enrollment secret. A hub that is registered with itself can be enrolled like
any of its clusters; the resource manager then refuses deliveries into the
namespaces of Open Cluster Management and writes none of the hub's own objects.

In the console, **Hubs** connects a hub and offers **Enroll clusters** for it.
The same steps through the API:

| Step                           | Request                                                                               | What an administrator applies                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Read-only account for a hub    | `GET /apis/ocm.open-cluster-management.io/v1alpha/hubAccessManifest`                  | The returned manifest, on the hub                                                                                                               |
| Connect the hub                | `POST /apis/ocm.open-cluster-management.io/v1alpha/hubs:connect`                      | Nothing                                                                                                                                         |
| Enroll clusters of the hub     | `POST /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}:generateEnrollmentAddon` | The returned add-on, on the hub, then a `ManagedClusterAddOn` for each cluster; with `rollout` `every-cluster` or `placement`, the add-on alone |
| Enroll a cluster without a hub | `POST /apis/kubernetes.open-cluster-management.io/v1alpha/enrollmentBundles`          | The returned manifest, on the cluster                                                                                                           |
| Withdraw a cluster             | `POST /apis/kubernetes.open-cluster-management.io/v1alpha/clusters/{id}:withdraw`     | Nothing. Deleting namespace `ocm-fleet-system` and its cluster role bindings on the cluster ends access from that side                          |
| Disconnect the hub             | `POST /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}:disconnect`              | Nothing. The read-only account stays on the hub, in namespace `ocm-fleet-hub-connection`, until its administrator removes it                    |

An enrollment manifest runs a small agent on the cluster: once as a job, or,
through the OCM add-on, for as long as the add-on is enabled there. Build its
image, make it available to the clusters, and name it with
`OCM_RM_ENROLLMENT_AGENT_IMAGE`:

```bash
podman build -f Dockerfile.enrollment-agent -t localhost/ocm-fleet-enrollment-agent:latest .
podman save -o enrollment-agent.tar localhost/ocm-fleet-enrollment-agent:latest
kind load image-archive enrollment-agent.tar --name <cluster>
```

With a live container socket the endpoint defaults suit kind clusters on the
`kind` network: the agent dials `ocm-fleet:8085` and verifies the sandbox CA.
For clusters elsewhere, set `OCM_RM_ENROLLMENT_SERVER_URL` to an address those
clusters can reach, with `OCM_RM_ENROLLMENT_CA_FILE` and
`OCM_RM_ENROLLMENT_CONNECT_ADDRESS` where they are needed.

The stored credential of an enrolled cluster applies signed deployments only,
unless the enrollment grants platform delivery (`grantPlatformDelivery`).
Design: [existing clusters and OCM hubs](../../docs/design/addons/existing-clusters-and-ocm-hubs.md).

## Fleetctl against Dex-on

Host CLIs do not inherit the browser certificate exception. Copy the sandbox CA
for a scoped `--oidc-ca-file` only — do not install it into the system trust
store:

### Podman

```bash
podman cp <container>:/data/sandbox/pki/ca.crt .

fleetctl auth setup \
  --issuer-url https://ocm-fleet.localhost:8085/idp \
  --client-id fleetctl \
  --key-enrollment-client-id resource-manager-signing \
  --oidc-ca-file ca.crt \
  --scopes 'openid,profile,email,audience:server:client_id:resource-manager'

fleetctl auth login
fleetctl auth inspect-token   # aud should include resource-manager (and fleetctl)
fleetctl deployments list
```

`auth login` opens a browser to Dex; sign in with a demo user above. Omit the
`audience:server:client_id:resource-manager` scope and API calls fail with `aud` not
satisfied (`fleetctl` only).

---

## Packaging internals

### Pins

Dex image digest and s6-overlay version/checksums are `ARG` defaults in
[`Dockerfile.aio`](../../Dockerfile.aio). The build pulls Dex by
digest and verifies s6 tarball SHA-256 after download. The image entrypoint is
the AIO log-level wrapper, which `exec`s `/init` so s6 remains PID 1.

### Layout

| Path                        | Role                                                                                                            |
| --------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `cmd/aio-init`              | Init helper: Dex-on/off branch, PKI, Dex config, serve argv, hosts alias, public.env                            |
| `cmd/aio-proxy`             | TLS edge: terminates the gateway cert and reverse-proxies Dex and the resource manager server                   |
| `cmd/kind-loopback-forward` | TCP proxy copied onto kind control-planes for the public origin                                                 |
| `internal/aioinit`          | Packaging helpers (same package, separate files: `endpoints`, `pki`, `dexconfig`, `serveargv`, `kind`, `hosts`) |
| `internal/aioproxy`         | Reverse-proxy implementation used by `aio-proxy`                                                                |
| `internal/loopbackforward`  | Proxy implementation used by `kind-loopback-forward`                                                            |
| `s6/`                       | s6-overlay v3 service defs (copied to `/etc/s6-overlay/`)                                                       |
| `s6/scripts/aio-entrypoint` | Maps `LOG_LEVEL` to `S6_VERBOSITY` and execs `/init`                                                            |
| `s6/scripts/aio-welcome`    | Polls public `/readyz`, then prints the sandbox welcome                                                         |

s6 services follow the [s6-overlay README](https://github.com/just-containers/s6-overlay/blob/v3.2.3.2/README.md)
source format:

- definitions under `s6-rc.d/{aio-init,aio-proxy,dex,resource-manager,aio-welcome}/`
- membership via `user-bundles.d/user/contents.d/`
- oneshot `aio-init` script at `scripts/aio-init` (referenced from `up`)
- oneshot `aio-welcome` script at `scripts/aio-welcome` (depends on `resource-manager`;
  polls public `/readyz` before printing)
- longruns depend on `base` (`dex` and `aio-proxy` → `aio-init`, `resource-manager` → `aio-init`+`dex`+`aio-proxy`)

### Branches

- **Dex-on (default):** no `OIDC_ISSUER_URL`. `aio-init` writes
  `/run/ocm-fleet/dex.enabled`; the `dex` longrun execs peer Dex on
  loopback HTTP; `aio-proxy` serves `https://ocm-fleet.localhost:8085`;
  packaging wires AuthMethod/UI defaults into serve.
- **Dex-off:** `OIDC_ISSUER_URL` set. No `dex.enabled` flag; the `dex` longrun
  parks on `s6-pause`. The AIO TLS edge and public callback stay the same.
  Packaging forwards the issuer and fills the same defaults for omitted fields.

### Identities

- Resource manager server (`resource-manager`): `1000:1000`
- Dex: `1001:1001`
- aio-proxy: `1002:1002` (owns the gateway leaf key; cannot read `/data` or Dex state)
- s6 starts as container root only to initialize and drop privileges
