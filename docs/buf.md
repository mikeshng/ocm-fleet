Use the `buf` CLI for grpc generation.

Run through Nx: `npx nx run proto:fmt` formats protobuf files;
`npx nx run proto:lint` checks formatting and Buf lint rules without edits.
`npx nx run proto:verify` formats first, then checks. These also participate in
the workspace's language-agnostic `fmt`, `lint`, `verify`, and `fix` targets.
CI installs Buf 1.75.0 and runs these checks through `ocm-fleet:check`.

Dependencies should be local to the repository, or remote, for repeatable builds.

`npx nx run proto:generate` writes the generated Go packages under `server/gen/`
and the committed OpenAPI document under `docs/openapi/`. The committed
artifacts were generated with Buf 1.75.0 (the version CI installs) and the
plugin versions pinned in `buf.gen.yaml`: `protoc-gen-go` v1.36.12,
`protoc-gen-go-grpc` v1.6.2, `grpc-gateway` v2.33.0, and `openapiv2` v2.33.0.
Upgrade a Buf or plugin pin deliberately and regenerate: a new generator version
can change the committed output even when the protos do not.

The OpenAPI generator emits its own YAML style, so after regenerating, run
`npx nx run ocm-fleet:fmt:workspace` before committing: the format check covers
that document.

`npx nx run proto:check:generated` regenerates both artifact trees, applies the
OpenAPI formatting, and fails when `server/gen/` or `docs/openapi/` differ from
the committed tree, so a stale artifact or a changed generator pin fails the
check. CI runs it in the workspace job, after Buf setup.

Follow the 1-1-1 Best Practice: https://protobuf.dev/best-practices/1-1-1/

And other Protobuf best practices: https://protobuf.dev/best-practices/dos-donts/

However, this should NOT supersede other API design guidance.
