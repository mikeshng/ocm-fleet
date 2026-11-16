# API Design

API design follows Google's API design guidelines, in particular:

- Resource-oriented design: https://google.aip.dev/121
- Declarative-friendly interfaces: https://google.aip.dev/128

FETCH these explicitly to ensure you are not hallucinating the standards.

Navigate other AIPs as relevant:

AIPs
1 AIP Purpose and Guidelines
2 AIP Numbering
3 AIP Versioning
8 AIP Style and Guidance
9 Glossary
100 API Design Review FAQ
111 Planes
121 Resource-oriented design
122 Resource names
123 Resource types
124 Resource association
126 Enumerations
127 HTTP and gRPC Transcoding
128 Declarative-friendly interfaces
129 Server-Modified Values and Defaults
130 Methods
131 Standard methods: Get
132 Standard methods: List
133 Standard methods: Create
134 Standard methods: Update
135 Standard methods: Delete
136 Custom methods
140 Field names
141 Quantities
142 Time and duration
143 Standardized codes
144 Repeated fields
145 Ranges
146 Generic fields
147 Sensitive fields
148 Standard fields
149 Unset field values
151 Long-running operations
152 Jobs
153 Import and export
154 Resource freshness validation
155 Request identification
156 Singleton resources
157 Partial responses
158 Pagination
159 Reading across collections
160 Filtering
161 Field masks
162 Resource Revisions
163 Change validation
164 Soft delete
165 Criteria-based delete
180 Backwards compatibility
181 Stability levels
182 External software dependencies
185 API Versioning
190 Naming conventions
191 File and directory structure
192 Documentation
193 Errors
194 Automatic retry configuration
200 Precedent
202 Fields
203 Field behavior documentation
205 Beta-blocking changes
210 Unicode
211 Authorization checks
213 Common components
214 Resource expiration
215 API-specific protos
216 States
217 Unreachable resources
231 Batch methods: Get
233 Batch methods: Create
234 Batch methods: Update
235 Batch methods: Delete
236 Policy preview

## Deliberate exceptions

The add-on HTTP routes — the enrollment, bundle, and hub routes under
`/apis/kubernetes.open-cluster-management.io/v1alpha` and
`/apis/ocm.open-cluster-management.io/v1alpha` — answer failures with the add-on
envelope `{"error": <message>, "reason": <word>, "details": {...}}` rather than
the `google.rpc.Status` JSON object AIP-193 prescribes, and they use a
non-canonical 422 `unverified` status and lowercase reason words. This is
deliberate: those routes are the add-ons' own public surface, not transcoded
gRPC methods, and their clients — the enrollment agent, a hub's add-on
template, and administrators reading a terse reason word in a terminal — are
served by the documented vocabulary. The envelope, every status, and every
reason word are the contract in
[existing-clusters-api.md](existing-clusters-api.md#bodies-and-answers). The
gRPC API, its transcoded HTTP routes, and the inventory query keep the AIP-193
shape.
