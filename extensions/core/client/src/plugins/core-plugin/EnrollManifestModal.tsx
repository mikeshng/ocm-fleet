import {
  Alert,
  Button,
  Checkbox,
  ClipboardCopy,
  ClipboardCopyVariant,
  Content,
  Form,
  FormGroup,
  FormSelect,
  FormSelectOption,
  List,
  ListItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { useEffect, useState } from "react";

import { formatTime } from "./clusterTypes";
import {
  createManifestBundle,
  ENROLLMENT_SERVICE,
  type EnrollmentBundleAnswer,
  revokeEnrollmentBundle,
} from "./enrollmentApi";

const PROFILES = [
  {
    value: "workloads",
    label: "Workloads: deploy and read workloads, read the rest",
  },
  { value: "read-only", label: "Read only: index the cluster, deploy nothing" },
  { value: "cluster-admin", label: "Cluster admin: everything" },
];

interface EnrollManifestModalProps {
  isOpen: boolean;
  onClose: () => void;
}

/**
 * Enrolls one cluster with no hub: the resource manager makes a bundle
 * for it, and its administrator applies the returned manifest. The
 * cluster then presents itself; nothing is applied from here.
 */
export default function EnrollManifestModal({
  isOpen,
  onClose,
}: EnrollManifestModalProps) {
  const [clusterId, setClusterId] = useState("");
  const [apiServer, setApiServer] = useState("");
  const [profile, setProfile] = useState(PROFILES[0].value);
  const [grantPlatformDelivery, setGrantPlatformDelivery] = useState(false);
  const [creating, setCreating] = useState(false);
  const [revoking, setRevoking] = useState(false);
  const [confirmRevoke, setConfirmRevoke] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [answer, setAnswer] = useState<EnrollmentBundleAnswer | null>(null);
  const [revoked, setRevoked] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    setClusterId("");
    setApiServer("");
    setProfile(PROFILES[0].value);
    setGrantPlatformDelivery(false);
    setCreating(false);
    setRevoking(false);
    setConfirmRevoke(false);
    setError(null);
    setAnswer(null);
    setRevoked(false);
  }, [isOpen]);

  const canSubmit = clusterId.trim() !== "" && apiServer.trim() !== "";

  const create = async () => {
    setCreating(true);
    setError(null);
    try {
      setAnswer(
        await createManifestBundle(ENROLLMENT_SERVICE, {
          clusterId,
          apiServer,
          profile,
          grantPlatformDelivery,
        }),
      );
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "The manifest was not generated",
      );
    } finally {
      setCreating(false);
    }
  };

  const revoke = async () => {
    if (!answer) return;
    setRevoking(true);
    setError(null);
    try {
      await revokeEnrollmentBundle(ENROLLMENT_SERVICE, answer.bundle.id);
      setRevoked(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "The bundle was not revoked");
    } finally {
      setRevoking(false);
      setConfirmRevoke(false);
    }
  };

  const startOver = () => {
    setAnswer(null);
    setRevoked(false);
    setError(null);
  };

  // A bundle's manifest carries the only copy of its secret. Closing
  // while a request is in flight would discard the answer, leaving a
  // bundle nobody can use or revoke, so the close button and Escape do
  // nothing until the request has answered.
  const canClose = !creating && !revoking && !confirmRevoke;

  return (
    <>
      {/* PatternFly hides the siblings of an open modal from screen
          readers, so a second modal cannot stay open behind the
          confirmation; the confirmation stands alone until answered. */}
      <Modal
        isOpen={isOpen && !confirmRevoke}
        onClose={canClose ? onClose : undefined}
        variant="large"
      >
        <ModalHeader
          title="Enroll with a manifest"
          description="For a cluster with no hub: its administrator applies a generated manifest, and the cluster presents itself to the resource manager."
        />
        <ModalBody>
          {revoked ? (
            <Alert variant="success" isInline title="Bundle revoked">
              Its manifest no longer enrolls anything. Generate another bundle
              if the cluster still needs to enroll.
            </Alert>
          ) : answer ? (
            <>
              <Alert
                variant="warning"
                isInline
                title="Shown once: the manifest carries an enrollment secret"
                className="pf-v6-u-mb-md"
              >
                Keep it as you keep other cluster manifests. The bundle&apos;s
                record does not carry the secret.
              </Alert>
              {error && (
                <Alert
                  variant="danger"
                  isInline
                  title="The bundle was not revoked"
                  className="pf-v6-u-mb-md"
                >
                  {error}
                </Alert>
              )}
              <Title headingLevel="h3" size="md">
                1. Apply to the cluster
              </Title>
              <ClipboardCopy
                isCode
                isReadOnly
                variant={ClipboardCopyVariant.expansion}
                hoverTip="Copy"
                clickTip="Copied"
                aria-label="Enrollment manifest"
                textAriaLabel="Enrollment manifest text"
                className="pf-v6-u-mb-md"
              >
                {answer.manifest}
              </ClipboardCopy>
              <Title headingLevel="h3" size="md">
                2. Let the job enroll the cluster
              </Title>
              <List component="ol">
                {answer.instructions.map((line) => (
                  <ListItem key={line}>{line}</ListItem>
                ))}
              </List>
              {answer.expiresAt && (
                <Content component="small" className="pf-v6-u-mt-md">
                  The bundle expires {formatTime(answer.expiresAt)}; after that
                  the manifest enrolls nothing.
                </Content>
              )}
            </>
          ) : (
            <Form>
              <FormGroup
                label="Cluster ID"
                fieldId="enroll-manifest-cluster-id"
              >
                <TextInput
                  id="enroll-manifest-cluster-id"
                  value={clusterId}
                  onChange={(_event, value) => setClusterId(value)}
                  aria-label="Cluster ID"
                />
                <Content component="small" className="pf-v6-u-mt-sm">
                  A DNS subdomain: lowercase letters, digits, &apos;-&apos; and
                  &apos;.&apos;, at most 200 characters. The cluster gets this
                  id here and in its target name.
                </Content>
              </FormGroup>
              <FormGroup
                label="API server"
                fieldId="enroll-manifest-api-server"
                className="pf-v6-u-mt-md"
              >
                <TextInput
                  id="enroll-manifest-api-server"
                  value={apiServer}
                  onChange={(_event, value) => setApiServer(value)}
                  aria-label="API server"
                />
                <Content component="small" className="pf-v6-u-mt-sm">
                  An https URL with no path, query, or credentials. The name
                  must resolve from inside the resource manager.
                </Content>
              </FormGroup>
              <FormGroup
                label="What the resource manager may do on the cluster"
                fieldId="enroll-manifest-profile"
                className="pf-v6-u-mt-md"
              >
                <FormSelect
                  id="enroll-manifest-profile"
                  value={profile}
                  onChange={(_event, value) => setProfile(value)}
                  aria-label="Access profile"
                >
                  {PROFILES.map((p) => (
                    <FormSelectOption
                      key={p.value}
                      value={p.value}
                      label={p.label}
                    />
                  ))}
                </FormSelect>
              </FormGroup>
              <Checkbox
                id="enroll-manifest-platform-delivery"
                className="pf-v6-u-mt-md"
                label="Let the stored credential deliver deployments that are not signed"
                description="Without this, the stored credential delivers signed deployments only, and a deployment that is not signed uses its caller's own token."
                isChecked={grantPlatformDelivery}
                onChange={(_event, checked) =>
                  setGrantPlatformDelivery(checked)
                }
              />
              <Content component="small" className="pf-v6-u-mt-md">
                The resource manager must have an enrollment agent image and its
                own address configured, and the cluster must be able to pull
                that image. Generating makes a bundle; nothing changes on the
                cluster until its administrator applies the manifest.
              </Content>
              {error && (
                <Alert
                  variant="danger"
                  isInline
                  title="The manifest was not generated"
                  className="pf-v6-u-mt-md"
                >
                  {error}
                </Alert>
              )}
            </Form>
          )}
        </ModalBody>
        <ModalFooter>
          {answer && !revoked && (
            <Button
              variant="danger"
              onClick={() => setConfirmRevoke(true)}
              isDisabled={revoking || creating}
            >
              Revoke bundle
            </Button>
          )}
          {!answer && (
            <Button
              variant="primary"
              onClick={() => void create()}
              isLoading={creating}
              isDisabled={creating || !canSubmit}
            >
              Generate manifest
            </Button>
          )}
          {revoked && (
            <Button variant="secondary" onClick={startOver}>
              Generate another
            </Button>
          )}
          <Button
            variant="link"
            onClick={onClose}
            isDisabled={creating || revoking}
          >
            {answer ? "Close" : "Cancel"}
          </Button>
        </ModalFooter>
      </Modal>

      <Modal
        isOpen={isOpen && confirmRevoke}
        onClose={revoking ? undefined : () => setConfirmRevoke(false)}
        variant="small"
      >
        <ModalHeader
          title="Revoke bundle"
          description={`Revoke bundle "${answer?.bundle.id ?? ""}"? Its manifest stops enrolling the cluster. This cannot be undone.`}
        />
        <ModalBody>
          <Content component="p">
            A copy of the manifest that was already applied stops working, and
            the bundle cannot be used again.
          </Content>
        </ModalBody>
        <ModalFooter>
          <Button
            variant="danger"
            onClick={() => void revoke()}
            isLoading={revoking}
            isDisabled={revoking}
          >
            Revoke bundle
          </Button>
          <Button
            variant="link"
            onClick={() => setConfirmRevoke(false)}
            isDisabled={revoking}
          >
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </>
  );
}
