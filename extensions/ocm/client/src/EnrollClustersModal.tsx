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
  Radio,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { DownloadIcon } from "@patternfly/react-icons";
import { useState } from "react";

import {
  createEnrollmentAddon,
  type EnrollmentAddon,
  type EnrollmentRollout,
  revokeEnrollmentBundle,
} from "./api";
import { copyLine, type EnrollmentCopyBox } from "./enrollmentCopy";
import { enrollmentDownload } from "./enrollmentDownload";
import {
  initialRolloutMode,
  ROLLOUT_CHOICES,
  rolloutProblem,
  rolloutRequest,
  rolloutSummary,
} from "./enrollmentRollout";

const PROFILES = [
  {
    value: "workloads",
    label: "Workloads: deploy and read workloads, read the rest",
  },
  { value: "read-only", label: "Read only: index the cluster, deploy nothing" },
  { value: "cluster-admin", label: "Cluster admin: everything" },
];

interface EnrollClustersModalProps {
  hubId: string;
  /** The rollout of the add-on last made for the hub, to start from. */
  lastRollout?: EnrollmentRollout;
  onClose: () => void;
}

/**
 * Asks which clusters of the hub are enrolled and what the resource
 * manager may do on them, then shows the add-on that the hub's
 * administrator applies to enroll them with the hub's own tools. Nothing
 * is applied from here.
 */
export default function EnrollClustersModal({
  hubId,
  lastRollout,
  onClose,
}: EnrollClustersModalProps) {
  const [rollout, setRollout] = useState(initialRolloutMode(lastRollout));
  const [placementNamespace, setPlacementNamespace] = useState(
    lastRollout?.mode === "placement"
      ? (lastRollout.placementNamespace ?? "")
      : "",
  );
  const [placementName, setPlacementName] = useState(
    lastRollout?.mode === "placement" ? (lastRollout.placementName ?? "") : "",
  );
  const [profile, setProfile] = useState(PROFILES[0].value);
  const [grantPlatformDelivery, setGrantPlatformDelivery] = useState(false);
  const [removeAccessWithAddon, setRemoveAccessWithAddon] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [revoking, setRevoking] = useState(false);
  const [revoked, setRevoked] = useState(false);
  const [confirmRevoke, setConfirmRevoke] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [addon, setAddon] = useState<EnrollmentAddon | null>(null);
  const [copied, setCopied] = useState<EnrollmentCopyBox | null>(null);

  const download = (box: EnrollmentCopyBox) => {
    if (!addon) return;
    const file = enrollmentDownload(box, addon.manifest, addon.enableExample);
    if (!file) return;
    const url = URL.createObjectURL(
      new Blob([file.text], { type: "application/yaml" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = file.filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  };

  const problem = rolloutProblem(rollout, placementNamespace, placementName);

  const generate = async () => {
    setGenerating(true);
    setError(null);
    try {
      setAddon(
        await createEnrollmentAddon(hubId, {
          profile,
          grantPlatformDelivery,
          removeAccessWithAddon,
          ...rolloutRequest(rollout, placementNamespace, placementName),
        }),
      );
      setCopied(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "The add-on was not generated");
    } finally {
      setGenerating(false);
    }
  };

  const revoke = async () => {
    if (!addon) return;
    setRevoking(true);
    setError(null);
    try {
      await revokeEnrollmentBundle(addon.bundle.id);
      setRevoked(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "The bundle was not revoked");
    } finally {
      setRevoking(false);
      setConfirmRevoke(false);
    }
  };

  // Turning a revoked bundle back into the form, so the hub can still be
  // given an add-on to enroll its clusters with.
  const startOver = () => {
    setAddon(null);
    setRevoked(false);
    setError(null);
    setCopied(null);
  };

  // The add-on's manifest carries the only copy of its secret. Closing
  // while a request is in flight would discard the answer, so the
  // close button and Escape do nothing until the request has answered.
  const canClose = !generating && !revoking && !confirmRevoke;

  return (
    <>
      {/* PatternFly hides the siblings of an open modal from screen
          readers, so a second modal cannot stay open behind the
          confirmation; the confirmation stands alone until answered. */}
      <Modal
        isOpen={!confirmRevoke}
        onClose={canClose ? onClose : undefined}
        variant="large"
      >
        <ModalHeader
          title={`Enroll clusters of ${hubId}`}
          description="An enrolled cluster is indexed and can be deployed to from here. The hub keeps everything it does for the cluster."
        />
        <ModalBody>
          {revoked ? (
            <Alert variant="success" isInline title="Bundle revoked">
              Its manifest no longer enrolls anything. Generate another add-on
              if the hub still needs to enroll clusters.
            </Alert>
          ) : addon ? (
            <>
              <Alert
                variant="warning"
                isInline
                title="Shown once: the manifest carries an enrollment secret"
                className="pf-v6-u-mb-md"
              >
                Only clusters that this hub has accepted can use it to present
                themselves. Keep it as you keep other hub manifests.
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
              <Content component="small" className="pf-v6-u-mb-sm">
                The bundle&apos;s id, to revoke it:
              </Content>
              <ClipboardCopy
                isCode
                isReadOnly
                hoverTip="Copy"
                clickTip="Copied"
                aria-label="Enrollment bundle id"
                copyAriaLabel="Copy the bundle's id"
                className="pf-v6-u-mb-sm"
                onClick={(event) => {
                  if (
                    (event.target as HTMLElement).closest(
                      'button[aria-label="Copy the bundle\'s id"]',
                    )
                  ) {
                    setCopied("bundle");
                  }
                }}
              >
                {addon.bundle.id}
              </ClipboardCopy>
              <Content
                component="small"
                aria-live="polite"
                className="pf-v6-u-mb-md"
              >
                {copyLine("bundle", copied)}
              </Content>
              <Title headingLevel="h3" size="md">
                1. Apply to the hub
              </Title>
              <ClipboardCopy
                isCode
                isReadOnly
                variant={ClipboardCopyVariant.expansion}
                hoverTip="Copy"
                clickTip="Copied"
                aria-label="Enrollment add-on manifest"
                copyAriaLabel="Copy the add-on manifest"
                className="pf-v6-u-mb-sm"
                onClick={(event) => {
                  if (
                    (event.target as HTMLElement).closest(
                      'button[aria-label="Copy the add-on manifest"]',
                    )
                  ) {
                    setCopied("manifest");
                  }
                }}
              >
                {addon.manifest}
              </ClipboardCopy>
              <Content
                component="small"
                aria-live="polite"
                className="pf-v6-u-mb-md"
              >
                {copyLine("manifest", copied)}{" "}
                <Button
                  variant="link"
                  isInline
                  icon={<DownloadIcon />}
                  onClick={() => download("manifest")}
                >
                  Download
                </Button>
              </Content>
              {addon.enableExample ? (
                <>
                  <Title headingLevel="h3" size="md">
                    2. Enable for a cluster
                  </Title>
                  <ClipboardCopy
                    isCode
                    isReadOnly
                    variant={ClipboardCopyVariant.expansion}
                    hoverTip="Copy"
                    clickTip="Copied"
                    aria-label="Enable the add-on for one cluster"
                    copyAriaLabel="Copy the example for one cluster"
                    className="pf-v6-u-mb-sm"
                    onClick={(event) => {
                      if (
                        (event.target as HTMLElement).closest(
                          'button[aria-label="Copy the example for one cluster"]',
                        )
                      ) {
                        setCopied("example");
                      }
                    }}
                  >
                    {addon.enableExample}
                  </ClipboardCopy>
                  <Content
                    component="small"
                    aria-live="polite"
                    className="pf-v6-u-mb-md"
                  >
                    {copyLine("example", copied)}{" "}
                    <Button
                      variant="link"
                      isInline
                      icon={<DownloadIcon />}
                      onClick={() => download("example")}
                    >
                      Download
                    </Button>
                  </Content>
                </>
              ) : (
                <>
                  <Title headingLevel="h3" size="md">
                    2. Nothing to do for each cluster
                  </Title>
                  <Content component="p" className="pf-v6-u-mb-md">
                    This add-on reaches: {rolloutSummary(addon.rollout)}. The
                    hub enables it for them by itself.
                  </Content>
                </>
              )}
              <List component="ol">
                {addon.instructions.map((line) => (
                  <ListItem key={line}>{line}</ListItem>
                ))}
              </List>
            </>
          ) : (
            <Form>
              <FormGroup
                role="radiogroup"
                isStack
                label="Which clusters of this hub are enrolled"
                fieldId="ocm-enroll-rollout"
              >
                {ROLLOUT_CHOICES.map((choice) => (
                  <Radio
                    key={choice.value}
                    id={`ocm-enroll-rollout-${choice.value}`}
                    name="ocm-enroll-rollout"
                    label={choice.label}
                    description={choice.description}
                    isChecked={rollout === choice.value}
                    onChange={() => setRollout(choice.value)}
                  />
                ))}
              </FormGroup>
              {rollout === "placement" && (
                <>
                  <FormGroup
                    label="Namespace of the Placement"
                    fieldId="ocm-enroll-placement-namespace"
                  >
                    <TextInput
                      id="ocm-enroll-placement-namespace"
                      value={placementNamespace}
                      onChange={(_event, value) => setPlacementNamespace(value)}
                    />
                  </FormGroup>
                  <FormGroup
                    label="Name of the Placement"
                    fieldId="ocm-enroll-placement-name"
                  >
                    <TextInput
                      id="ocm-enroll-placement-name"
                      value={placementName}
                      onChange={(_event, value) => setPlacementName(value)}
                    />
                  </FormGroup>
                  {problem && <Content component="small">{problem}</Content>}
                </>
              )}
              <FormGroup
                label="What the resource manager may do on an enrolled cluster"
                fieldId="ocm-enroll-profile"
              >
                <FormSelect
                  id="ocm-enroll-profile"
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
                id="ocm-enroll-platform-delivery"
                label="Let the stored credential deliver deployments that are not signed"
                description="Without this, the stored credential delivers signed deployments only, and a deployment that is not signed uses its caller's own token."
                isChecked={grantPlatformDelivery}
                onChange={(_event, checked) =>
                  setGrantPlatformDelivery(checked)
                }
              />
              <Checkbox
                id="ocm-enroll-remove-access"
                label="Withdraw a cluster when the add-on is disabled for it"
                description="Without this, disabling the add-on removes its agent and the cluster stays enrolled."
                isChecked={removeAccessWithAddon}
                onChange={(_event, checked) =>
                  setRemoveAccessWithAddon(checked)
                }
              />
              <Content component="small">
                Generating creates an enrollment bundle for this hub and returns
                the add-on that carries it. Nothing changes on the hub or on any
                cluster until the hub&apos;s administrator applies it.
              </Content>
              {error && (
                <Alert
                  variant="danger"
                  isInline
                  title="The add-on was not generated"
                >
                  {error}
                </Alert>
              )}
            </Form>
          )}
        </ModalBody>
        <ModalFooter>
          {addon && !revoked && (
            <Button
              variant="secondary"
              isDanger
              onClick={() => setConfirmRevoke(true)}
              isDisabled={revoking || generating}
            >
              Revoke bundle
            </Button>
          )}
          {!addon && (
            <Button
              variant="primary"
              onClick={() => void generate()}
              isDisabled={generating || problem !== null}
              isLoading={generating}
            >
              Generate add-on
            </Button>
          )}
          {revoked && (
            <Button variant="secondary" onClick={startOver}>
              Generate another
            </Button>
          )}
          <Button
            variant={addon && !revoked ? "primary" : "link"}
            onClick={onClose}
            isDisabled={!canClose}
          >
            {addon ? "Close" : "Cancel"}
          </Button>
        </ModalFooter>
      </Modal>

      <Modal
        isOpen={confirmRevoke}
        onClose={revoking ? undefined : () => setConfirmRevoke(false)}
        variant="small"
      >
        <ModalHeader
          title="Revoke bundle"
          description={`Revoke bundle "${addon?.bundle.id ?? ""}"? Its manifest stops enrolling clusters of ${hubId}. This cannot be undone.`}
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
