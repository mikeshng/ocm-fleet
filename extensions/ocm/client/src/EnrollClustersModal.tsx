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
  Title,
} from "@patternfly/react-core";
import { useState } from "react";

import { createEnrollmentAddon, type EnrollmentAddon } from "./api";

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
  onClose: () => void;
}

/**
 * Asks what the resource manager may do on the hub's clusters, then shows
 * the add-on that the hub's administrator applies to enroll them with the
 * hub's own tools. Nothing is applied from here.
 */
export default function EnrollClustersModal({
  hubId,
  onClose,
}: EnrollClustersModalProps) {
  const [profile, setProfile] = useState(PROFILES[0].value);
  const [grantPlatformDelivery, setGrantPlatformDelivery] = useState(false);
  const [removeAccessWithAddon, setRemoveAccessWithAddon] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [addon, setAddon] = useState<EnrollmentAddon | null>(null);

  const generate = async () => {
    setGenerating(true);
    setError(null);
    try {
      setAddon(
        await createEnrollmentAddon(hubId, {
          profile,
          grantPlatformDelivery,
          removeAccessWithAddon,
        }),
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "The add-on was not generated");
    } finally {
      setGenerating(false);
    }
  };

  return (
    <Modal isOpen onClose={onClose} variant="large">
      <ModalHeader
        title={`Enroll clusters of ${hubId}`}
        description="An enrolled cluster is indexed and can be deployed to from here. The hub keeps everything it does for the cluster."
      />
      <ModalBody>
        {addon ? (
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
              className="pf-v6-u-mb-md"
            >
              {addon.manifest}
            </ClipboardCopy>
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
              className="pf-v6-u-mb-md"
            >
              {addon.enableExample}
            </ClipboardCopy>
            <List component="ol">
              {addon.instructions.map((line) => (
                <ListItem key={line}>{line}</ListItem>
              ))}
            </List>
          </>
        ) : (
          <Form>
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
              onChange={(_event, checked) => setGrantPlatformDelivery(checked)}
            />
            <Checkbox
              id="ocm-enroll-remove-access"
              label="Withdraw a cluster when the add-on is disabled for it"
              description="Without this, disabling the add-on removes its agent and the cluster stays enrolled."
              isChecked={removeAccessWithAddon}
              onChange={(_event, checked) => setRemoveAccessWithAddon(checked)}
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
        {!addon && (
          <Button
            variant="primary"
            onClick={() => void generate()}
            isDisabled={generating}
            isLoading={generating}
          >
            Generate add-on
          </Button>
        )}
        <Button variant="link" onClick={onClose} isDisabled={generating}>
          {addon ? "Close" : "Cancel"}
        </Button>
      </ModalFooter>
    </Modal>
  );
}
