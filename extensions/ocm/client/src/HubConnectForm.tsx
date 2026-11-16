import {
  ActionGroup,
  Alert,
  Button,
  ClipboardCopy,
  ClipboardCopyVariant,
  Content,
  Form,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  List,
  ListItem,
  Radio,
  Spinner,
  TextArea,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { useEffect, useState } from "react";

import {
  connectHub,
  getHubAccessManifest,
  type HubAccessManifest,
} from "./api";
import {
  connectHubRequest,
  displayNameError,
  hubIdError,
} from "./connectHubRequest";

interface HubConnectFormProps {
  onConnected: (hubId: string) => void;
  onCancel: () => void;
  /** Reports a connect in flight so the host can keep the form open. */
  onBusyChange?: (busy: boolean) => void;
}

/**
 * The two steps of connecting a hub: what the hub's administrator applies
 * first, shown so that it can be read before it is used, and then the
 * address and the credential of the account it creates.
 */
export default function HubConnectForm({
  onConnected,
  onCancel,
  onBusyChange,
}: HubConnectFormProps) {
  const [access, setAccess] = useState<HubAccessManifest | null>(null);
  const [accessError, setAccessError] = useState<string | null>(null);
  const [hubId, setHubId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [mode, setMode] = useState<"fields" | "kubeconfig">("fields");
  const [apiServer, setApiServer] = useState("");
  const [caCert, setCaCert] = useState("");
  const [token, setToken] = useState("");
  const [kubeconfig, setKubeconfig] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getHubAccessManifest()
      .then((manifest) => {
        if (!cancelled) setAccess(manifest);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setAccessError(
            e instanceof Error ? e.message : "The access manifest did not load",
          );
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const id = hubId.trim();
  const idError = hubIdError(hubId);
  const nameError = displayNameError(displayName);
  const complete =
    idError === null &&
    nameError === null &&
    (mode === "kubeconfig"
      ? kubeconfig.trim() !== ""
      : apiServer.trim() !== "" && caCert.trim() !== "" && token.trim() !== "");

  useEffect(() => {
    onBusyChange?.(submitting);
  }, [submitting, onBusyChange]);

  const submit = async () => {
    setSubmitting(true);
    setError(null);
    try {
      await connectHub(
        connectHubRequest({
          hubId,
          displayName,
          mode,
          apiServer,
          caCert,
          token,
          kubeconfig,
        }),
      );
      onConnected(id);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Connecting the hub failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Form
      onSubmit={(event) => {
        event.preventDefault();
        if (complete && !submitting) void submit();
      }}
    >
      <Title headingLevel="h2" size="lg">
        1. Give read access to the hub
      </Title>
      <Content component="p">
        The hub&apos;s administrator applies this manifest to the hub. It
        creates one read-only account. Connecting a hub writes nothing to it and
        changes nothing on its clusters.
      </Content>
      {accessError && <Alert variant="warning" isInline title={accessError} />}
      {!access && !accessError && (
        <Spinner size="md" aria-label="Loading the access manifest" />
      )}
      {access && (
        <>
          <ClipboardCopy
            isCode
            isReadOnly
            variant={ClipboardCopyVariant.expansion}
            hoverTip="Copy"
            clickTip="Copied"
            aria-label="Hub access manifest"
          >
            {access.manifest}
          </ClipboardCopy>
          <List component="ol">
            {access.instructions.map((line) => (
              <ListItem key={line}>{line}</ListItem>
            ))}
          </List>
        </>
      )}

      <Title headingLevel="h2" size="lg">
        2. Connect
      </Title>
      <FormGroup label="Hub ID" isRequired fieldId="ocm-hub-id">
        <TextInput
          id="ocm-hub-id"
          isRequired
          value={hubId}
          onChange={(_event, value) => setHubId(value)}
          validated={id !== "" && idError ? "error" : "default"}
          placeholder="hub1"
        />
        <FormHelperText>
          <HelperText>
            <HelperTextItem
              variant={id !== "" && idError ? "error" : "default"}
            >
              {id !== "" && idError
                ? idError
                : "A short name for this hub here: 1 to 40 lowercase letters, digits, or hyphens, starting and ending with a letter or digit."}
            </HelperTextItem>
          </HelperText>
        </FormHelperText>
      </FormGroup>
      <FormGroup label="Display name" fieldId="ocm-hub-display-name">
        <TextInput
          id="ocm-hub-display-name"
          value={displayName}
          onChange={(_event, value) => setDisplayName(value)}
          validated={nameError ? "error" : "default"}
          placeholder="Production hub"
        />
        <FormHelperText>
          <HelperText>
            <HelperTextItem variant={nameError ? "error" : "default"}>
              {nameError ??
                "Shown beside the hub ID in the hubs table. At most 120 bytes."}
            </HelperTextItem>
          </HelperText>
        </FormHelperText>
      </FormGroup>
      <FormGroup
        role="radiogroup"
        isInline
        fieldId="ocm-hub-mode"
        label="Connection details"
      >
        <Radio
          id="ocm-hub-mode-fields"
          name="ocm-hub-mode"
          label="Address, CA certificate, and token"
          isChecked={mode === "fields"}
          onChange={() => setMode("fields")}
        />
        <Radio
          id="ocm-hub-mode-kubeconfig"
          name="ocm-hub-mode"
          label="Kubeconfig"
          isChecked={mode === "kubeconfig"}
          onChange={() => setMode("kubeconfig")}
        />
      </FormGroup>
      {mode === "fields" ? (
        <>
          <FormGroup
            label="Hub API address"
            isRequired
            fieldId="ocm-hub-api-server"
          >
            <TextInput
              id="ocm-hub-api-server"
              isRequired
              value={apiServer}
              onChange={(_event, value) => setApiServer(value)}
              placeholder="https://hub.example.com:6443"
            />
            <FormHelperText>
              <HelperText>
                <HelperTextItem>
                  The resource manager must be able to reach this address.
                </HelperTextItem>
              </HelperText>
            </FormHelperText>
          </FormGroup>
          <FormGroup label="CA certificate" isRequired fieldId="ocm-hub-ca">
            <TextArea
              id="ocm-hub-ca"
              isRequired
              value={caCert}
              onChange={(_event, value) => setCaCert(value)}
              rows={4}
              resizeOrientation="vertical"
              placeholder="-----BEGIN CERTIFICATE-----"
            />
          </FormGroup>
          <FormGroup
            label="Token of the read-only account"
            isRequired
            fieldId="ocm-hub-token"
          >
            <TextInput
              id="ocm-hub-token"
              type="password"
              isRequired
              value={token}
              onChange={(_event, value) => setToken(value)}
            />
          </FormGroup>
        </>
      ) : (
        <FormGroup label="Kubeconfig" isRequired fieldId="ocm-hub-kubeconfig">
          <TextArea
            id="ocm-hub-kubeconfig"
            isRequired
            value={kubeconfig}
            onChange={(_event, value) => setKubeconfig(value)}
            rows={8}
            resizeOrientation="vertical"
          />
          <FormHelperText>
            <HelperText>
              <HelperTextItem>
                A kubeconfig with the hub&apos;s address, its CA certificate,
                and the token of the read-only account.
              </HelperTextItem>
            </HelperText>
          </FormHelperText>
        </FormGroup>
      )}

      {error && (
        <Alert variant="danger" isInline title="The hub was not connected">
          {error}
        </Alert>
      )}
      <ActionGroup>
        <Button
          variant="primary"
          type="submit"
          isDisabled={!complete || submitting}
          isLoading={submitting}
        >
          Connect hub
        </Button>
        <Button variant="link" onClick={onCancel} isDisabled={submitting}>
          Cancel
        </Button>
      </ActionGroup>
    </Form>
  );
}
