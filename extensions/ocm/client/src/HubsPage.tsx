import {
  observedClusterRouteId,
  observedClusterStatus,
  PluginLink,
} from "@ocm-fleet/common";
import {
  Alert,
  Bullseye,
  Button,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  Flex,
  FlexItem,
  Label,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Spinner,
  Title,
} from "@patternfly/react-core";
import {
  ActionsColumn,
  ExpandableRowContent,
  Table,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from "@patternfly/react-table";
import { useCallback, useEffect, useState } from "react";

import {
  disconnectHub,
  hubIdOf,
  type HubResult,
  listHubClusters,
  listHubs,
  type ObservedClusterResult,
  unavailableClusterCount,
} from "./api";
import EnrollClustersModal from "./EnrollClustersModal";
import { enrollmentState } from "./enrollmentRollout";
import HubConnectForm from "./HubConnectForm";

type LabelColor = "blue" | "green" | "orange" | "red" | "grey";

const ENROLLMENT_LABELS: Record<string, { text: string; color: LabelColor }> = {
  observed: { text: "Observed", color: "grey" },
  enrolled: { text: "Enrolled", color: "blue" },
  created: { text: "Created here", color: "blue" },
  withdrawn: { text: "Withdrawn", color: "orange" },
};

function hubStatus(hub: HubResult): { text: string; color: LabelColor } {
  const conditions = hub.resource.conditions ?? {};
  if (conditions.Ready?.status === "True") {
    return { text: "Connected", color: "green" };
  }
  if (conditions.CredentialValid?.status === "False") {
    return { text: "Credential not valid", color: "red" };
  }
  if (conditions.Connected?.status === "False") {
    return { text: "Not reachable", color: "orange" };
  }
  if (conditions.Synced?.status === "False") {
    return { text: "Reading", color: "blue" };
  }
  return { text: "Not ready", color: "orange" };
}

function formatTime(iso: string | undefined): string {
  return iso ? new Date(iso).toLocaleString() : "—";
}

function HubClusters({ hubId }: { hubId: string }) {
  const [clusters, setClusters] = useState<ObservedClusterResult[] | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    listHubClusters(hubId)
      .then((results) => {
        if (!cancelled) setClusters(results);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(
            e instanceof Error ? e.message : "The clusters did not load",
          );
        }
      });
    return () => {
      cancelled = true;
    };
  }, [hubId]);

  if (error) return <Alert variant="warning" isInline title={error} />;
  if (!clusters) return <Spinner size="md" aria-label="Loading clusters" />;
  if (clusters.length === 0) {
    return <Content component="small">This hub has no clusters.</Content>;
  }

  return (
    <Table aria-label={`Clusters of ${hubId}`} variant="compact">
      <Thead>
        <Tr>
          <Th>Cluster</Th>
          <Th>Cluster set</Th>
          <Th>On the hub</Th>
          <Th>Here</Th>
        </Tr>
      </Thead>
      <Tbody>
        {clusters.map((cluster) => {
          const observation = cluster.resource.observation;
          const name = observation?.name ?? cluster.resource.name;
          const onHub = observedClusterStatus(cluster);
          const here =
            ENROLLMENT_LABELS[observation?.enrollment?.state ?? "observed"] ??
            ENROLLMENT_LABELS.observed;
          return (
            <Tr key={cluster.resource.name}>
              <Td dataLabel="Cluster">
                <PluginLink
                  scope="core-plugin"
                  module="ClustersModule"
                  to={observedClusterRouteId(cluster)}
                  fallback={name}
                >
                  {name}
                </PluginLink>
              </Td>
              <Td dataLabel="Cluster set">{observation?.clusterSet || "—"}</Td>
              <Td dataLabel="On the hub">
                <Label color={onHub.color} isCompact>
                  {onHub.text}
                </Label>
              </Td>
              <Td dataLabel="Here">
                <Label color={here.color} isCompact>
                  {here.text}
                </Label>
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
}

export default function HubsPage() {
  const [hubs, setHubs] = useState<HubResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [connectOpen, setConnectOpen] = useState(false);
  const [connectBusy, setConnectBusy] = useState(false);
  const [enrollHubId, setEnrollHubId] = useState<string | null>(null);
  const [disconnectId, setDisconnectId] = useState<string | null>(null);
  const [disconnecting, setDisconnecting] = useState(false);

  const fetchHubs = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      setHubs(await listHubs());
      setError(null);
    } catch (e) {
      // A failed refresh leaves the table as it was; say so instead of
      // letting the stale rows look current.
      setError(e instanceof Error ? e.message : "The hubs did not load");
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void fetchHubs();
  }, [fetchHubs]);

  // A hub that was just connected is still being read: follow it.
  const settling = hubs.some(
    (hub) => hub.resource.conditions?.Synced?.status !== "True",
  );
  useEffect(() => {
    if (!settling) return;
    const id = setInterval(() => void fetchHubs(true), 5000);
    return () => clearInterval(id);
  }, [settling, fetchHubs]);

  const disconnect = async () => {
    if (!disconnectId) return;
    setDisconnecting(true);
    try {
      await disconnectHub(disconnectId);
      setDisconnectId(null);
      await fetchHubs(true);
    } catch (e) {
      setDisconnectId(null);
      setError(e instanceof Error ? e.message : "The hub was not disconnected");
    } finally {
      setDisconnecting(false);
    }
  };

  if (loading) {
    return (
      <Bullseye>
        <Spinner aria-label="Loading hubs" />
      </Bullseye>
    );
  }

  const someAway = hubs.some((hub) => {
    const status = hubStatus(hub).text;
    return status === "Not reachable" || status === "Credential not valid";
  });

  return (
    <div>
      <Flex
        justifyContent={{ default: "justifyContentSpaceBetween" }}
        alignItems={{ default: "alignItemsFlexStart" }}
        className="pf-v6-u-mb-lg"
      >
        <FlexItem>
          <Title headingLevel="h1">Hubs</Title>
          <Content component="p">
            Open Cluster Management hubs that this console reads. A connection
            shows the hub&apos;s clusters and writes nothing to the hub.
          </Content>
        </FlexItem>
        <FlexItem>
          <Button variant="primary" onClick={() => setConnectOpen(true)}>
            Connect hub
          </Button>
        </FlexItem>
      </Flex>

      {error && hubs.length > 0 && (
        <Alert
          variant="warning"
          title={error}
          isInline
          className="pf-v6-u-mb-md"
        />
      )}

      {someAway && (
        <Alert
          variant="info"
          title="The numbers beside a hub that does not answer are the ones it last reported."
          isInline
          className="pf-v6-u-mb-md"
        />
      )}

      {error && hubs.length === 0 ? (
        <EmptyState titleText="The hubs did not load" headingLevel="h2">
          <EmptyStateBody>{error}</EmptyStateBody>
          <EmptyStateFooter>
            <EmptyStateActions>
              <Button variant="primary" onClick={() => void fetchHubs()}>
                Retry
              </Button>
            </EmptyStateActions>
          </EmptyStateFooter>
        </EmptyState>
      ) : hubs.length === 0 ? (
        <EmptyState titleText="No hub is connected" headingLevel="h2">
          <EmptyStateBody>
            Connect a hub to see its clusters next to the ones created here, and
            to enroll the ones you choose.
          </EmptyStateBody>
          <EmptyStateFooter>
            <EmptyStateActions>
              <Button variant="primary" onClick={() => setConnectOpen(true)}>
                Connect hub
              </Button>
            </EmptyStateActions>
          </EmptyStateFooter>
        </EmptyState>
      ) : (
        <Table aria-label="Connected hubs" variant="compact">
          <Thead>
            <Tr>
              <Th screenReaderText="Row expansion" />
              <Th>Hub</Th>
              <Th>API address</Th>
              <Th>Status</Th>
              <Th>Clusters</Th>
              <Th>Available</Th>
              <Th>Enrolled</Th>
              <Th>Last heard from</Th>
              <Th screenReaderText="Actions" />
            </Tr>
          </Thead>
          {hubs.map((hub, rowIndex) => {
            const id = hubIdOf(hub);
            const observation = hub.resource.observation;
            const status = hubStatus(hub);
            const enrollment = enrollmentState(observation);
            const isExpanded = expandedId === id;
            return (
              <Tbody key={id} isExpanded={isExpanded}>
                <Tr>
                  <Td
                    expand={{
                      rowIndex,
                      isExpanded,
                      onToggle: () => setExpandedId(isExpanded ? null : id),
                    }}
                  />
                  <Td dataLabel="Hub">
                    <span className="pf-v6-u-font-weight-bold">{id}</span>
                    {observation?.displayName && (
                      <span className="pf-v6-u-ml-sm pf-v6-u-text-color-subtle">
                        {observation.displayName}
                      </span>
                    )}
                  </Td>
                  <Td dataLabel="API address">
                    {observation?.apiServer ?? "—"}
                  </Td>
                  <Td dataLabel="Status">
                    <Label color={status.color} isCompact>
                      {status.text}
                    </Label>
                  </Td>
                  <Td dataLabel="Clusters">{observation?.clusterCount ?? 0}</Td>
                  <Td dataLabel="Available">
                    {unavailableClusterCount(observation) > 0 ? (
                      <Label color="orange" isCompact>
                        {observation?.availableCount ?? 0}
                      </Label>
                    ) : (
                      (observation?.availableCount ?? 0)
                    )}
                  </Td>
                  <Td dataLabel="Enrolled">
                    {observation?.enrolledCount ?? 0}
                  </Td>
                  <Td dataLabel="Last heard from">
                    {formatTime(observation?.lastSeenAt)}
                  </Td>
                  <Td isActionCell>
                    <Flex
                      flexWrap={{ default: "nowrap" }}
                      alignItems={{ default: "alignItemsCenter" }}
                    >
                      <FlexItem>
                        <Button
                          variant="secondary"
                          size="sm"
                          onClick={() => setEnrollHubId(id)}
                        >
                          Enroll clusters
                        </Button>
                      </FlexItem>
                      <FlexItem>
                        <ActionsColumn
                          items={[
                            {
                              title: "Disconnect",
                              onClick: () => setDisconnectId(id),
                            },
                          ]}
                        />
                      </FlexItem>
                    </Flex>
                  </Td>
                </Tr>
                <Tr isExpanded={isExpanded}>
                  <Td />
                  <Td colSpan={8}>
                    <ExpandableRowContent>
                      {observation?.writeAccessFound && (
                        <Alert
                          variant="warning"
                          isInline
                          title="The credential in use can change this hub"
                          className="pf-v6-u-mb-md"
                        >
                          Reading is all a connection needs. Give the account
                          the read-only role of the access manifest.
                        </Alert>
                      )}
                      {enrollment.warning && (
                        <Alert
                          variant="warning"
                          isInline
                          title="The add-on on this hub has to be made again"
                          className="pf-v6-u-mb-md"
                        >
                          {enrollment.warning}
                        </Alert>
                      )}
                      <DescriptionList
                        isHorizontal
                        isCompact
                        className="pf-v6-u-mb-md"
                      >
                        <DescriptionListGroup>
                          <DescriptionListTerm>Account</DescriptionListTerm>
                          <DescriptionListDescription>
                            {observation?.serviceAccount || "—"}
                          </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                          <DescriptionListTerm>
                            Credential expires
                          </DescriptionListTerm>
                          <DescriptionListDescription>
                            {formatTime(observation?.credentialExpiresAt)}
                          </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                          <DescriptionListTerm>Connected</DescriptionListTerm>
                          <DescriptionListDescription>
                            {formatTime(observation?.connectedAt)}
                          </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                          <DescriptionListTerm>
                            Enrollment add-on reaches
                          </DescriptionListTerm>
                          <DescriptionListDescription>
                            {enrollment.summary}
                          </DescriptionListDescription>
                        </DescriptionListGroup>
                      </DescriptionList>
                      {isExpanded && <HubClusters hubId={id} />}
                    </ExpandableRowContent>
                  </Td>
                </Tr>
              </Tbody>
            );
          })}
        </Table>
      )}

      {connectOpen && (
        <Modal
          isOpen
          // Closing in flight would discard the answer, including its error.
          onClose={connectBusy ? undefined : () => setConnectOpen(false)}
          variant="large"
        >
          <ModalHeader title="Connect an OCM hub" />
          <ModalBody>
            <HubConnectForm
              onConnected={() => {
                setConnectOpen(false);
                void fetchHubs(true);
              }}
              onCancel={() => setConnectOpen(false)}
              onBusyChange={setConnectBusy}
            />
          </ModalBody>
        </Modal>
      )}

      {enrollHubId && (
        <EnrollClustersModal
          hubId={enrollHubId}
          lastRollout={
            hubs.find((hub) => hubIdOf(hub) === enrollHubId)?.resource
              .observation?.enrollmentRollout
          }
          onClose={() => {
            setEnrollHubId(null);
            void fetchHubs(true);
          }}
        />
      )}

      <Modal
        isOpen={disconnectId !== null}
        onClose={() => setDisconnectId(null)}
        variant="small"
      >
        <ModalHeader
          title="Disconnect hub"
          description={`Disconnect "${disconnectId}"? Its clusters are no longer listed as observed, and its enrollment bundles stop working. Clusters that are enrolled stay enrolled. Nothing changes on the hub.`}
        />
        <ModalBody />
        <ModalFooter>
          <Button
            variant="danger"
            onClick={() => void disconnect()}
            isLoading={disconnecting}
            isDisabled={disconnecting}
          >
            Disconnect
          </Button>
          <Button
            variant="link"
            onClick={() => setDisconnectId(null)}
            isDisabled={disconnecting}
          >
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </div>
  );
}
