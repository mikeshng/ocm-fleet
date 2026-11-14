import "./ClusterDetailPage.scss";

import type { ClusterDetailTabProps, ClusterResource } from "@ocm-fleet/common";
import {
  clusterRecordFilter,
  createApiClient,
  createResourceApi,
  mergeFleetClusters,
  observedRecordFilter,
  PluginLink,
  usePluginNavigate,
} from "@ocm-fleet/common";
import type { Extension } from "@openshift/dynamic-plugin-sdk";
import {
  useExtensions,
  useResolvedExtensions,
} from "@openshift/dynamic-plugin-sdk";
import { PageHeader } from "@patternfly/react-component-groups/dist/dynamic/PageHeader";
import {
  Alert,
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Dropdown,
  DropdownItem,
  DropdownList,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  Grid,
  GridItem,
  Label,
  MenuToggle,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Spinner,
  Tab,
  Tabs,
  TabTitleText,
  Title,
} from "@patternfly/react-core";
import { PauseCircleIcon } from "@patternfly/react-icons";
import type { ComponentType } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import {
  buildAddonBasePath,
  clusterCreatedTime,
  type ClusterRow,
  clusterRowStatus,
  deriveClusterState,
  extractService,
  formatTime,
  originLabel,
} from "./clusterTypes";

const clusterApi = createResourceApi<ClusterResource>("-");

interface ResolvedTab {
  id: string;
  title: string;
  eventKey: string;
  priority: number;
  service?: string;
  Component: ComponentType<ClusterDetailTabProps>;
}

const CLUSTER_DETAIL_TAB_TYPE = "ocm-fleet.cluster-detail-tab";

function isClusterDetailTabExtension(
  e: Extension,
): e is Extension<"ocm-fleet.cluster-detail-tab"> {
  return e.type === CLUSTER_DETAIL_TAB_TYPE;
}

function text(value: unknown): string {
  return typeof value === "string" ? value : "";
}

/** The facts shown under "Cluster Information", by the cluster's origin. */
function clusterFacts(row: ClusterRow): [string, string][] {
  const cluster = row.result.resource;
  const observation = cluster.observation ?? {};
  const facts: [string, string][] = [
    ["Cluster ID", row.id],
    ["UID", cluster.uid],
    ["Origin", originLabel(row)],
    ["Created", formatTime(clusterCreatedTime(row))],
    ["Updated", formatTime(cluster.updateTime)],
  ];
  if (row.origin === "enrolled") {
    facts.push(
      ["API server", text(observation.apiServer)],
      ["Kubernetes version", text(observation.kubernetesVersion)],
      ["Access", text(observation.profile)],
      ["Deployment target", text(observation.target)],
      ["Stored credential delivers", text(observation.deliveryCredential)],
      ["Enrolled by", text(observation.enrolledBy)],
      [
        "Credential expires",
        formatTime(text(observation.credentialExpiresAt) || undefined),
      ],
    );
    if (text(observation.state) === "withdrawn") {
      facts.push(
        ["Withdrawn", formatTime(text(observation.withdrawnAt) || undefined)],
        ["Withdrawn by", text(observation.withdrawnBy)],
        ["Reason", text(observation.withdrawnReason)],
      );
    }
  }
  if (row.observed) {
    const hubView = row.observed.resource.observation ?? {};
    facts.push(
      ["Hub", row.hub ?? ""],
      ["Name on the hub", text(hubView.name)],
      ["Cluster set", text(hubView.clusterSet)],
    );
    if (row.origin === "observed") {
      facts.push(["Kubernetes version", text(hubView.kubernetesVersion)]);
    }
  }
  return facts.filter(([, value]) => value && value !== "—");
}

function OverviewTab({ row }: { row: ClusterRow }) {
  const cluster = row.result.resource;
  const status = clusterRowStatus(row);
  const isKind =
    extractService(row.result.name) === "kind.open-cluster-management.io";
  const enrolledState =
    row.origin === "enrolled" ? text(cluster.observation?.state) : "";
  const apiCondition = isKind
    ? cluster.conditions?.APIServerAvailable
    : row.origin === "enrolled"
      ? cluster.conditions?.Reachable
      : undefined;

  return (
    <div className="ocm-fleet-core-overview-layout">
      {row.origin === "observed" && (
        <Alert
          variant="info"
          isInline
          title="Observed through a hub, not enrolled"
        >
          Hub {row.hub} reports this cluster. Its resources are not indexed and
          nothing can be deployed to it from here until it is enrolled.
          Enrollment starts on the Hubs page and changes nothing on the hub
          until its administrator applies it.
        </Alert>
      )}
      {enrolledState === "needs-enrollment" && (
        <Alert variant="warning" isInline title="Needs enrollment">
          The stored credential for this cluster no longer works. Indexing and
          delivery resume when the cluster is enrolled again.
        </Alert>
      )}
      {enrolledState === "withdrawn" && (
        <Alert variant="warning" isInline title="Withdrawn">
          This cluster was withdrawn. It is no longer indexed and nothing is
          delivered to it. What was delivered before stays on the cluster.
        </Alert>
      )}
      <Grid hasGutter>
        <GridItem span={4}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Status</Content>
              <div className="pf-v6-u-mt-sm">
                <Label color={status.color}>
                  {status.text}
                  {cluster.reconciling ? " (reconciling)" : ""}
                </Label>
              </div>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={4}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">Origin</Content>
              <Title headingLevel="h2" size="2xl">
                {originLabel(row)}
              </Title>
            </CardBody>
          </Card>
        </GridItem>
        <GridItem span={4}>
          <Card isCompact isFullHeight>
            <CardBody>
              <Content component="p">API Server</Content>
              <div className="pf-v6-u-mt-sm">
                {apiCondition ? (
                  <Label
                    color={apiCondition.status === "True" ? "green" : "grey"}
                    isCompact
                  >
                    {apiCondition.status === "True"
                      ? "Available"
                      : "Unavailable"}
                  </Label>
                ) : (
                  <Label color="grey" isCompact>
                    {row.origin === "observed" ? "Not connected" : "—"}
                  </Label>
                )}
              </div>
            </CardBody>
          </Card>
        </GridItem>
      </Grid>

      <Card>
        <CardBody>
          <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
            Cluster Information
          </Title>
          <DescriptionList columnModifier={{ default: "2Col" }}>
            {clusterFacts(row).map(([term, value]) => (
              <DescriptionListGroup key={term}>
                <DescriptionListTerm>{term}</DescriptionListTerm>
                <DescriptionListDescription>{value}</DescriptionListDescription>
              </DescriptionListGroup>
            ))}
          </DescriptionList>
        </CardBody>
      </Card>

      {cluster.conditions && Object.keys(cluster.conditions).length > 0 && (
        <Card>
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Conditions
            </Title>
            <DescriptionList isHorizontal>
              {Object.entries(cluster.conditions).map(([type, cond]) => (
                <DescriptionListGroup key={type}>
                  <DescriptionListTerm>{type}</DescriptionListTerm>
                  <DescriptionListDescription>
                    <Label
                      isCompact
                      color={cond.status === "True" ? "green" : "grey"}
                    >
                      {cond.status}
                    </Label>
                    {cond.reason && (
                      <span className="pf-v6-u-ml-sm">{cond.reason}</span>
                    )}
                  </DescriptionListDescription>
                </DescriptionListGroup>
              ))}
            </DescriptionList>
          </CardBody>
        </Card>
      )}

      {cluster.pauseReason && (
        <Card>
          <CardBody>
            <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
              Pause Reason
            </Title>
            <Content component="p">{cluster.pauseReason}</Content>
          </CardBody>
        </Card>
      )}
    </div>
  );
}

export default function ClusterDetailPage() {
  const { clusterId } = useParams<{ clusterId: string }>();
  const clusters = usePluginNavigate("core-plugin", "ClustersModule");
  const [row, setRow] = useState<ClusterRow | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const activeTab = searchParams.get("tab") ?? "overview";
  const setActiveTab = useCallback(
    (key: string | number) => {
      const tab = String(key);
      setSearchParams(
        (prev) => {
          if (tab === "overview") {
            prev.delete("tab");
          } else {
            prev.set("tab", tab);
          }
          return prev;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );
  const [actionsOpen, setActionsOpen] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const requestIdRef = useRef(0);

  const tabExtensionCandidates = useExtensions(isClusterDetailTabExtension);
  const [tabExtensions] = useResolvedExtensions(tabExtensionCandidates);
  const resolvedTabs = useMemo<ResolvedTab[]>(() => {
    const tabs: ResolvedTab[] = [];
    for (const ext of tabExtensions) {
      const p = ext.properties as unknown as {
        id: string;
        title: string;
        eventKey: string;
        priority?: number;
        service?: string;
        component: ComponentType<ClusterDetailTabProps>;
      };
      tabs.push({
        id: p.id,
        title: p.title,
        eventKey: p.eventKey,
        priority: p.priority ?? 100,
        service: p.service,
        Component: p.component,
      });
    }
    return tabs.sort((a, b) => a.priority - b.priority);
  }, [tabExtensions]);

  const service = row ? extractService(row.result.name) : "";

  const fetchCluster = useCallback(
    async (silent = false) => {
      if (!clusterId) return;
      const id = ++requestIdRef.current;
      if (!silent) setError(null);
      try {
        // The cluster's own record and the record a connected hub has of
        // it are different types; either may be the only one there is.
        const [own, observed] = await Promise.all([
          clusterApi.search({
            filter: clusterRecordFilter(clusterId),
            pageSize: 2,
          }),
          clusterApi.search({
            filter: observedRecordFilter(clusterId),
            pageSize: 5,
          }),
        ]);
        if (id !== requestIdRef.current) return;
        const found = mergeFleetClusters([
          ...own.resources,
          ...observed.resources,
        ]).find((r) => r.id === clusterId);
        if (found) {
          setRow(found);
        } else {
          if (silent) setRow(null);
          else setError("Cluster not found");
        }
      } catch (e) {
        if (id !== requestIdRef.current) return;
        if (!silent) {
          const message =
            e instanceof Error ? e.message : "Failed to load cluster";
          setError(message);
        }
      } finally {
        if (id === requestIdRef.current && !silent) setLoading(false);
      }
    },
    [clusterId],
  );

  useEffect(() => {
    fetchCluster();
  }, [fetchCluster]);

  const status = row ? clusterRowStatus(row) : undefined;
  const isTransient = status?.transient ?? false;

  useEffect(() => {
    if (!isTransient) return;
    const id = setInterval(() => fetchCluster(true), 5000);
    return () => clearInterval(id);
  }, [isTransient, fetchCluster]);

  const clusterName = useMemo(() => clusterId ?? "", [clusterId]);

  // Only a cluster that was created here can be deleted here. An enrolled
  // cluster is withdrawn, and an observed one belongs to its hub.
  const canDelete =
    row?.origin === "created" &&
    deriveClusterState(row.result.resource) !== "DELETING";

  const handleDelete = async () => {
    if (!clusterId || !service || !canDelete) return;
    setIsDeleting(true);
    setShowDeleteModal(false);
    try {
      const client = createApiClient(buildAddonBasePath(service));
      await client.delete(`/clusters/${clusterId}`);
      clusters.navigate();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
      setIsDeleting(false);
    }
  };

  if (loading) {
    return <Spinner aria-label="Loading cluster details" />;
  }

  if (error || !row || !status) {
    return (
      <EmptyState titleText={error ?? "Cluster not found"} headingLevel="h1">
        <EmptyStateBody>
          The requested cluster could not be loaded.
        </EmptyStateBody>
        <EmptyStateFooter>
          <PluginLink scope="core-plugin" module="ClustersModule">
            Back to Clusters
          </PluginLink>
        </EmptyStateFooter>
      </EmptyState>
    );
  }

  // A tab that names a service is for clusters that have a record from
  // that service, their own or the hub's. The other tabs show what is
  // indexed from inside a cluster, so they need a cluster that is indexed.
  const services = new Set([
    service,
    row.observed ? extractService(row.observed.name) : service,
  ]);
  const tabElements = resolvedTabs
    .filter((tab) => (tab.service ? services.has(tab.service) : row.indexed))
    .map((tab) => (
      <Tab
        key={tab.eventKey}
        eventKey={tab.eventKey}
        title={<TabTitleText>{tab.title}</TabTitleText>}
      >
        <div className="pf-v6-u-pt-md">
          <tab.Component clusterId={clusterId} />
        </div>
      </Tab>
    ));

  return (
    <div className="ocm-fleet-core-detail-layout">
      <PageHeader
        title={clusterName}
        subtitle={`Created ${formatTime(clusterCreatedTime(row))}`}
        label={
          <Label
            color={status.color}
            isCompact
            className="pf-v6-u-mr-sm"
            icon={status.paused ? <PauseCircleIcon /> : undefined}
          >
            {status.text}
            {row.result.resource.reconciling ? " (reconciling)" : ""}
          </Label>
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem
              render={({ className, ariaCurrent }) => (
                <PluginLink
                  scope="core-plugin"
                  module="ClustersModule"
                  className={className}
                  aria-current={ariaCurrent}
                >
                  Clusters
                </PluginLink>
              )}
            />
            <BreadcrumbItem isActive>{clusterName}</BreadcrumbItem>
          </Breadcrumb>
        }
        actionMenu={
          canDelete ? (
            <Dropdown
              isOpen={actionsOpen}
              onOpenChange={setActionsOpen}
              toggle={(toggleRef) => (
                <MenuToggle
                  ref={toggleRef}
                  onClick={() => setActionsOpen((prev) => !prev)}
                  variant="primary"
                  isDisabled={isDeleting}
                >
                  Actions
                </MenuToggle>
              )}
              popperProps={{ position: "end" }}
            >
              <DropdownList>
                <DropdownItem
                  key="delete"
                  isDanger
                  onClick={() => {
                    setActionsOpen(false);
                    setShowDeleteModal(true);
                  }}
                >
                  Delete cluster
                </DropdownItem>
              </DropdownList>
            </Dropdown>
          ) : undefined
        }
      />

      <Tabs
        mountOnEnter
        unmountOnExit
        activeKey={activeTab}
        onSelect={(_e, key) => setActiveTab(key)}
      >
        {[
          <Tab
            key="overview"
            eventKey="overview"
            title={<TabTitleText>Overview</TabTitleText>}
          >
            <div className="pf-v6-u-pt-md">
              <OverviewTab row={row} />
            </div>
          </Tab>,
          ...tabElements,
        ]}
      </Tabs>

      <Modal
        isOpen={showDeleteModal}
        onClose={() => setShowDeleteModal(false)}
        variant="small"
      >
        <ModalHeader
          title="Delete cluster"
          description={`Are you sure you want to delete "${clusterName}"? This will terminate the provisioned cluster.`}
        />
        <ModalBody />
        <ModalFooter>
          <Button
            variant="danger"
            onClick={handleDelete}
            isLoading={isDeleting}
            isDisabled={isDeleting}
          >
            Delete
          </Button>
          <Button
            variant="link"
            onClick={() => setShowDeleteModal(false)}
            isDisabled={isDeleting}
          >
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </div>
  );
}
