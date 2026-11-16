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
  AlertActionCloseButton,
  AlertActionLink,
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
  EmptyStateActions,
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

import { pollingPaused, shouldPoll } from "./clusterPolling";
import {
  buildAddonBasePath,
  clusterCreatedTime,
  type ClusterRow,
  clusterRowStatus,
  deriveClusterState,
  extractService,
  formatTime,
  hubFactsNotice,
  hubViewNotice,
  originLabel,
} from "./clusterTypes";
import DeleteClusterModal from "./DeleteClusterModal";
import {
  credentialExpiresAt,
  enrollmentActions,
  forgetCluster,
  renewClusterCredential,
} from "./enrollmentApi";
import { refetchAfterAction } from "./refetchAfterAction";
import WithdrawClusterModal from "./WithdrawClusterModal";

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
  // Withdrawal removes the stored credential and the target
  // (EnrollmentService.release); the record keeps them nowhere, so the
  // facts that only describe the ended enrollment are left out.
  const withdrawn =
    row.origin === "enrolled" && text(observation.state) === "withdrawn";
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
  return facts
    .filter(([, value]) => value && value !== "—")
    .filter(([term]) => !(withdrawn && term === "Stored credential delivers"));
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
  const hubFacts =
    row.origin === "enrolled" && enrolledState !== "withdrawn"
      ? hubFactsNotice(cluster.conditions)
      : null;
  const hubView = enrolledState !== "withdrawn" ? hubViewNotice(row) : null;

  return (
    <div className="ocm-fleet-core-overview-layout">
      {hubFacts && (
        <Alert variant="warning" isInline title={hubFacts.title}>
          {hubFacts.body}
        </Alert>
      )}
      {hubView && (
        <Alert variant="warning" isInline title={hubView.title}>
          {hubView.body}
        </Alert>
      )}
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
                {status.onHub && (
                  <Label color={status.onHub.color} className="pf-v6-u-ml-sm">
                    On its hub: {status.onHub.text}
                  </Label>
                )}
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
                {enrolledState === "withdrawn" ? (
                  <Label color="grey" isCompact>
                    Credential removed
                  </Label>
                ) : apiCondition ? (
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
  const [showWithdrawModal, setShowWithdrawModal] = useState(false);
  const [showForgetModal, setShowForgetModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isRenewing, setIsRenewing] = useState(false);
  const [isForgetting, setIsForgetting] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [silentFailCount, setSilentFailCount] = useState(0);
  const [refreshError, setRefreshError] = useState<string | null>(null);
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
    async (silent = false): Promise<boolean> => {
      if (!clusterId) return false;
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
        if (id !== requestIdRef.current) return false;
        // The backend answered, so polling may resume after a run of
        // transient failures.
        setSilentFailCount(0);
        const found = mergeFleetClusters([
          ...own.resources,
          ...observed.resources,
        ]).find((r) => r.id === clusterId);
        if (found) {
          setRow(found);
          return true;
        }
        if (silent) setRow(null);
        else setError("Cluster not found");
        return false;
      } catch (e) {
        if (id !== requestIdRef.current) return false;
        if (silent) {
          setSilentFailCount((c) => c + 1);
        } else {
          const message =
            e instanceof Error ? e.message : "Failed to load cluster";
          setError(message);
        }
        return false;
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
    if (!shouldPoll(isTransient, silentFailCount)) return;
    const id = setInterval(() => fetchCluster(true), 5000);
    return () => clearInterval(id);
  }, [isTransient, silentFailCount, fetchCluster]);

  const clusterName = useMemo(() => clusterId ?? "", [clusterId]);

  // A manual refresh while polling is paused reads the record again
  // without replacing the page: if the read fails, the row and the
  // paused warning stay, and the failure is reported in the warning.
  const refreshNow = async () => {
    setRefreshError(null);
    if (!(await fetchCluster(true))) {
      setRefreshError(
        "The refresh did not reach the server; the page still shows the record from the last successful read.",
      );
    }
  };

  // Only a cluster that was created here can be deleted here. An enrolled
  // cluster is withdrawn, and an observed one belongs to its hub.
  const canDelete =
    row?.origin === "created" &&
    deriveClusterState(row.result.resource) !== "DELETING";

  const handleDelete = async () => {
    if (!clusterId || !service || !canDelete) return;
    setIsDeleting(true);
    setShowDeleteModal(false);
    setActionError(null);
    try {
      const client = createApiClient(buildAddonBasePath(service));
      await client.delete(`/clusters/${clusterId}`);
      clusters.navigate();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Delete failed");
      setIsDeleting(false);
    }
  };

  const handleRenew = async () => {
    if (!row) return;
    setIsRenewing(true);
    setActionError(null);
    try {
      const renewed = await renewClusterCredential(
        extractService(row.result.name),
        row.id,
      );
      const expiresAt = credentialExpiresAt(renewed);
      setNotice(
        expiresAt
          ? `Credential renewed; it expires ${formatTime(expiresAt)}`
          : "Credential renewed",
      );
      // The renewal answer names the new expiry, but the record the page
      // shows is read again here. A failed read must not leave the old
      // expiry looking current.
      await refetchAfterAction(
        () => fetchCluster(true),
        setActionError,
        "The credential was renewed, but the cluster record could not be read again; reload the page to see its new expiry.",
      );
    } catch (e) {
      setActionError(
        e instanceof Error ? e.message : "The credential was not renewed",
      );
    } finally {
      setIsRenewing(false);
    }
  };

  const handleForget = async () => {
    if (!row) return;
    setIsForgetting(true);
    setShowForgetModal(false);
    setActionError(null);
    try {
      await forgetCluster(extractService(row.result.name), row.id);
      clusters.navigate();
    } catch (e) {
      setActionError(
        e instanceof Error ? e.message : "The record was not forgotten",
      );
      setIsForgetting(false);
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
          <EmptyStateActions>
            <Button
              variant="primary"
              onClick={() => {
                if (!clusterId) return;
                setLoading(true);
                void fetchCluster();
              }}
            >
              Retry
            </Button>
            <PluginLink scope="core-plugin" module="ClustersModule">
              Back to Clusters
            </PluginLink>
          </EmptyStateActions>
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
  const visibleTabs = resolvedTabs.filter((tab) =>
    tab.service ? services.has(tab.service) : row.indexed,
  );
  const tabElements = visibleTabs.map((tab) => (
    <Tab
      key={tab.eventKey}
      eventKey={tab.eventKey}
      title={<TabTitleText>{tab.title}</TabTitleText>}
    >
      <div className="pf-v6-u-pt-md">
        <tab.Component clusterId={clusterId} hub={row.hub} />
      </div>
    </Tab>
  ));
  // A withdrawal or a lost index can drop the tab a URL names; the
  // overview stays, rather than a blank panel under a dead deep link.
  const activeKey =
    activeTab === "overview" ||
    visibleTabs.some((tab) => tab.eventKey === activeTab)
      ? activeTab
      : "overview";

  const actions = enrollmentActions(row);
  const hasActions =
    canDelete || actions.renew || actions.withdraw || actions.forget;

  return (
    <div className="ocm-fleet-core-detail-layout">
      <PageHeader
        title={clusterName}
        subtitle={`Created ${formatTime(clusterCreatedTime(row))}`}
        label={
          <>
            <Label
              color={status.color}
              isCompact
              className="pf-v6-u-mr-sm"
              icon={status.paused ? <PauseCircleIcon /> : undefined}
            >
              {status.text}
              {row.result.resource.reconciling ? " (reconciling)" : ""}
            </Label>
            {status.onHub && (
              <Label
                color={status.onHub.color}
                isCompact
                className="pf-v6-u-mr-sm"
              >
                On its hub: {status.onHub.text}
              </Label>
            )}
          </>
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
          hasActions ? (
            <Dropdown
              isOpen={actionsOpen}
              onOpenChange={setActionsOpen}
              toggle={(toggleRef) => (
                <MenuToggle
                  ref={toggleRef}
                  onClick={() => setActionsOpen((prev) => !prev)}
                  variant="primary"
                  isDisabled={isDeleting || isRenewing || isForgetting}
                >
                  Actions
                </MenuToggle>
              )}
              popperProps={{ position: "end" }}
            >
              <DropdownList>
                {actions.renew && (
                  <DropdownItem
                    key="renew"
                    onClick={() => {
                      setActionsOpen(false);
                      handleRenew();
                    }}
                    isDisabled={isRenewing}
                  >
                    {isRenewing ? "Renewing..." : "Renew credential"}
                  </DropdownItem>
                )}
                {actions.withdraw && (
                  <DropdownItem
                    key="withdraw"
                    onClick={() => {
                      setActionsOpen(false);
                      setShowWithdrawModal(true);
                    }}
                  >
                    Withdraw...
                  </DropdownItem>
                )}
                {canDelete && (
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
                )}
                {actions.forget && (
                  <DropdownItem
                    key="forget"
                    isDanger
                    onClick={() => {
                      setActionsOpen(false);
                      setShowForgetModal(true);
                    }}
                    isDisabled={isForgetting}
                  >
                    {isForgetting ? "Forgetting..." : "Forget record..."}
                  </DropdownItem>
                )}
              </DropdownList>
            </Dropdown>
          ) : undefined
        }
      />

      {actionError && (
        <Alert
          variant="danger"
          isInline
          title="The action did not complete"
          className="pf-v6-u-mt-md"
          actionClose={
            <AlertActionCloseButton
              aria-label="Close action error"
              onClose={() => setActionError(null)}
            />
          }
        >
          {actionError}
        </Alert>
      )}

      {notice && (
        <Alert
          variant="success"
          isInline
          title={notice}
          className="pf-v6-u-mt-md"
          actionClose={
            <AlertActionCloseButton
              aria-label="Close notice"
              onClose={() => setNotice(null)}
            />
          }
        />
      )}

      {pollingPaused(isTransient, silentFailCount) && (
        <Alert
          variant="warning"
          isInline
          title="Live updates are paused"
          className="pf-v6-u-mt-md"
          actionLinks={
            <AlertActionLink onClick={() => void refreshNow()}>
              Refresh now
            </AlertActionLink>
          }
        >
          The cluster could not be read again three times in a row, so this page
          is no longer updated automatically; what is shown may be out of date.
          {refreshError && <div className="pf-v6-u-mt-sm">{refreshError}</div>}
        </Alert>
      )}

      <Tabs
        mountOnEnter
        unmountOnExit
        activeKey={activeKey}
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

      <DeleteClusterModal
        clusterId={clusterName}
        isOpen={showDeleteModal}
        isDeleting={isDeleting}
        onClose={() => setShowDeleteModal(false)}
        onDelete={handleDelete}
      />

      {showWithdrawModal && (
        <WithdrawClusterModal
          key={row.id}
          isOpen
          clusterId={row.id}
          service={service}
          onClose={() => setShowWithdrawModal(false)}
          onWithdrawn={async () => {
            setShowWithdrawModal(false);
            setNotice("Cluster withdrawn");
            // The withdrawal removes the target and the cluster's actions change with it. A
            // failed read must not leave the pre-withdrawal row looking current.
            await refetchAfterAction(
              () => fetchCluster(true),
              setActionError,
              "The cluster was withdrawn, but the record could not be read again; reload the page to see its state.",
            );
          }}
        />
      )}

      <Modal
        isOpen={showForgetModal}
        onClose={() => setShowForgetModal(false)}
        variant="small"
      >
        <ModalHeader
          title="Forget cluster record"
          description={`Forget the withdrawn record for "${clusterName}"? The record is removed, so the cluster id can be enrolled again. Nothing on the cluster itself is touched.`}
        />
        <ModalBody />
        <ModalFooter>
          <Button
            variant="danger"
            onClick={handleForget}
            isLoading={isForgetting}
            isDisabled={isForgetting}
          >
            Forget
          </Button>
          <Button
            variant="link"
            onClick={() => setShowForgetModal(false)}
            isDisabled={isForgetting}
          >
            Cancel
          </Button>
        </ModalFooter>
      </Modal>
    </div>
  );
}
