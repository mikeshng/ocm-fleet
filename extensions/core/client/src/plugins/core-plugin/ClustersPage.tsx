import "./ClustersPage.css";

import {
  ClusterResource,
  createApiClient,
  createResourceApi,
  fleetClusterTypeFilter,
  mergeFleetClusters,
  PluginLink,
} from "@ocm-fleet/common";
import {
  SkeletonTableBody,
  SkeletonTableHead,
} from "@patternfly/react-component-groups";
import {
  Alert,
  AlertActionCloseButton,
  AlertActionLink,
  AlertGroup,
  Button,
  Content,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  Label,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Pagination,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import {
  DataView,
  DataViewState,
} from "@patternfly/react-data-view/dist/dynamic/DataView";
import { DataViewFilters } from "@patternfly/react-data-view/dist/dynamic/DataViewFilters";
import {
  DataViewTable,
  type DataViewTh,
  type DataViewTr,
} from "@patternfly/react-data-view/dist/dynamic/DataViewTable";
import { DataViewTextFilter } from "@patternfly/react-data-view/dist/dynamic/DataViewTextFilter";
import { DataViewToolbar } from "@patternfly/react-data-view/dist/dynamic/DataViewToolbar";
import {
  useDataViewFilters,
  useDataViewPagination,
} from "@patternfly/react-data-view/dist/dynamic/Hooks";
import { CubesIcon, PauseCircleIcon } from "@patternfly/react-icons";
import {
  ActionsColumn,
  type IAction,
  Tbody,
  Td,
  Tr,
} from "@patternfly/react-table";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { pollingPaused, shouldPoll } from "./clusterPolling";
import ClusterSummaryCards from "./ClusterSummaryCards";
import {
  buildAddonBasePath,
  clusterCreatedTime,
  type ClusterListFilters,
  type ClusterRow,
  clusterRowStatus,
  deriveClusterState,
  extractService,
  formatTime,
  hubColumn,
  matchesClusterFilters,
  originLabel,
} from "./clusterTypes";
import CreateClusterModal from "./CreateClusterModal";
import DeleteClusterModal from "./DeleteClusterModal";
import EnrollManifestModal from "./EnrollManifestModal";
import {
  credentialExpiresAt,
  enrollmentActions,
  forgetCluster,
  renewClusterCredential,
} from "./enrollmentApi";
import { refetchAfterAction } from "./refetchAfterAction";
import WithdrawClusterModal from "./WithdrawClusterModal";

const columns: DataViewTh[] = [
  "Name",
  "Origin",
  "Hub",
  "Status",
  "Created",
  "",
];

const PER_PAGE_OPTIONS = [
  { title: "10", value: 10 },
  { title: "25", value: 25 },
  { title: "50", value: 50 },
];

const clusterApi = createResourceApi<ClusterResource>("-");

export default function ClustersPage() {
  const [rows, setRows] = useState<ClusterRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ClusterRow | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [withdrawTarget, setWithdrawTarget] = useState<ClusterRow | null>(null);
  const [forgetTarget, setForgetTarget] = useState<ClusterRow | null>(null);
  const [renewing, setRenewing] = useState<string | null>(null);
  const [forgetting, setForgetting] = useState<string | null>(null);
  const [enrollOpen, setEnrollOpen] = useState(false);
  const [silentFailCount, setSilentFailCount] = useState(0);
  const [loaded, setLoaded] = useState(false);

  const [searchParams, setSearchParams] = useSearchParams();
  const createParam = searchParams.get("create");
  const wizardOpen = createParam !== null;
  const preselectedProvider =
    createParam && createParam !== "" ? createParam : null;

  const openCreateWizard = useCallback(() => {
    setSearchParams((prev) => {
      prev.set("create", "");
      return prev;
    });
  }, [setSearchParams]);

  const selectProvider = useCallback(
    (providerId: string) => {
      setSearchParams((prev) => {
        prev.set("create", providerId);
        return prev;
      });
    },
    [setSearchParams],
  );

  const { filters, onSetFilters, clearAllFilters } =
    useDataViewFilters<ClusterListFilters>({
      initialFilters: { name: "", hub: "" },
    });
  const pagination = useDataViewPagination({ perPage: 10 });
  const { page, perPage } = pagination;

  const fetchClusters = useCallback(
    async (silent = false): Promise<boolean> => {
      if (!silent) setLoading(true);
      try {
        const results = await clusterApi.searchAll({
          filter: fleetClusterTypeFilter(),
        });
        setRows(mergeFleetClusters(results));
        setError(null);
        // Any successful read, silent or not, means the backend answers:
        // polling may resume after a run of transient failures.
        setSilentFailCount(0);
        setLoaded(true);
        return true;
      } catch (e) {
        if (silent) {
          setSilentFailCount((c) => c + 1);
        } else {
          setError(e instanceof Error ? e.message : "Failed to load clusters");
        }
        return false;
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [],
  );

  const closeCreateWizard = useCallback(() => {
    setSearchParams((prev) => {
      prev.delete("create");
      return prev;
    });
  }, [setSearchParams]);

  const handleClusterCreated = useCallback(() => {
    closeCreateWizard();
    setNotice("Cluster created");
    void refetchAfterAction(
      () => fetchClusters(true),
      setActionError,
      "The cluster was created, but the list could not be read again; refresh to see it.",
    );
  }, [closeCreateWizard, fetchClusters]);

  useEffect(() => {
    fetchClusters();
  }, [fetchClusters]);

  const hasTransient = rows.some((r) => clusterRowStatus(r).transient);
  useEffect(() => {
    if (!shouldPoll(hasTransient, silentFailCount)) return;
    const id = setInterval(() => fetchClusters(true), 5000);
    return () => clearInterval(id);
  }, [hasTransient, silentFailCount, fetchClusters]);

  const filtered = useMemo(
    () => rows.filter((r) => matchesClusterFilters(r, filters)),
    [rows, filters],
  );

  const deleteTargetRef = useRef(deleteTarget);
  deleteTargetRef.current = deleteTarget;
  const forgetTargetRef = useRef(forgetTarget);
  forgetTargetRef.current = forgetTarget;

  const handleDelete = async () => {
    const target = deleteTargetRef.current;
    if (!target) return;
    setDeleting(target.id);
    setDeleteTarget(null);
    setActionError(null);
    try {
      const client = createApiClient(
        buildAddonBasePath(extractService(target.result.name)),
      );
      await client.delete(`/clusters/${target.id}`);
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Delete failed");
      return;
    } finally {
      setDeleting(null);
    }
    setNotice("Cluster deleted");
    await refetchAfterAction(
      () => fetchClusters(true),
      setActionError,
      "The cluster was deleted, but the list could not be read again; refresh to see it.",
    );
  };

  const handleRenew = useCallback(
    async (row: ClusterRow) => {
      setRenewing(row.id);
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
      } catch (e) {
        setActionError(
          e instanceof Error ? e.message : "The credential was not renewed",
        );
        return;
      } finally {
        setRenewing(null);
      }
      await refetchAfterAction(
        () => fetchClusters(true),
        setActionError,
        "The credential was renewed, but the list could not be read again; refresh to see it.",
      );
    },
    [fetchClusters],
  );

  const handleForget = useCallback(async () => {
    const target = forgetTargetRef.current;
    if (!target) return;
    setForgetting(target.id);
    setForgetTarget(null);
    setActionError(null);
    try {
      await forgetCluster(extractService(target.result.name), target.id);
    } catch (e) {
      setActionError(
        e instanceof Error ? e.message : "The record was not forgotten",
      );
      return;
    } finally {
      setForgetting(null);
    }
    setNotice("Record forgotten");
    await refetchAfterAction(
      () => fetchClusters(true),
      setActionError,
      "The record was forgotten, but the list could not be read again; refresh to see its state.",
    );
  }, [fetchClusters]);

  const pageRows: DataViewTr[] = useMemo(
    () =>
      filtered
        .slice((page - 1) * perPage, (page - 1) * perPage + perPage)
        .map((r) => {
          const status = clusterRowStatus(r);
          const isDeleting = deleting === r.id;
          const isRenewing = renewing === r.id;
          const isForgetting = forgetting === r.id;
          const canDelete =
            r.origin === "created" &&
            deriveClusterState(r.result.resource) !== "DELETING";
          const actions = enrollmentActions(r);
          const items: IAction[] = [];
          if (actions.renew) {
            items.push({
              title: isRenewing ? "Renewing..." : "Renew credential",
              onClick: () => handleRenew(r),
              isDisabled: isRenewing,
            });
          }
          if (actions.withdraw) {
            items.push({
              title: "Withdraw...",
              onClick: () => setWithdrawTarget(r),
            });
          }
          if (canDelete) {
            items.push({
              title: isDeleting ? "Deleting..." : "Delete",
              onClick: () => setDeleteTarget(r),
              isDisabled: isDeleting,
            });
          }
          if (actions.forget) {
            items.push({
              title: "Forget record...",
              onClick: () => setForgetTarget(r),
              isDisabled: isForgetting,
            });
          }
          return [
            {
              cell: (
                <PluginLink
                  scope="core-plugin"
                  module="ClustersModule"
                  to={r.id}
                >
                  <strong>{r.id}</strong>
                </PluginLink>
              ),
            },
            {
              cell: (
                <Label
                  color={r.origin === "observed" ? "grey" : "blue"}
                  isCompact
                >
                  {originLabel(r)}
                </Label>
              ),
            },
            hubColumn(r),
            {
              cell: (
                <>
                  <Label
                    color={status.color}
                    isCompact
                    icon={status.paused ? <PauseCircleIcon /> : undefined}
                  >
                    {status.text}
                    {r.result.resource.reconciling ? " (reconciling)" : ""}
                  </Label>
                  {status.onHub && (
                    <Label
                      color={status.onHub.color}
                      isCompact
                      className="pf-v6-u-ml-sm"
                    >
                      On its hub: {status.onHub.text}
                    </Label>
                  )}
                </>
              ),
            },
            formatTime(clusterCreatedTime(r)),
            {
              cell: items.length > 0 ? <ActionsColumn items={items} /> : null,
              props: { isActionCell: true },
            },
          ];
        }),
    [filtered, page, perPage, deleting, renewing, forgetting, handleRenew],
  );
  const activeState = loading
    ? DataViewState.loading
    : error
      ? "error"
      : filtered.length === 0
        ? "empty"
        : undefined;

  const emptyBody = (
    <Tbody>
      <Tr>
        <Td colSpan={columns.length}>
          <EmptyState
            headingLevel="h2"
            icon={CubesIcon}
            titleText="No clusters found"
          >
            <EmptyStateBody>
              {rows.length === 0
                ? "Get started by creating your first cluster."
                : "No clusters match the current filter criteria."}
            </EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                {rows.length === 0 ? (
                  <>
                    <Button variant="primary" onClick={openCreateWizard}>
                      Create cluster
                    </Button>
                    <Button
                      variant="secondary"
                      onClick={() => setEnrollOpen(true)}
                    >
                      Enroll with a manifest
                    </Button>
                  </>
                ) : (
                  <Button variant="link" onClick={clearAllFilters}>
                    Clear filters
                  </Button>
                )}
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </Td>
      </Tr>
    </Tbody>
  );

  const errorBody = (
    <Tbody>
      <Tr>
        <Td colSpan={columns.length}>
          <EmptyState headingLevel="h2" titleText="Unable to load clusters">
            <EmptyStateBody>{error}</EmptyStateBody>
            <EmptyStateFooter>
              <EmptyStateActions>
                <Button variant="primary" onClick={() => fetchClusters()}>
                  Try again
                </Button>
              </EmptyStateActions>
            </EmptyStateFooter>
          </EmptyState>
        </Td>
      </Tr>
    </Tbody>
  );

  return (
    <Stack hasGutter>
      <StackItem>
        <div>
          <Title headingLevel="h1">Clusters</Title>
          <Content component="p">
            Manage and monitor your Kubernetes clusters
          </Content>
        </div>
      </StackItem>

      {loaded && (
        <StackItem>
          <ClusterSummaryCards rows={rows} />
        </StackItem>
      )}

      {(actionError || notice) && (
        <AlertGroup isToast isLiveRegion aria-label="Cluster actions">
          {actionError && (
            <Alert
              variant="danger"
              title="The action did not complete"
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
              title={notice}
              actionClose={
                <AlertActionCloseButton
                  aria-label="Close notice"
                  onClose={() => setNotice(null)}
                />
              }
            />
          )}
        </AlertGroup>
      )}

      {pollingPaused(hasTransient, silentFailCount) && (
        <StackItem>
          <Alert
            variant="warning"
            title="Live updates are paused"
            isInline
            actionLinks={
              <AlertActionLink onClick={() => fetchClusters()}>
                Refresh now
              </AlertActionLink>
            }
          >
            The cluster list could not be read again three times in a row, so it
            is no longer updated automatically; what is shown may be out of
            date.
          </Alert>
        </StackItem>
      )}

      <StackItem>
        <DataView activeState={activeState}>
          <DataViewToolbar
            clearAllFilters={clearAllFilters}
            className="ocm-fleet-core-clusters-toolbar"
            actions={
              <>
                <Button
                  variant="secondary"
                  className="pf-v6-u-mr-sm"
                  onClick={() => setEnrollOpen(true)}
                >
                  Enroll with a manifest
                </Button>
                <Button variant="primary" onClick={openCreateWizard}>
                  Create cluster
                </Button>
              </>
            }
            pagination={
              <Pagination
                perPageOptions={PER_PAGE_OPTIONS}
                itemCount={filtered.length}
                {...pagination}
              />
            }
            filters={
              <DataViewFilters
                onChange={(_e, values) => onSetFilters(values)}
                values={filters}
              >
                <DataViewTextFilter
                  filterId="name"
                  title="Name"
                  placeholder="Filter by name"
                />
                <DataViewTextFilter
                  filterId="hub"
                  title="Hub"
                  placeholder="Filter by hub"
                />
              </DataViewFilters>
            }
          />
          <DataViewTable
            aria-label="Clusters table"
            columns={columns}
            rows={pageRows}
            headStates={{
              loading: <SkeletonTableHead columns={columns} />,
            }}
            bodyStates={{
              loading: (
                <SkeletonTableBody
                  rowsCount={5}
                  columnsCount={columns.length}
                />
              ),
              empty: emptyBody,
              error: errorBody,
            }}
          />
          <DataViewToolbar
            pagination={
              <Pagination
                isCompact
                perPageOptions={PER_PAGE_OPTIONS}
                itemCount={filtered.length}
                {...pagination}
              />
            }
          />
        </DataView>
      </StackItem>

      <DeleteClusterModal
        clusterId={deleteTarget?.id ?? ""}
        isOpen={deleteTarget !== null}
        isDeleting={deleteTarget !== null && deleting === deleteTarget.id}
        onClose={() => setDeleteTarget(null)}
        onDelete={handleDelete}
      />

      {withdrawTarget && (
        <WithdrawClusterModal
          key={withdrawTarget.id}
          isOpen
          clusterId={withdrawTarget.id}
          service={extractService(withdrawTarget.result.name)}
          onClose={() => setWithdrawTarget(null)}
          onWithdrawn={() => {
            setWithdrawTarget(null);
            setNotice("Cluster withdrawn");
            void refetchAfterAction(
              () => fetchClusters(true),
              setActionError,
              "The cluster was withdrawn, but the list could not be read again; refresh to see it.",
            );
          }}
        />
      )}

      <Modal
        isOpen={forgetTarget !== null}
        onClose={() => setForgetTarget(null)}
        variant="small"
      >
        <ModalHeader
          title="Forget cluster record"
          description={`Forget the withdrawn record for "${forgetTarget?.id}"? The record is removed, so the cluster id can be enrolled again. Nothing on the cluster itself is touched.`}
        />
        <ModalBody />
        <ModalFooter>
          <Button
            variant="danger"
            onClick={handleForget}
            isLoading={forgetting !== null}
            isDisabled={forgetting !== null}
          >
            Forget
          </Button>
          <Button variant="link" onClick={() => setForgetTarget(null)}>
            Cancel
          </Button>
        </ModalFooter>
      </Modal>

      <CreateClusterModal
        isOpen={wizardOpen}
        preselectedProvider={preselectedProvider}
        onClose={closeCreateWizard}
        onCreated={handleClusterCreated}
        onProviderSelect={selectProvider}
      />

      <EnrollManifestModal
        isOpen={enrollOpen}
        onClose={() => {
          setEnrollOpen(false);
          fetchClusters(true);
        }}
      />
    </Stack>
  );
}
