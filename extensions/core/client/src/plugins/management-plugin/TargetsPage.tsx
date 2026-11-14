import { extractClusterId, PluginLink } from "@ocm-fleet/common";
import {
  Alert,
  Bullseye,
  Button,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  EmptyState,
  EmptyStateBody,
  Flex,
  FlexItem,
  Label,
  LabelGroup,
  SearchInput,
  Spinner,
  Title,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
  Tooltip,
} from "@patternfly/react-core";
import { SyncAltIcon } from "@patternfly/react-icons";
import {
  ExpandableRowContent,
  Table,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from "@patternfly/react-table";
import { useCallback, useEffect, useMemo, useState } from "react";

import { listDeployments, listTargets, type MgmtTarget } from "./api";

interface TargetInfo extends MgmtTarget {
  deployments: string[];
}

function stateColor(state: string): "green" | "orange" | "grey" {
  if (state === "ready") return "green";
  return state ? "orange" : "grey";
}

export default function TargetsPage() {
  const [targets, setTargets] = useState<TargetInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [nameFilter, setNameFilter] = useState("");

  const fetchTargets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [targetsResp, deploymentsResp] = await Promise.all([
        listTargets(),
        listDeployments(),
      ]);

      const byTarget = new Map<string, string[]>();
      for (const dep of deploymentsResp.deployments ?? []) {
        for (const tid of dep.resolvedTargetIds ?? []) {
          const existing = byTarget.get(tid) ?? [];
          existing.push(dep.name);
          byTarget.set(tid, existing);
        }
      }

      setTargets(
        (targetsResp.targets ?? []).map((t) => ({
          ...t,
          deployments: byTarget.get(t.id) ?? [],
        })),
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch targets");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTargets();
  }, [fetchTargets]);

  const filtered = useMemo(
    () =>
      targets.filter((t) =>
        nameFilter
          ? t.id.toLowerCase().includes(nameFilter.toLowerCase())
          : true,
      ),
    [targets, nameFilter],
  );

  const toggleExpand = useCallback((id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  }, []);

  if (loading) {
    return (
      <Bullseye>
        <Spinner />
      </Bullseye>
    );
  }

  return (
    <div>
      <Flex
        alignItems={{ default: "alignItemsBaseline" }}
        gap={{ default: "gapSm" }}
        className="pf-v6-u-mb-lg"
      >
        <FlexItem>
          <Title headingLevel="h1">Targets</Title>
        </FlexItem>
        <FlexItem>
          <span className="pf-v6-u-font-size-sm pf-v6-u-text-color-subtle">
            {targets.length} registered
          </span>
        </FlexItem>
      </Flex>

      {error && (
        <Alert
          variant="warning"
          title={error}
          isInline
          className="pf-v6-u-mb-md"
        />
      )}

      {targets.length === 0 ? (
        <EmptyState titleText="No targets found" headingLevel="h2">
          <EmptyStateBody>
            Targets are registered when the management plane starts, when a
            cluster is created, and when a cluster is enrolled.
          </EmptyStateBody>
          <Button variant="primary" onClick={fetchTargets}>
            Refresh
          </Button>
        </EmptyState>
      ) : (
        <>
          <Toolbar clearAllFilters={() => setNameFilter("")}>
            <ToolbarContent>
              <ToolbarItem>
                <SearchInput
                  placeholder="Filter by target ID"
                  value={nameFilter}
                  onChange={(_event, value) => setNameFilter(value)}
                  onClear={() => setNameFilter("")}
                />
              </ToolbarItem>
              <ToolbarItem>
                <Tooltip content="Refresh targets">
                  <Button
                    variant="plain"
                    aria-label="Refresh targets"
                    onClick={fetchTargets}
                  >
                    <SyncAltIcon />
                  </Button>
                </Tooltip>
              </ToolbarItem>
            </ToolbarContent>
          </Toolbar>

          {filtered.length === 0 ? (
            <EmptyState titleText="No matching targets" headingLevel="h2">
              <EmptyStateBody>
                No targets match the current filter. Try adjusting or clearing
                the filter.
              </EmptyStateBody>
              <Button variant="link" onClick={() => setNameFilter("")}>
                Clear filter
              </Button>
            </EmptyState>
          ) : (
            <Table
              aria-label="Registered targets"
              variant="compact"
              hasAnimations
            >
              <Thead>
                <Tr>
                  <Th screenReaderText="Row expansion" />
                  <Th>Target ID</Th>
                  <Th>Type</Th>
                  <Th>State</Th>
                  <Th>Cluster</Th>
                  <Th>Deployments</Th>
                </Tr>
              </Thead>
              {filtered.map((t, rowIndex) => (
                <Tbody key={t.id} isExpanded={expandedId === t.id}>
                  <Tr>
                    <Td
                      expand={{
                        rowIndex,
                        isExpanded: expandedId === t.id,
                        onToggle: () => toggleExpand(t.id),
                      }}
                    />
                    <Td dataLabel="Target ID">
                      <span className="pf-v6-u-font-weight-bold">{t.id}</span>
                    </Td>
                    <Td dataLabel="Type">
                      <Label color="blue" isCompact>
                        {t.type}
                      </Label>
                    </Td>
                    <Td dataLabel="State">
                      <Label color={stateColor(t.state)} isCompact>
                        {t.state || "unknown"}
                      </Label>
                    </Td>
                    <Td dataLabel="Cluster">
                      {t.cluster ? (
                        <PluginLink
                          scope="core-plugin"
                          module="ClustersModule"
                          to={extractClusterId(t.cluster)}
                          fallback={extractClusterId(t.cluster)}
                        >
                          {extractClusterId(t.cluster)}
                        </PluginLink>
                      ) : (
                        <span className="pf-v6-u-text-color-subtle">—</span>
                      )}
                    </Td>
                    <Td dataLabel="Deployments">
                      {t.deployments.length > 0 ? (
                        <LabelGroup>
                          {t.deployments.map((d) => (
                            <Label key={d} color="green" isCompact>
                              {d.replace("deployments/", "")}
                            </Label>
                          ))}
                        </LabelGroup>
                      ) : (
                        <span className="pf-v6-u-text-color-subtle">None</span>
                      )}
                    </Td>
                  </Tr>
                  <Tr isExpanded={expandedId === t.id}>
                    <Td />
                    <Td colSpan={5}>
                      <ExpandableRowContent>
                        <DescriptionList isHorizontal isCompact>
                          <DescriptionListGroup>
                            <DescriptionListTerm>Target ID</DescriptionListTerm>
                            <DescriptionListDescription>
                              <span className="pf-v6-u-font-family-monospace">
                                {t.id}
                              </span>
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                          <DescriptionListGroup>
                            <DescriptionListTerm>Origin</DescriptionListTerm>
                            <DescriptionListDescription>
                              {t.origin}
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                          {t.accessForm && (
                            <DescriptionListGroup>
                              <DescriptionListTerm>
                                Access form
                              </DescriptionListTerm>
                              <DescriptionListDescription>
                                {t.accessForm}
                              </DescriptionListDescription>
                            </DescriptionListGroup>
                          )}
                          {t.deliveryCredential && (
                            <DescriptionListGroup>
                              <DescriptionListTerm>
                                Stored credential delivers
                              </DescriptionListTerm>
                              <DescriptionListDescription>
                                {t.deliveryCredential}
                              </DescriptionListDescription>
                            </DescriptionListGroup>
                          )}
                          <DescriptionListGroup>
                            <DescriptionListTerm>Labels</DescriptionListTerm>
                            <DescriptionListDescription>
                              {Object.keys(t.labels ?? {}).length > 0 ? (
                                <LabelGroup numLabels={8}>
                                  {Object.entries(t.labels).map(([k, v]) => (
                                    <Label key={k} color="grey" isCompact>
                                      {k}={v}
                                    </Label>
                                  ))}
                                </LabelGroup>
                              ) : (
                                <span className="pf-v6-u-text-color-subtle">
                                  None
                                </span>
                              )}
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                          <DescriptionListGroup>
                            <DescriptionListTerm>
                              Accepted Resource Types
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                              <Label color="grey" isCompact>
                                {(t.acceptedManifestTypes ?? []).length > 0
                                  ? t.acceptedManifestTypes.join(", ")
                                  : "any"}
                              </Label>
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                          <DescriptionListGroup>
                            <DescriptionListTerm>
                              Active Deployments
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                              {t.deployments.length}
                            </DescriptionListDescription>
                          </DescriptionListGroup>
                        </DescriptionList>
                      </ExpandableRowContent>
                    </Td>
                  </Tr>
                </Tbody>
              ))}
            </Table>
          )}
        </>
      )}
    </div>
  );
}
