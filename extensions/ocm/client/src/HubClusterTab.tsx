import type { ClusterDetailTabProps } from "@ocm-fleet/common";
import {
  Alert,
  Card,
  CardBody,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Label,
  LabelGroup,
  Spinner,
  Title,
} from "@patternfly/react-core";
import { useEffect, useState } from "react";

import { findObservedCluster, type ObservedClusterResult } from "./api";

const ENROLLMENT_TEXT: Record<string, string> = {
  observed: "Observed: known through the hub, not enrolled",
  enrolled: "Enrolled",
  created: "Created here",
  withdrawn: "Withdrawn",
};

function pairs(map: Record<string, string> | undefined): [string, string][] {
  return Object.entries(map ?? {}).sort(([a], [b]) => a.localeCompare(b));
}

/** What a connected hub reports about the cluster. */
export default function HubClusterTab({ clusterId }: ClusterDetailTabProps) {
  const [record, setRecord] = useState<ObservedClusterResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!clusterId) return;
    let cancelled = false;
    setLoading(true);
    findObservedCluster(clusterId)
      .then((found) => {
        if (cancelled) return;
        setRecord(found ?? null);
        setError(null);
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setError(
          e instanceof Error ? e.message : "The hub's record did not load",
        );
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [clusterId]);

  if (loading) return <Spinner aria-label="Loading the hub's record" />;
  if (error) return <Alert variant="warning" isInline title={error} />;
  const observation = record?.resource.observation;
  if (!record || !observation) {
    return (
      <Alert
        variant="info"
        isInline
        title="No connected hub reports this cluster"
      />
    );
  }

  const labels = pairs(record.resource.localLabels);
  const claims = pairs(observation.claims);
  const enrollment = observation.enrollment;
  const facts: [string, string][] = [
    ["Hub", observation.hub],
    ["Name on the hub", observation.name],
    ["Accepted by the hub", observation.accepted ? "Yes" : "No"],
    ["Available", observation.available],
    ["Cluster set", observation.clusterSet ?? ""],
    ["Kubernetes version", observation.kubernetesVersion ?? ""],
    [
      "API address the hub records",
      (observation.clientConfigs ?? []).map((c) => c.url).join(", "),
    ],
    ["Taints", (observation.taints ?? []).join(", ")],
    [
      "Here",
      ENROLLMENT_TEXT[enrollment?.state ?? "observed"] ??
        enrollment?.state ??
        "",
    ],
    ["Cluster record", enrollment?.cluster ?? ""],
    ["Deployment target", enrollment?.target ?? ""],
    ["Linked by", enrollment?.basis ?? ""],
  ];

  return (
    <Card>
      <CardBody>
        {observation.conflict && (
          <Alert
            variant="warning"
            isInline
            title="The hub's record and the cluster's record disagree"
            className="pf-v6-u-mb-md"
          >
            {observation.conflict}
          </Alert>
        )}
        <Title headingLevel="h2" size="lg" className="pf-v6-u-mb-md">
          As hub {observation.hub} reports it
        </Title>
        <DescriptionList columnModifier={{ default: "2Col" }}>
          {facts
            .filter(([, value]) => value)
            .map(([term, value]) => (
              <DescriptionListGroup key={term}>
                <DescriptionListTerm>{term}</DescriptionListTerm>
                <DescriptionListDescription>{value}</DescriptionListDescription>
              </DescriptionListGroup>
            ))}
          {labels.length > 0 && (
            <DescriptionListGroup>
              <DescriptionListTerm>Labels on the hub</DescriptionListTerm>
              <DescriptionListDescription>
                <LabelGroup numLabels={12}>
                  {labels.map(([key, value]) => (
                    <Label key={key} color="grey" isCompact>
                      {key}={value}
                    </Label>
                  ))}
                </LabelGroup>
              </DescriptionListDescription>
            </DescriptionListGroup>
          )}
          {claims.length > 0 && (
            <DescriptionListGroup>
              <DescriptionListTerm>Cluster claims</DescriptionListTerm>
              <DescriptionListDescription>
                <LabelGroup numLabels={12}>
                  {claims.map(([key, value]) => (
                    <Label key={key} color="grey" isCompact>
                      {key}={value}
                    </Label>
                  ))}
                </LabelGroup>
              </DescriptionListDescription>
            </DescriptionListGroup>
          )}
        </DescriptionList>
      </CardBody>
    </Card>
  );
}
