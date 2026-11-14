import type {
  OnboardingActionCardProps,
  OnboardingActionFormProps,
} from "@ocm-fleet/common";
import { PluginLink } from "@ocm-fleet/common";
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  Icon,
  Split,
  SplitItem,
  Title,
} from "@patternfly/react-core";
import { CheckCircleIcon, NetworkIcon } from "@patternfly/react-icons";
import { useEffect, useState } from "react";

import { listHubs } from "./api";
import HubConnectForm from "./HubConnectForm";

export function HubOnboardingCard({
  completed,
  onConfigure,
}: OnboardingActionCardProps) {
  // The card reports what is connected, not only what this browser did:
  // a hub may have been connected from elsewhere.
  const [hubs, setHubs] = useState<string[] | null>(null);

  useEffect(() => {
    let cancelled = false;
    listHubs()
      .then((results) => {
        if (!cancelled) {
          setHubs(results.map((r) => r.resource.name.replace(/^hubs\//, "")));
        }
      })
      .catch(() => {
        if (!cancelled) setHubs(null);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const connected = hubs !== null ? hubs.length > 0 : completed;

  return (
    <Card isCompact>
      <CardHeader>
        <CardTitle>
          <Split hasGutter>
            <SplitItem>
              <Icon size="xl">
                <NetworkIcon />
              </Icon>
            </SplitItem>
            <SplitItem isFilled>Connect an OCM hub</SplitItem>
            {connected && (
              <SplitItem>
                <Icon status="success">
                  <CheckCircleIcon />
                </Icon>
              </SplitItem>
            )}
          </Split>
        </CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="pf-v6-u-mb-md">
          See every cluster of an existing Open Cluster Management hub, and
          enroll the ones you choose. Connecting is read-only.
        </Content>
        {hubs && hubs.length > 0 && (
          <Content component="small" className="pf-v6-u-mb-md">
            Connected: {hubs.join(", ")}.{" "}
            <PluginLink scope="ocm-plugin" module="HubsPage">
              View hubs
            </PluginLink>
          </Content>
        )}
        <Button variant="secondary" onClick={onConfigure}>
          {connected ? "Connect another hub" : "Connect a hub"}
        </Button>
      </CardBody>
    </Card>
  );
}

export function HubOnboardingForm({
  onComplete,
  onCancel,
}: OnboardingActionFormProps) {
  return (
    <div className="ocm-fleet-setup-whats-next__body">
      <div className="ocm-fleet-setup-whats-next__inner">
        <Title headingLevel="h1" className="pf-v6-u-mb-md">
          Connect an OCM hub
        </Title>
        <HubConnectForm onConnected={onComplete} onCancel={onCancel} />
      </div>
    </div>
  );
}
