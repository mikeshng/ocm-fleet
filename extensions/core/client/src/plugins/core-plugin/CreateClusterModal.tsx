import type {
  ClusterProviderCardProps,
  ClusterProviderWizardProps,
} from "@ocm-fleet/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import {
  useExtensions,
  useResolvedExtensions,
} from "@openshift/dynamic-plugin-sdk";
import {
  Bullseye,
  Button,
  Content,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  Gallery,
  GalleryItem,
  Modal,
  ModalBody,
  ModalHeader,
  Spinner,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import type { ComponentType } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";

type ClusterProviderExtension = Extension<
  "ocm-fleet.cluster-provider",
  {
    id: string;
    label: string;
    description: string;
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<ClusterProviderCardProps>>;
    wizard: CodeRef<ComponentType<ClusterProviderWizardProps>>;
  }
>;

function isClusterProvider(e: Extension): e is ClusterProviderExtension {
  return e.type === "ocm-fleet.cluster-provider";
}

interface CreateClusterModalProps {
  isOpen: boolean;
  preselectedProvider: string | null;
  onClose: () => void;
  /** Called after the provider reports that the cluster exists. */
  onCreated: () => void;
  onProviderSelect: (providerId: string) => void;
}

export default function CreateClusterModal({
  isOpen,
  preselectedProvider,
  onClose,
  onCreated,
  onProviderSelect,
}: CreateClusterModalProps) {
  const providerCandidates = useExtensions(isClusterProvider);
  const [providerExtensions, providersLoaded] =
    useResolvedExtensions(providerCandidates);
  const sortedProviders = useMemo(() => {
    const cpy = [...providerExtensions];
    cpy.sort((a, b) => a.properties.label.localeCompare(b.properties.label));
    return cpy;
  }, [providerExtensions]);

  const selectedProviderId = isOpen ? preselectedProvider : null;
  const [wizardBusy, setWizardBusy] = useState(false);

  // A different wizard (or none) starts with no create in flight.
  useEffect(() => {
    setWizardBusy(false);
  }, [selectedProviderId]);

  const selectProvider = useCallback(
    (id: string) => {
      onProviderSelect(id);
    },
    [onProviderSelect],
  );

  const backToProviders = useCallback(() => {
    onProviderSelect("");
  }, [onProviderSelect]);

  return (
    <Modal
      isOpen={isOpen}
      onClose={wizardBusy ? undefined : onClose}
      variant="large"
      aria-label="Create cluster"
    >
      {selectedProviderId ? (
        (() => {
          const ext = sortedProviders.find(
            (p) => p.properties.id === selectedProviderId,
          );
          if (!ext) {
            return (
              <ModalBody>
                <EmptyState headingLevel="h2" titleText="Provider not found">
                  <EmptyStateBody>
                    The cluster provider &ldquo;{selectedProviderId}&rdquo; is
                    not available.
                  </EmptyStateBody>
                  <EmptyStateFooter>
                    <EmptyStateActions>
                      <Button variant="primary" onClick={backToProviders}>
                        Back to providers
                      </Button>
                    </EmptyStateActions>
                  </EmptyStateFooter>
                </EmptyState>
              </ModalBody>
            );
          }
          const WizardComponent = ext.properties
            .wizard as unknown as ComponentType<ClusterProviderWizardProps>;
          return (
            <WizardComponent
              onClose={onClose}
              onCreated={onCreated}
              onBusyChange={setWizardBusy}
            />
          );
        })()
      ) : (
        <>
          <ModalHeader title="Create cluster" />
          <ModalBody>
            {!providersLoaded ? (
              <Bullseye>
                <Spinner />
              </Bullseye>
            ) : sortedProviders.length === 0 ? (
              <EmptyState headingLevel="h3" titleText="No providers available">
                <EmptyStateBody>
                  No cluster providers are installed. Install a provider plugin
                  to create clusters.
                </EmptyStateBody>
              </EmptyState>
            ) : (
              <Stack hasGutter>
                <StackItem>
                  <Content component="p">
                    Select a cluster provider to get started.
                  </Content>
                </StackItem>
                <StackItem>
                  <Gallery hasGutter minWidths={{ default: "300px" }}>
                    {sortedProviders.map((ext) => {
                      const CardComponent = ext.properties
                        .card as unknown as ComponentType<ClusterProviderCardProps>;
                      return (
                        <GalleryItem key={ext.properties.id}>
                          <CardComponent
                            onSelect={() => selectProvider(ext.properties.id)}
                          />
                        </GalleryItem>
                      );
                    })}
                  </Gallery>
                </StackItem>
              </Stack>
            )}
          </ModalBody>
        </>
      )}
    </Modal>
  );
}
