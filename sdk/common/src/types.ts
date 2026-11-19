export interface User {
  id: string;
  username: string;
  display_name: string;
}

export interface PluginEntry {
  name: string;
  key: string;
  label: string;
  persona: "ops" | "dev";
}

export interface PluginRegistry {
  assetsHost: string;
  plugins: Record<string, PluginEntry>;
}

export interface ClusterProviderCardProps {
  onSelect: () => void;
}

export interface ClusterProviderWizardProps {
  onClose?: () => void;
  /** Called once a cluster exists, before the wizard closes itself. */
  onCreated?: () => void;
  /** Reports a create in flight so the host can keep the wizard open. */
  onBusyChange?: (busy: boolean) => void;
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

export interface OnboardingActionCardProps {
  completed: boolean;
  onConfigure: () => void;
}

export interface OnboardingActionFormProps {
  onComplete: () => void;
  onCancel: () => void;
}

export interface ClusterDetailTabProps {
  clusterId: string | undefined;
  /** Id of the hub the page shows for the cluster, when a hub reports it. */
  hub?: string;
}

export interface SearchEntry {
  id: string;
  title: string;
  description: string;
  category: string;
  pathname: string;
  icon: string;
  status: string;
  meta: string;
  feature?: string;
}
