import {
  type Extension,
  PluginStoreProvider,
  TestPluginStore,
} from "@openshift/dynamic-plugin-sdk";
import { ScalprumContext } from "@scalprum/react-core";
import { createElement, useMemo } from "react";
import { MemoryRouter } from "react-router-dom";

import SetupChecklist from "../SetupChecklist";

const scalprum = {
  initialized: true,
  config: {},
  api: { fleet: { getPluginPagePath: () => "/plugins/settings" } },
  pluginStore: {},
} as never;

function checklistStore(): TestPluginStore {
  const extensions: Extension[] = [
    {
      type: "ocm-fleet.onboarding-action",
      properties: {
        id: "registry",
        label: "Connect a registry",
        completeWhenResourceExists: "kind.open-cluster-management.io/Registry",
      },
    },
    {
      type: "ocm-fleet.onboarding-action",
      properties: {
        id: "provider",
        label: "Create a provider",
        completeWhenResourceExists: "kind.open-cluster-management.io/Provider",
      },
    },
  ];
  const store = new TestPluginStore();
  store.addLoadedPlugin(
    {
      name: "overview-test",
      version: "0.0.0",
      extensions,
      registrationMethod: "local",
    },
    extensions.map((extension, i) => ({
      ...extension,
      pluginName: "overview-test",
      uid: `extension-${i}`,
    })),
  );
  store.enablePlugins(["overview-test"]);
  return store;
}

/**
 * The component under test reads its actions from the plugin store, so the
 * harness builds a store and the surrounding providers in the browser, where
 * the component runs.
 */
export default function SetupChecklistHarness() {
  const store = useMemo(checklistStore, []);
  return createElement(
    PluginStoreProvider,
    { store },
    <ScalprumContext.Provider value={scalprum}>
      <MemoryRouter>
        <SetupChecklist />
      </MemoryRouter>
    </ScalprumContext.Provider>,
  );
}
