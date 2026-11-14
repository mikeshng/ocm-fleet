import { ModuleFederationPlugin as BaseMFPlugin } from "@module-federation/enhanced/rspack";
import {
  createClusterDetailTab,
  createCommonModuleReplacementPlugin,
  createCommonTransformImport,
  createOnboardingAction,
  createPfModuleReplacementPlugin,
  createPfTransformImport,
  createRoute,
  createSearchResultRenderer,
  FleetPlugin,
  getCommonDynamicModules,
  getDynamicModules,
} from "@ocm-fleet/build-utils";
import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";
import { fileURLToPath } from "url";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const uiRoot = path.resolve(configDir, "..");
const pfSharedModules = getDynamicModules(configDir, uiRoot);
const commonSharedModules = getCommonDynamicModules();
const pfTransformImport = createPfTransformImport();
const commonTransformImport = createCommonTransformImport();

const swcLoaderRule = {
  test: /\.tsx?$/,
  exclude: [/node_modules/, /packages\/common\/dist/],
  loader: "builtin:swc-loader" as const,
  options: {
    jsc: {
      parser: { syntax: "typescript" as const, tsx: true },
      // TODO: enable reactCompiler: true when rspack >= 2.1.0
      transform: { react: { runtime: "automatic" as const } },
    },
    transformImport: [...pfTransformImport, commonTransformImport],
  },
  type: "javascript/auto" as const,
};

const sharedModules = {
  react: { singleton: true, requiredVersion: "*" },
  "react-dom": { singleton: true, requiredVersion: "*" },
  "react/jsx-dev-runtime": { singleton: true, requiredVersion: "^19" },
  "@scalprum/core": { singleton: true, requiredVersion: "*" },
  "@scalprum/react-core": { singleton: true, requiredVersion: "*" },
  "@openshift/dynamic-plugin-sdk": {
    singleton: true,
    requiredVersion: "*",
    version: "*",
  },
  "react-router-dom": { singleton: true, requiredVersion: "*" },
  "react/jsx-runtime": { singleton: true, requiredVersion: "^19" },
  ...pfSharedModules,
  ...commonSharedModules,
};

class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false, manifest: false });
  }
}

const mfOverride = {
  libraryType: "global",
  pluginOverride: {
    ModuleFederationPlugin,
  },
};

const p = (rel: string) => path.resolve(configDir, rel);

const OcmPlugin = new FleetPlugin({
  extensions: [
    createRoute({
      id: "hubs",
      label: "Hubs",
      component: { $codeRef: "HubsPage.default" },
      icon: { $codeRef: "HubIcon.default" },
      description:
        "Connect Open Cluster Management hubs, see their clusters, and enroll the ones you choose",
      keywords: [
        "hub",
        "ocm",
        "open cluster management",
        "managed cluster",
        "enroll",
        "import",
      ],
    }),
    createOnboardingAction({
      id: "ocm-hub-connect",
      label: "OCM hub",
      description:
        "See every cluster of an existing Open Cluster Management hub and enroll the ones you choose.",
      icon: { $codeRef: "HubIcon.default" },
      card: { $codeRef: "HubOnboarding.HubOnboardingCard" },
      form: { $codeRef: "HubOnboarding.HubOnboardingForm" },
      overviewCta: "Connect a hub",
    }),
    createClusterDetailTab({
      id: "hub-tab",
      label: "Hub",
      title: "Hub",
      eventKey: "hub",
      priority: 40,
      service: "ocm.open-cluster-management.io",
      component: { $codeRef: "HubClusterTab.default" },
    }),
    createSearchResultRenderer({
      id: "observed-cluster-renderer",
      label: "Hub cluster",
      resourceType: "ocm.open-cluster-management.io/ManagedCluster",
      resolve: {
        $codeRef: "ObservedClusterSearchResult.resolveObservedCluster",
      },
      icon: { $codeRef: "ObservedClusterSearchResult.ObservedClusterIcon" },
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/ocm/ocm-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/ocm/ocm-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "ocm-plugin",
    version: "1.0.0",
    exposedModules: {
      HubsPage: p("./src/HubsPage.tsx"),
      HubIcon: p("./src/HubIcon.tsx"),
      HubOnboarding: p("./src/HubOnboarding.tsx"),
      HubClusterTab: p("./src/HubClusterTab.tsx"),
      ObservedClusterSearchResult: p("./src/ObservedClusterSearchResult.tsx"),
    },
  },
});

const pluginConfigs = [{ plugin: OcmPlugin, key: "ocm" }] as const;

const configs: Configuration[] = pluginConfigs.map(({ plugin, key }) => ({
  name: key,
  cache: {
    type: "persistent" as const,
    version: key,
    buildDependencies: [fileURLToPath(import.meta.url)],
  },
  entry: {
    mock: path.resolve(configDir, "./src/index.ts"),
  },
  output: {
    publicPath: "auto",
    chunkFilename: `plugins/${key}/[name].js`,
    assetModuleFilename: `plugins/${key}/assets/[hash][ext]`,
    uniqueName: key,
  },
  mode: (process.env.NODE_ENV === "development"
    ? "development"
    : "production") as Configuration["mode"],
  ignoreWarnings: [/Plugin base URL/, /Plugin has no extensions/],
  stats: {
    preset: "normal",
    colors: true,
    timings: true,
    modules: false,
  },
  plugins: [
    plugin,
    new rspack.DefinePlugin({
      "process.env.DRAGGABLE_DEBUG": "false",
    }),
    createPfModuleReplacementPlugin(uiRoot),
    createCommonModuleReplacementPlugin(),
    new rspack.NormalModuleReplacementPlugin(
      /^@patternfly\/react-core\/dist\/esm\/(components|helpers|layouts)(\/|$)/,
      (resource) => {
        const compMatch = resource.request.match(
          /^(@patternfly\/react-core\/)dist\/esm\/((?:components|layouts)\/[^/]+)/,
        );
        if (compMatch) {
          resource.request = `${compMatch[1]}dist/dynamic/${compMatch[2]}`;
          return;
        }
        resource.request = resource.request.replace(
          "/dist/esm/",
          "/dist/dynamic/",
        );
        resource.request = resource.request.replace(/\.(js|mjs)$/, "");
      },
    ),
  ],
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
    fallback: {
      cookie: false,
      "set-cookie-parser": false,
    },
  },
  module: {
    rules: [
      swcLoaderRule,
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
      },
      {
        test: /\.scss$/,
        use: ["style-loader", "css-loader", "sass-loader"],
      },
      {
        test: /\.(png|jpe?g|gif|svg|webp)$/i,
        type: "asset/resource",
      },
    ],
  },
}));

export default configs;
