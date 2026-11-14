import type { ResourceResult, SearchResultResolve } from "@ocm-fleet/common";
import {
  createResourceApi,
  ENROLLED_CLUSTER_TYPE,
  foldLinkedClusterResults,
  KIND_CLUSTER_TYPE,
  KUBERNETES_OBJECT_TYPE,
  OBSERVED_CLUSTER_TYPE,
} from "@ocm-fleet/common";
import {
  useExtensions,
  useResolvedExtensions,
} from "@openshift/dynamic-plugin-sdk";
import { Label } from "@patternfly/react-core";
import {
  type ComponentType,
  type ReactNode,
  useCallback,
  useMemo,
} from "react";

import {
  extractFieldPaths,
  resolveFieldValue,
} from "../components/Search/advanced/extractMatchFields";
import { highlightText } from "../components/Search/highlightUtils";
import type { SearchResultItem } from "../components/Search/searchIndex";
import type { SearchResultRendererExtension } from "../extensions/isSearchResultRendererExtension";
import { isSearchResultRendererExtension } from "../extensions/isSearchResultRendererExtension";

interface ResolvedRenderer {
  label: string;
  resolve: SearchResultResolve;
  icon?: ComponentType;
}

function badgedDescription(badge: string, description: ReactNode): ReactNode {
  return (
    <>
      <Label isCompact color="blue">
        {badge}
      </Label>{" "}
      {description}
    </>
  );
}

const client = createResourceApi("-");

export function useInventorySearch(): {
  search: (term: string) => Promise<SearchResultItem[]>;
  filterSearch: (celFilter: string) => Promise<SearchResultItem[]>;
  loaded: boolean;
} {
  const rendererExtensions = useExtensions<SearchResultRendererExtension>(
    isSearchResultRendererExtension,
  );
  const [extensions, loaded] =
    useResolvedExtensions<SearchResultRendererExtension>(rendererExtensions);

  const rendererMap = useMemo(() => {
    const map = new Map<string, ResolvedRenderer>();
    for (const ext of extensions) {
      map.set(ext.properties.resourceType, {
        label: ext.properties.label,
        resolve: ext.properties.resolve,
        icon: ext.properties.icon,
      });
    }
    return map;
  }, [extensions]);

  const mapResult = useCallback(
    (result: ResourceResult, highlight?: string): SearchResultItem => {
      const renderer = rendererMap.get(result.resourceType);
      const id =
        ((result.resource as Record<string, unknown>).uuid as string) ??
        ((result.resource as Record<string, unknown>).uid as string) ??
        result.resource.name;
      const rawName =
        result.resource.name.split("/").pop() ?? result.resource.name;

      if (!renderer) {
        return {
          id,
          title: highlight ? highlightText(highlight, rawName) : rawName,
          description: result.resourceType,
          category: "resources",
          pathname: "",
          icon: "",
          status: "",
        };
      }

      let rendered;
      try {
        rendered = renderer.resolve(result);
      } catch (err) {
        console.error(err);
        return {
          id,
          title: highlight ? highlightText(highlight, rawName) : rawName,
          description: result.resourceType,
          category: "resources",
          pathname: "",
          icon: "",
          status: "",
        };
      }

      const title = rendered.title ?? rawName;
      return {
        id,
        title: highlight ? highlightText(highlight, title) : title,
        description: "",
        descriptionNode: badgedDescription(
          renderer.label,
          rendered.description,
        ),
        category: "resources",
        pathname: "",
        icon: "",
        status: "",
        IconComponent: renderer.icon,
        pluginLink: {
          scope: rendered.scope,
          module: rendered.module,
          to: rendered.to,
          search: rendered.search,
        },
        navigable: rendered.navigable,
      };
    },
    [rendererMap],
  );

  const search = useCallback(
    async (term: string): Promise<SearchResultItem[]> => {
      if (!loaded) return [];
      try {
        const escaped = term.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
        const clusterFilter = [
          `(resourceType == "${KIND_CLUSTER_TYPE}")`,
          `&& (resource.name.startsWith("${escaped}") || resource.name.startsWith("clusters/${escaped}"))`,
        ].join(" ");
        // Enrolled clusters and the clusters of connected hubs are found
        // by the start of their name, or by a whole term: a label
        // (key=value), a cluster set, or a hub.
        const enrolledFilter = [
          `(resourceType == "${ENROLLED_CLUSTER_TYPE}")`,
          `&& (resource.name.startsWith("clusters/${escaped}") || "${escaped}" in resource.observation.searchTerms)`,
        ].join(" ");
        const observedFilter = [
          `(resourceType == "${OBSERVED_CLUSTER_TYPE}")`,
          `&& (resource.observation.name.startsWith("${escaped}") || "${escaped}" in resource.observation.searchTerms)`,
        ].join(" ");
        const k8sFilter = [
          `resourceType == "${KUBERNETES_OBJECT_TYPE}"`,
          `&& (resource.observation.kind == "Pod" || resource.observation.kind == "Namespace" || resource.observation.kind == "Node")`,
          `&& resource.observation.metadata.name.startsWith("${escaped}")`,
        ].join(" ");
        const [
          clusterResponse,
          enrolledResponse,
          observedResponse,
          k8sResponse,
        ] = await Promise.all([
          client.search({ filter: clusterFilter, pageSize: 5 }),
          client.search({ filter: enrolledFilter, pageSize: 5 }),
          client.search({ filter: observedFilter, pageSize: 5 }),
          client.search({ filter: k8sFilter, pageSize: 5 }),
        ]);
        // A cluster that a hub reports appears once, as the hub's record
        // of it, which names the hub and leads to the cluster's page.
        const allResults = [
          ...foldLinkedClusterResults([
            ...clusterResponse.resources,
            ...enrolledResponse.resources,
            ...observedResponse.resources,
          ]),
          ...k8sResponse.resources,
        ];
        return allResults.map((r) => mapResult(r, term));
      } catch (error) {
        console.error(error);
        return [];
      }
    },
    [mapResult, loaded],
  );

  const filterSearch = useCallback(
    async (celFilter: string): Promise<SearchResultItem[]> => {
      if (!loaded) return [];
      console.info("[filterSearch] CEL filter:", celFilter);
      try {
        const response = await client.search({
          filter: celFilter,
          pageSize: 20,
        });
        console.info("[filterSearch] results:", response.resources.length);
        const fieldPaths = extractFieldPaths(celFilter);
        return response.resources.map((r) => {
          const item = mapResult(r);
          if (fieldPaths.length > 0) {
            const res = r as unknown as Record<string, unknown>;
            item.matchFields = fieldPaths
              .map((p) => {
                const val = resolveFieldValue(res, p);
                if (val === undefined) return null;
                return {
                  path: p,
                  value:
                    typeof val === "object" ? JSON.stringify(val) : String(val),
                };
              })
              .filter((f): f is { path: string; value: string } => f !== null);
          }
          return item;
        });
      } catch (error) {
        console.error("[filterSearch] error:", error);
        return [];
      }
    },
    [mapResult, loaded],
  );

  return { search, filterSearch, loaded };
}
