import "./setup-checklist.scss";

import { createResourceApi, PluginLink } from "@ocm-fleet/common";
import type { Extension } from "@openshift/dynamic-plugin-sdk";
import {
  useExtensions,
  useResolvedExtensions,
} from "@openshift/dynamic-plugin-sdk";
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Icon,
  Split,
  SplitItem,
} from "@patternfly/react-core";
import {
  CheckCircleIcon,
  CircleIcon,
  TimesIcon,
} from "@patternfly/react-icons";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { getSetupProgressStore } from "../setup-plugin/setupProgress";
import {
  celResourceTypeFilter,
  completionOf,
  resourceTypesToCheck,
} from "./liveCompletion";

const DISMISS_KEY = "ocm-fleet:setup-checklist-dismissed";

const CATEGORY_LABELS: Record<string, string> = {
  "ocm-fleet.cluster-provider": "Configure a cluster provider",
};

const CATEGORY_CTA: Record<string, string> = {
  "ocm-fleet.cluster-provider": "Configure a provider",
};

const HIDDEN_CATEGORIES = new Set(["ocm-fleet.route"]);

type OnboardingActionExtension = Extension<
  "ocm-fleet.onboarding-action",
  {
    id: string;
    label: string;
    overviewCta?: string;
    category?: string;
    completeWhenResourceExists?: string;
  }
>;

function isOnboardingAction(e: Extension): e is OnboardingActionExtension {
  return e.type === "ocm-fleet.onboarding-action";
}

function useSetupProgressReadonly() {
  const [progress, setProgress] = useState<Record<string, boolean>>({});
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const store = getSetupProgressStore();

    store
      .getProgress()
      .then((state) => {
        if (!cancelled) {
          setProgress(state);
          setLoaded(true);
        }
      })
      .catch(() => {
        if (!cancelled) setLoaded(true);
      });

    const unsub = store.subscribe((state) => {
      if (!cancelled) setProgress(state);
    });

    return () => {
      cancelled = true;
      unsub();
    };
  }, []);

  return { progress, loaded };
}

interface ChecklistItem {
  id: string;
  label: string;
  completed: boolean;
  scope: string;
  module: string;
  search?: string;
  ctaText: string;
}

export default function SetupChecklist() {
  const { progress, loaded: progressLoaded } = useSetupProgressReadonly();
  const onboardingCandidates = useExtensions(isOnboardingAction);
  const [extensions, extensionsLoaded] =
    useResolvedExtensions(onboardingCandidates);
  const [dismissed, setDismissed] = useState(
    () => localStorage.getItem(DISMISS_KEY) === "true",
  );
  const [present, setPresent] = useState<Record<string, boolean>>({});
  const [liveLoaded, setLiveLoaded] = useState(false);

  const liveTypes = useMemo(
    () => resourceTypesToCheck(extensions.map((ext) => ext.properties)),
    [extensions],
  );
  const liveTypesKey = JSON.stringify(liveTypes);
  const checkedTypesKey = useRef<string | null>(null);

  // Steps that can be completed outside this browser ask the server
  // whether their resource exists. Until every answer is in, the
  // checklist does not paint, so a live hub never shows as missing.
  // Resolution hands back a fresh extensions array, so the same set of
  // types must not be searched twice.
  useEffect(() => {
    if (!extensionsLoaded || checkedTypesKey.current === liveTypesKey) return;
    checkedTypesKey.current = liveTypesKey;
    if (liveTypes.length === 0) {
      setLiveLoaded(true);
      return;
    }
    const api = createResourceApi("-");
    Promise.allSettled(
      liveTypes.map((type) =>
        api.search({ filter: celResourceTypeFilter(type), pageSize: 1 }),
      ),
    ).then((results) => {
      // A newer set of types supersedes this answer.
      if (checkedTypesKey.current !== liveTypesKey) return;
      const found: Record<string, boolean> = {};
      results.forEach((result, i) => {
        if (result.status === "fulfilled") {
          found[liveTypes[i]] = result.value.resources.length > 0;
        }
      });
      setPresent(found);
      setLiveLoaded(true);
    });
  }, [extensionsLoaded, liveTypes, liveTypesKey]);

  const items = useMemo<ChecklistItem[]>(() => {
    const setupItems: ChecklistItem[] = [];

    const grouped = new Map<string, OnboardingActionExtension[]>();
    const ungrouped: OnboardingActionExtension[] = [];

    for (const ext of extensions) {
      const cat = ext.properties.category;
      if (cat) {
        const list = grouped.get(cat);
        if (list) {
          list.push(ext);
        } else {
          grouped.set(cat, [ext]);
        }
      } else {
        ungrouped.push(ext);
      }
    }

    const actionItems: ChecklistItem[] = [];

    for (const [category, exts] of grouped) {
      if (HIDDEN_CATEGORIES.has(category)) continue;

      const anyComplete = exts.some((e) =>
        completionOf(e.properties, progress, present),
      );
      actionItems.push({
        id: `category:${category}`,
        label: CATEGORY_LABELS[category] ?? category,
        completed: anyComplete,
        scope: "settings-plugin",
        module: "ExtensionsPage",
        search: `?category=${category}`,
        ctaText: CATEGORY_CTA[category] ?? `Configure`,
      });
    }

    for (const ext of ungrouped) {
      actionItems.push({
        id: ext.properties.id,
        label: ext.properties.label,
        completed: completionOf(ext.properties, progress, present),
        scope: "settings-plugin",
        module: "ExtensionsPage",
        search: `?action=${ext.properties.id}`,
        ctaText:
          ext.properties.overviewCta ?? `Configure ${ext.properties.label}`,
      });
    }

    return [...setupItems, ...actionItems];
  }, [progress, present, extensions]);

  const completedCount = items.filter((i) => i.completed).length;
  const allComplete = items.length > 0 && completedCount === items.length;

  const dismiss = useCallback(() => {
    localStorage.setItem(DISMISS_KEY, "true");
    setDismissed(true);
  }, []);

  if (
    !progressLoaded ||
    !extensionsLoaded ||
    !liveLoaded ||
    dismissed ||
    allComplete
  ) {
    return null;
  }

  return (
    <Card className="ocm-fleet-overview-checklist">
      <CardHeader
        actions={{
          actions: (
            <Button
              variant="plain"
              aria-label="Dismiss setup checklist"
              icon={<TimesIcon />}
              onClick={dismiss}
            />
          ),
        }}
      >
        <CardTitle>
          <Split hasGutter>
            <SplitItem isFilled>Getting started</SplitItem>
            <SplitItem className="pf-v6-u-font-size-sm pf-v6-u-text-color-subtle">
              {completedCount} of {items.length} complete
            </SplitItem>
          </Split>
        </CardTitle>
      </CardHeader>
      <CardBody>
        <ul className="ocm-fleet-overview-checklist__list">
          {items.map((item) => (
            <li key={item.id} className="ocm-fleet-overview-checklist__item">
              <Icon size="md" status={item.completed ? "success" : undefined}>
                {item.completed ? <CheckCircleIcon /> : <CircleIcon />}
              </Icon>
              <span
                className={
                  item.completed ? "pf-v6-u-text-color-subtle" : undefined
                }
              >
                {item.label}
              </span>
              {!item.completed && (
                <PluginLink
                  scope={item.scope}
                  module={item.module}
                  to={item.search ? { search: item.search } : undefined}
                  className="pf-v6-u-ml-auto"
                >
                  {item.ctaText}
                </PluginLink>
              )}
            </li>
          ))}
        </ul>
      </CardBody>
    </Card>
  );
}
