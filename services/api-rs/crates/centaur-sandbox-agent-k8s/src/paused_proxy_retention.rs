//! Bounding the retained iron-proxy pods of paused sandboxes.
//!
//! Pausing tears down the agent pod but keeps the Sandbox CR and its
//! iron-proxy pod so a session can resume without a cold create. That
//! retention is deliberate, but it is unbounded against the node's pod
//! budget: every retained proxy holds one of the node's allocatable pod
//! slots, and once enough accumulate the scheduler refuses new sandbox
//! pods with `FailedScheduling: Too many pods`, so a turn dies before it
//! starts.
//!
//! The sweep runs periodically, compares each steering node's current pod
//! load with its allocatable pod count, and evicts the longest-paused
//! proxies beyond the node's remaining headroom. An evicted sandbox keeps
//! its CR; `resume` recreates the proxy on demand, so only the evicted
//! session's resume pays the cold-create cost.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use centaur_sandbox_core::SandboxResult;
use jiff::Timestamp;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::ListParams;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::iron_proxy::IRON_PROXY_LABEL;
use crate::{
    AgentSandboxBackend, MANAGED_BY_LABEL, MANAGED_BY_VALUE, SANDBOX_ID_LABEL, SandboxId,
    map_kube_error,
};

/// Retention policy for paused sandboxes' iron-proxy pods.
#[derive(Clone, Copy, Debug)]
pub struct PausedProxyRetentionConfig {
    /// How often to sweep. `None` disables the sweep.
    pub interval: Option<Duration>,
    /// Pod slots each node keeps free beyond its current load, covering the
    /// agent pod and proxy pod a new sandbox needs to schedule.
    pub margin_pods: usize,
    /// Absolute ceiling on retained paused proxies. `None` bounds retention
    /// by node capacity alone.
    pub cap: Option<usize>,
}

impl PausedProxyRetentionConfig {
    pub fn is_enabled(&self) -> bool {
        self.interval.is_some()
    }
}

impl Default for PausedProxyRetentionConfig {
    /// Retention is bounded by node pod capacity, swept every five minutes,
    /// with two pod slots of headroom kept free per node.
    fn default() -> Self {
        Self {
            interval: Some(Duration::from_secs(300)),
            margin_pods: 2,
            cap: None,
        }
    }
}

/// One steering node's pod budget as a sweep pass observed it.
#[derive(Clone, Debug)]
pub struct NodePodBudget {
    pub name: String,
    pub allocatable_pods: usize,
    /// Pods scheduled on the node, excluding terminating pods and evictable
    /// paused proxies.
    pub load_pods: usize,
    /// Evictable paused proxies held on the node.
    pub retained_pods: usize,
}

/// A retained proxy a sweep may evict: the proxy pod of a paused sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedPausedProxy {
    pub node: String,
    pub sandbox_id: String,
    pub paused_at: Timestamp,
}

/// Indices into `retained` a sweep pass should evict.
///
/// Per node, retention may hold at most
/// `allocatable_pods - load_pods - margin_pods` slots, leaving the headroom
/// a new sandbox's pods need. When `cap` is set, retention may hold at most
/// `cap` slots in total. Victims are the longest-paused first; ties break
/// on sandbox id so the selection is deterministic.
pub fn select_evictions(
    budgets: &[NodePodBudget],
    retained: &[RetainedPausedProxy],
    margin_pods: usize,
    cap: Option<usize>,
) -> BTreeSet<usize> {
    let order = |a: &usize, b: &usize| {
        retained[*a]
            .paused_at
            .cmp(&retained[*b].paused_at)
            .then_with(|| retained[*a].sandbox_id.cmp(&retained[*b].sandbox_id))
    };

    let mut by_node: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, entry) in retained.iter().enumerate() {
        by_node.entry(entry.node.as_str()).or_default().push(index);
    }

    let mut evicted: BTreeSet<usize> = BTreeSet::new();
    for (node, indices) in by_node.iter_mut() {
        indices.sort_by(order);
        let Some(budget) = budgets.iter().find(|budget| budget.name == *node) else {
            // The node is gone or unobservable: its budget is unknown, so
            // its retained proxies stay (the node may simply have drained).
            continue;
        };
        let allowed = budget
            .allocatable_pods
            .saturating_sub(budget.load_pods)
            .saturating_sub(margin_pods);
        let excess = indices.len().saturating_sub(allowed);
        evicted.extend(indices.iter().take(excess).copied());
    }

    if let Some(cap) = cap {
        let mut remaining: Vec<usize> = (0..retained.len())
            .filter(|index| !evicted.contains(index))
            .collect();
        remaining.sort_by(order);
        let excess = remaining.len().saturating_sub(cap);
        evicted.extend(remaining.into_iter().take(excess));
    }

    evicted
}

/// What one sweep pass did.
#[derive(Clone, Debug, Default)]
pub struct PausedProxyRetentionReport {
    pub evicted: usize,
    pub retained: usize,
    pub node_budgets: Vec<NodePodBudget>,
}

pub struct PausedProxyRetentionSweep {
    backend: Arc<AgentSandboxBackend>,
}

impl PausedProxyRetentionSweep {
    pub fn new(backend: Arc<AgentSandboxBackend>) -> Self {
        Self { backend }
    }

    /// Run the sweep on its interval. A no-op when the sweep is disabled.
    ///
    /// The first pass runs immediately so a restart heals an over-full node
    /// before new turns try to schedule on it.
    pub fn spawn(self) {
        let Some(sweep_interval) = self.backend.config.paused_proxy_retention.interval else {
            return;
        };
        tokio::spawn(async move {
            let mut tick = interval(sweep_interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(error) = self.sweep_once().await {
                    warn!(%error, "paused proxy retention sweep failed");
                }
            }
        });
    }

    /// One sweep pass. A failed eviction is logged and skipped so one
    /// wedged sandbox cannot stall the pass.
    pub async fn sweep_once(&self) -> SandboxResult<PausedProxyRetentionReport> {
        let retention = self.backend.config.paused_proxy_retention;
        let mut node_budgets = self.observe_node_budgets().await?;
        if node_budgets.is_empty() {
            // No steering node exposed a pod budget. Nothing is observable,
            // so nothing is evicted.
            return Ok(PausedProxyRetentionReport::default());
        }
        let steering: BTreeSet<&str> = node_budgets
            .iter()
            .map(|budget| budget.name.as_str())
            .collect();
        let (load, candidates) = self.observe_cluster_pods(&steering).await?;
        let retained = self.paused_proxy_candidates(&candidates).await?;

        for budget in &mut node_budgets {
            budget.load_pods = load.get(&budget.name).copied().unwrap_or_default();
            budget.retained_pods = retained
                .iter()
                .filter(|entry| entry.node == budget.name)
                .count();
        }

        let victims = select_evictions(
            &node_budgets,
            &retained,
            retention.margin_pods,
            retention.cap,
        );
        if victims.is_empty() {
            debug!(
                retained = retained.len(),
                ?node_budgets,
                "paused proxy retention sweep kept all proxies"
            );
            return Ok(PausedProxyRetentionReport {
                evicted: 0,
                retained: retained.len(),
                node_budgets,
            });
        }

        let mut ordered: Vec<RetainedPausedProxy> = victims
            .into_iter()
            .map(|index| retained[index].clone())
            .collect();
        ordered.sort_by(|a, b| {
            a.paused_at
                .cmp(&b.paused_at)
                .then_with(|| a.sandbox_id.cmp(&b.sandbox_id))
        });

        let mut evicted = 0;
        for victim in ordered {
            match self.evict_one(&victim).await {
                Ok(()) => {
                    evicted += 1;
                    info!(
                        event = "sandbox_paused_proxy_evicted",
                        sandbox_id = %victim.sandbox_id,
                        node = %victim.node,
                        paused_at = %victim.paused_at,
                        "evicted paused sandbox proxy to free node pod slots"
                    );
                }
                Err(error) => {
                    warn!(
                        sandbox_id = %victim.sandbox_id,
                        node = %victim.node,
                        %error,
                        "failed to evict paused sandbox proxy; retrying next sweep"
                    );
                }
            }
        }
        info!(
            event = "sandbox_paused_proxy_retention_sweep",
            retained = retained.len(),
            evicted,
            ?node_budgets,
            "paused proxy retention sweep completed"
        );
        Ok(PausedProxyRetentionReport {
            evicted,
            retained: retained.len(),
            node_budgets,
        })
    }

    /// The steering nodes and their allocatable pod counts. Nodes without a
    /// readable `pods` allocatable are skipped: evicting against an unknown
    /// budget could free slots a node still needs.
    async fn observe_node_budgets(&self) -> SandboxResult<Vec<NodePodBudget>> {
        let nodes = kube::Api::<Node>::all(self.backend.client.clone());
        let nodes = nodes
            .list(&ListParams::default())
            .await
            .map_err(|err| map_kube_error("list nodes for proxy retention", err))?;
        let selector = &self.backend.config.node_selector;
        Ok(nodes
            .items
            .iter()
            .filter(|node| node_matches_selector(node, selector))
            .filter_map(|node| {
                let name = node.metadata.name.clone()?;
                let allocatable = node_allocatable_pods(node)?;
                Some(NodePodBudget {
                    name,
                    allocatable_pods: allocatable,
                    load_pods: 0,
                    retained_pods: 0,
                })
            })
            .collect())
    }

    /// A cluster-wide pod census over the steering nodes: per-node load
    /// (everything except evictable paused proxies) and the candidate proxy
    /// pods, keyed by sandbox id with their node.
    async fn observe_cluster_pods(
        &self,
        steering: &BTreeSet<&str>,
    ) -> SandboxResult<(BTreeMap<String, usize>, BTreeMap<String, String>)> {
        let pods = kube::Api::<Pod>::all(self.backend.client.clone());
        let pods = pods
            .list(&ListParams::default())
            .await
            .map_err(|err| map_kube_error("list pods for proxy retention", err))?;
        let mut load: BTreeMap<String, usize> = BTreeMap::new();
        let mut candidates: BTreeMap<String, String> = BTreeMap::new();
        for pod in pods.items {
            let Some(node_name) = pod.spec.as_ref().and_then(|spec| spec.node_name.clone()) else {
                continue;
            };
            if !steering.contains(node_name.as_str()) || pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let labels = pod.metadata.labels.as_ref();
            let sandbox_id = if labels.is_some_and(|labels| labels.contains_key(IRON_PROXY_LABEL)) {
                labels
                    .and_then(|labels| labels.get(SANDBOX_ID_LABEL))
                    .map(|id| id.to_owned())
            } else {
                None
            };
            match sandbox_id {
                // A proxy pod we cannot attribute to a sandbox still counts
                // as load: it holds a slot the node budget has to cover.
                Some(sandbox_id) => {
                    candidates.entry(sandbox_id).or_insert_with(|| node_name);
                }
                None => *load.entry(node_name).or_default() += 1,
            }
        }
        Ok((load, candidates))
    }

    /// The candidates whose sandbox is actually paused. Candidates with a
    /// missing CR, a running replica, an unorderable pause, or an in-flight
    /// resume are kept out: eviction only costs resume speed, so anything
    /// ambiguous stays.
    async fn paused_proxy_candidates(
        &self,
        candidates: &BTreeMap<String, String>,
    ) -> SandboxResult<Vec<RetainedPausedProxy>> {
        let params =
            ListParams::default().labels(&format!("{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"));
        let sandboxes = self
            .backend
            .sandboxes()
            .list(&params)
            .await
            .map_err(|err| map_kube_error("list sandboxes for proxy retention", err))?;
        let by_name: BTreeMap<&str, &crate::crd::Sandbox> = sandboxes
            .items
            .iter()
            .filter_map(|sandbox| sandbox.metadata.name.as_deref().map(|name| (name, sandbox)))
            .collect();
        let resuming = self.backend.resuming_sandbox_ids();
        let mut retained = Vec::new();
        for (sandbox_id, node) in candidates {
            if resuming.contains(sandbox_id.as_str()) {
                continue;
            }
            let Some(sandbox) = by_name.get(sandbox_id.as_str()) else {
                continue;
            };
            if sandbox.spec.replicas.unwrap_or(1) != 0 {
                continue;
            }
            // Parse the annotation directly: it is the RFC 3339 instant the
            // pause patch wrote, so no SystemTime round trip can lose it.
            let Some(raw) = sandbox
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(crate::PAUSED_AT_ANNOTATION))
            else {
                continue;
            };
            let Ok(paused_at) = raw.parse::<Timestamp>() else {
                continue;
            };
            retained.push(RetainedPausedProxy {
                node: node.clone(),
                sandbox_id: sandbox_id.clone(),
                paused_at,
            });
        }
        Ok(retained)
    }

    async fn evict_one(&self, victim: &RetainedPausedProxy) -> SandboxResult<()> {
        let id = SandboxId::new(victim.sandbox_id.as_str());
        // Deregister the iron-control proxy row by its durable OID (pod
        // annotation) so the sweep works across api-rs restarts, not just
        // against the in-memory id map.
        if let Some(proxy_id) = self.backend.proxy_id_for_sandbox(&id).await? {
            let _ = self
                .backend
                .config
                .iron_control
                .client
                .delete_proxy(&proxy_id)
                .await;
        }
        self.backend.delete_iron_proxy_resources(&id).await
    }
}

fn node_matches_selector(node: &Node, selector: &BTreeMap<String, String>) -> bool {
    selector.iter().all(|(key, value)| {
        node.metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get(key))
            .is_some_and(|label| label == value)
    })
}

fn node_allocatable_pods(node: &Node) -> Option<usize> {
    let allocatable = node.status.as_ref()?.allocatable.as_ref()?.get("pods")?;
    allocatable.0.trim().parse::<usize>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, allocatable: usize, load: usize) -> NodePodBudget {
        NodePodBudget {
            name: name.to_owned(),
            allocatable_pods: allocatable,
            load_pods: load,
            retained_pods: 0,
        }
    }

    /// `asbx-000` is the most recently paused; higher indices paused longer
    /// ago.
    fn paused_on(node: &str, count: usize) -> Vec<RetainedPausedProxy> {
        (0..count)
            .map(|i| RetainedPausedProxy {
                node: node.to_owned(),
                sandbox_id: format!("asbx-{i:03}"),
                paused_at: Timestamp::now() - Duration::from_secs(i as u64),
            })
            .collect()
    }

    #[test]
    fn evicts_oldest_beyond_node_headroom() {
        let budgets = [node("node-a", 250, 168)];
        let retained = paused_on("node-a", 88);
        // allowed = 250 - 168 - 2 = 80; the 8 longest-paused go.
        let victims = select_evictions(&budgets, &retained, 2, None);
        assert_eq!(victims.len(), 8);
        for index in 80..88 {
            assert!(
                victims.contains(&index),
                "asbx-{index:03} should be evicted"
            );
        }
        for index in 0..80 {
            assert!(!victims.contains(&index), "asbx-{index:03} should be kept");
        }
    }

    #[test]
    fn keeps_retention_within_headroom() {
        let budgets = [node("node-a", 250, 100)];
        let retained = paused_on("node-a", 20);
        assert!(select_evictions(&budgets, &retained, 2, None).is_empty());
    }

    #[test]
    fn saturates_to_zero_when_node_is_full() {
        let budgets = [node("node-a", 10, 12)];
        let retained = paused_on("node-a", 3);
        // allowed = 10 - 12 - 2 = 0; every retained proxy goes.
        assert_eq!(
            select_evictions(&budgets, &retained, 2, None),
            BTreeSet::from([0, 1, 2])
        );
    }

    #[test]
    fn cap_bounds_retention_globally() {
        let budgets = [node("node-a", 100, 10), node("node-b", 100, 10)];
        let retained = vec![
            RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: "asbx-001".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(40),
            },
            RetainedPausedProxy {
                node: "node-b".to_owned(),
                sandbox_id: "asbx-002".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(30),
            },
            RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: "asbx-003".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(20),
            },
            RetainedPausedProxy {
                node: "node-b".to_owned(),
                sandbox_id: "asbx-004".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(10),
            },
        ];
        // Node headroom is ample (allowed 88 each); the cap of 2 evicts the
        // two globally oldest (asbx-001, asbx-002).
        assert_eq!(
            select_evictions(&budgets, &retained, 2, Some(2)),
            BTreeSet::from([0, 1])
        );
    }

    #[test]
    fn cap_never_expands_node_budget() {
        let budgets = [node("node-a", 20, 17), node("node-b", 100, 10)];
        let retained = vec![
            RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: "asbx-a1".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(30),
            },
            RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: "asbx-a2".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(20),
            },
            RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: "asbx-a3".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(10),
            },
            RetainedPausedProxy {
                node: "node-b".to_owned(),
                sandbox_id: "asbx-b1".to_owned(),
                paused_at: Timestamp::now() - Duration::from_secs(40),
            },
        ];
        // node-a headroom is 20 - 17 - 2 = 1, so its two oldest go no matter
        // how generous the global cap is; node-b is far under budget.
        assert_eq!(
            select_evictions(&budgets, &retained, 2, Some(100)),
            BTreeSet::from([0, 1])
        );
        // A cap tighter than the surviving count reaches into the survivors:
        // of {asbx-a3, asbx-b1} the globally oldest (asbx-b1) goes.
        assert_eq!(
            select_evictions(&budgets, &retained, 2, Some(1)),
            BTreeSet::from([0, 1, 3])
        );
    }

    #[test]
    fn unobservable_node_keeps_its_proxies() {
        let base = Timestamp::now();
        let budgets = [node("node-a", 20, 5)];
        let retained = vec![
            RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: "asbx-a1".to_owned(),
                paused_at: base - Duration::from_secs(30),
            },
            RetainedPausedProxy {
                node: "node-gone".to_owned(),
                sandbox_id: "asbx-g1".to_owned(),
                paused_at: base - Duration::from_secs(40),
            },
        ];
        // node-gone is absent from the budget list (drained or unobservable),
        // so no node-budget eviction applies to it; node-a headroom (13)
        // covers its one proxy, and nothing is evicted.
        assert!(select_evictions(&budgets, &retained, 2, None).is_empty());
    }

    #[test]
    fn ties_break_on_sandbox_id() {
        let base = Timestamp::now();
        let retained: Vec<RetainedPausedProxy> = ["asbx-z", "asbx-a", "asbx-m"]
            .into_iter()
            .map(|sandbox_id| RetainedPausedProxy {
                node: "node-a".to_owned(),
                sandbox_id: sandbox_id.to_owned(),
                paused_at: base,
            })
            .collect();
        // One slot of headroom keeps a single survivor; the paused-at set is
        // a three-way tie, so the sandbox id orders the victims (asbx-a and
        // asbx-m go before asbx-z, which is kept).
        assert_eq!(
            select_evictions(&[node("node-a", 11, 8)], &retained, 2, None),
            BTreeSet::from([1, 2])
        );
        // Zero headroom: every tied proxy is evicted.
        assert_eq!(
            select_evictions(&[node("node-a", 10, 8)], &retained, 2, None),
            BTreeSet::from([0, 1, 2])
        );
    }

    #[test]
    fn retention_config_defaults_and_toggle() {
        let default = PausedProxyRetentionConfig::default();
        assert!(default.is_enabled());
        assert_eq!(default.interval, Some(Duration::from_secs(300)));
        assert_eq!(default.margin_pods, 2);
        assert!(default.cap.is_none());

        let disabled = PausedProxyRetentionConfig {
            interval: None,
            ..default
        };
        assert!(!disabled.is_enabled());
    }
}
