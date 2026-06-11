# Operators Blip Degraded=True During Upgrades

**Bugs:** [OCPBUGS-38661](https://redhat.atlassian.net/browse/OCPBUGS-38661) (kube-apiserver), [OCPBUGS-38662](https://redhat.atlassian.net/browse/OCPBUGS-38662) (kube-controller-manager), [OCPBUGS-38663](https://redhat.atlassian.net/browse/OCPBUGS-38663) (kube-scheduler)

**Parent initiative:** [TRT-1578](https://redhat.atlassian.net/browse/TRT-1578) — Ensure all HA components are not degraded by design during upgrades

## Status Summary (as of 2026-06-11)

| Bug | Component | Status | Assignee |
|-----|-----------|--------|----------|
| OCPBUGS-38661 | kube-apiserver | New | Ondra Kupka |
| OCPBUGS-38662 | kube-controller-manager | POST | Ondra Kupka |
| OCPBUGS-38663 | kube-scheduler | New | Ondra Kupka |

**Escalation:** On 2026-06-11, W. Trevor King set OCPBUGS-38663 as **Release Blocker: Approved**, Target Version: **5.0.0**, because it blocks [OCPBUILD-176](https://redhat.atlassian.net/browse/OCPBUILD-176), which is part of the Release Blocker [OCPSTRAT-2949](https://redhat.atlassian.net/browse/OCPSTRAT-2949).

**Open PRs:**
- [library-go#2081](https://github.com/openshift/library-go/pull/2081) — MCO annotation-based upgrade detection. Open since 2026-01-19, last activity 2026-03-25. **Approach rejected** by `#forum-ocp-arch` consensus — needs rework or closure.
- [cluster-kube-apiserver-operator#2052](https://github.com/openshift/cluster-kube-apiserver-operator/pull/2052) — Configures `WithDegradedInertia` for KAS-O (by tjungblu). Still open.

## The Problem

During cluster upgrades, nodes are rebooted one at a time by MCO. While a node is rebooting, library-go controllers detect problems and report `Degraded=True`. This violates the ClusterOperator contract:

> "A service should not report Degraded during the course of a normal upgrade."
> — [ClusterOperator dev guide](https://github.com/openshift/enhancements/blob/master/dev-guide/cluster-version-operator/dev/clusteroperator.md)

Three operators are affected: **kube-apiserver**, **kube-controller-manager**, and **kube-scheduler**. All three use the same library-go static pod controller framework. A fourth operator, **etcd**, uses the same framework but is **not affected** because CEO already has `WithDegradedInertia` configured — proving the fix works.

### What Actually Fires in CI

From recent 5.0 upgrade job artifacts ([AWS 4.22→5.0](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-release-main-ci-5.0-upgrade-from-stable-4.22-e2e-aws-ovn-upgrade/2063958455428845568), [GCP 5.0](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-release-main-ci-5.0-e2e-gcp-ovn-upgrade/2063958506695823360), [GCP 5.0](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-release-main-ci-5.0-e2e-gcp-ovn-upgrade/2063958515092819968)), **two controllers are firing Degraded:**

| Operator | Reason | Controller | Frequency |
|----------|--------|------------|-----------|
| kube-apiserver | `NodeController_MasterNodesReady` | NodeController | Every upgrade job |
| kube-controller-manager | `NodeController_MasterNodesReady` | NodeController | Every upgrade job |
| kube-controller-manager | `NodeController_MasterNodesReady::StaticPods_Error` | NodeController + StaticPodStateController | AWS jobs |
| kube-scheduler | `NodeController_MasterNodesReady` | NodeController | Every upgrade job |
| openshift-samples | `APIServerServiceUnavailableError` | (downstream) | Occasionally |

1. **NodeController** (`node/node_controller.go`) — `NodeControllerDegraded`: Monitors master node Ready conditions. Fires within seconds of kubelet stopping on the rebooting node. This is the primary and consistent trigger across all three operators.

2. **StaticPodStateController** (`staticpodstate/staticpodstate_controller.go`) — `StaticPodsDegraded`: Monitors running static pod containers. Fires alongside NodeController on KCM when containers are stuck in `ContainerCreating` on the rebooting node.

`GuardController_SyncError` was reported in the [OCPBUGS-38663 description for 4.21](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-multiarch-master-nightly-4.21-ocp-e2e-aws-ovn-upgrade-multi-x-ax/1991368329922613248) but was **not observed** in the 5.0 jobs checked. Other controllers (InstallerController, InstallerStateController, MissingStaticPodController) have longer timeouts and are not firing during typical cloud node reboots.

### Other Controllers That Could Theoretically Fire

All controllers live under `library-go/pkg/operator/staticpod/controller/`. Beyond the two that are actually firing, these could fire in slower environments (e.g., bare-metal):

- **GuardController** (`guard/guard_controller.go`) — `GuardControllerDegraded`: Sync errors when updating guard pods on not-ready nodes. Was seen in 4.21 CI.
- **InstallerController** (`installer/installer_controller.go`) — `InstallerControllerDegraded` / `NodeInstallerDegraded`: Fails when trying to install a new static pod revision on a rebooting node.
- **InstallerStateController** (`installerstate/installer_state_controller.go`) — `InstallerPodPendingDegraded` / `InstallerPodContainerWaitingDegraded` / `InstallerPodNetworkingDegraded`: Fires when installer pods are stuck Pending for >5 minutes.
- **MissingStaticPodController** (`missingstaticpod/missing_static_pod_controller.go`): Fires when a static pod doesn't appear after installer completion (timeout: 150s multi-node / 180s SNO).

### Shared Code Path

All four static pod operators (KAS-O, KCM-O, KSO, CEO) use `staticpod.NewBuilder(...).ToControllers()` from library-go (`pkg/operator/staticpod/controllers.go`). The builder creates all controllers unconditionally. Minor differences:

- **KAS-O** enables `WithStartupMonitor` and `WithMinReadyDuration(30s)`, uses `WithCustomInstaller` with `installerErrorInjector`
- **KCM-O** and **KSO** use plain `WithInstaller`
- **CEO** is the only one with `WithDegradedInertia` configured (2-minute default, 5 minutes for `NodeControllerDegraded` and `EtcdMembersDegraded` and `DefragControllerDegraded`)

### Sippy Data (5.0, as of 2026-06-11)

`[Monitor:legacy-cvo-invariants]` Degraded condition test results (1241 upgrade runs):

| Operator | Pass Rate | Flakes | Hard Fails | Status |
|----------|-----------|--------|------------|--------|
| **kube-apiserver** | **54.1%** | 569 | 0 | Excepted → OCPBUGS-38661 |
| **kube-controller-manager** | **54.5%** | 565 | 0 | Excepted → OCPBUGS-38662 |
| **kube-scheduler** | **54.6%** | 564 | 0 | Excepted → OCPBUGS-38663 |
| console | 95.8% | 52 | 0 | Excepted (separate issue) |
| machine-config | 97.7% | 0 | 62 | Not excepted (separate issue) |
| **etcd** | **100%** | 0 | 0 | **Clean — has `WithDegradedInertia`** |
| All others | 99.5–100% | 0–3 | 0–3 | Clean or near-clean |

**~46% of all upgrade runs** trigger the Degraded blip on KAS, KCM, and KSO. These are suppressed by CI exceptions (recorded as "flakes", not hard failures).

### How Long Do Reboots Actually Take?

From the [AWS 4.22→5.0 job](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-release-main-ci-5.0-upgrade-from-stable-4.22-e2e-aws-ovn-upgrade/2063958455428845568), the Degraded blips per node were:

| Node | Degraded=True | Degraded=False | Duration |
|------|---------------|----------------|----------|
| Node 1 | 14:05:52 | 14:06:13 | **21 seconds** |
| Node 2 | 14:12:07 | 14:12:22 | **15 seconds** |
| Node 3 | 14:19:46 | 14:19:57 | **11 seconds** |

On cloud (AWS/GCP), node reboots cause Degraded blips of **11–21 seconds**. CEO's 5-minute inertia easily absorbs this.

**Bare-metal CI** ([4.22→5.0 BM upgrade](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-release-main-nightly-5.0-upgrade-from-stable-4.22-e2e-metal-ipi-ovn-upgrade/2064943236853534720), [5.0 BM upgrade](https://prow.ci.openshift.org/view/gs/test-platform-results/logs/periodic-ci-openshift-release-main-nightly-5.0-e2e-metal-ipi-ovn-upgrade/2064943251114168320)) showed **zero Degraded=True events** and **zero NotReady events** — nodes do not appear to go NotReady during these upgrades. This is likely because bare-metal CI uses dev-scripts VMs, not real hardware, and the reboot characteristics differ. **Real bare-metal reboots (BIOS POST, RAID init, firmware checks) can take 10–20+ minutes**, which is well beyond CEO's 5-minute inertia.

This means CEO's 100% pass rate only proves the approach works for cloud and dev-scripts environments. It does **not** validate the inertia approach for real bare-metal deployments.

## Why We're Blocked

### The Design Question (Resolved)

Three approaches were discussed:

**Approach A — Detect upgrade via MCO annotations (PR #2081):** Read MCO annotations on Node objects, skip degrading for nodes in `Rebooting` or `Working` state. Add a safety timeout (1–2h).

**Approach B — Degraded inertia only:** Use `StatusSyncer.WithDegradedInertia` to add a delay before reporting Degraded. Covers all controllers uniformly without upgrade awareness.

**Approach C — Platform-level solution:** Wait for Kubernetes KEP [kubernetes/enhancements#5769](https://github.com/kubernetes/enhancements/pull/5769) to provide a standard upgrade-awareness mechanism. Not available yet — the KEP is not even merged.

### Resolution: Approach A is Rejected

The [`#forum-ocp-arch` discussion](https://redhat-internal.slack.com/archives/C011CSSPBLK/p1774461403805309) reached consensus (sdodson, wking, deads2k):

- **Components must not behave differently based on upgrade status.** This is a confirmed design principle.
- Any suppression of degraded conditions due to an upgrade is considered a mistake.
- **Inertia values should be chosen to be sane without knowledge of cluster upgrade state.**
- Disabling CI errors for edge cases is an improper solution.
- Setting inertia based on the **environment** (e.g., longer on bare-metal) was mentioned as acceptable.

**PR #2081 cannot be merged as-is** — it fundamentally relies on detecting upgrades.

The environment-based inertia option is notable but arguably leaks intent — if the only reason to set a longer inertia on bare-metal is that reboots take longer there, the operator is still implicitly adapting to upgrade behavior, just indirectly through platform type rather than MCO annotations.

### The Unsolved Problem

The path forward is Approach B (inertia only), but there is a fundamental tension:

- **Cloud reboots** take 11–21 seconds → a 5-minute inertia is plenty
- **Real bare-metal reboots** can take 10–20+ minutes → a 5-minute inertia is insufficient
- A 20–30 minute inertia would cover bare-metal but would also **delay detection of genuine failures by 20–30 minutes in all environments**, which is unacceptable

Per the `#forum-ocp-arch` consensus, the inertia must be "sane without knowledge of cluster upgrade state" — meaning the same timeout applies whether or not an upgrade is happening. This makes it very hard to pick a value that works for both fast cloud reboots (where 5 minutes is fine) and slow bare-metal reboots (where 5 minutes is not enough), without also masking real problems.

This tension is why the PR discussion stalled even before the design objection. It remains the core unsolved problem.

## Timeline

| Date | Event |
|------|-------|
| 2024-08-19 | All three bugs filed by TRT (Ken Zhang) as part of HA degradation audit |
| 2024-10-08 | Arda Guclu investigates, confirms kubelet/cri-o stopping causes degradation |
| 2024-10-22 | Filip Krepinsky provides detailed analysis of the node reboot sequence |
| 2024-10-23 | Petr Muller (TRT) rejects test-side fix: "this is a user-facing problem" |
| 2025-06-30 | Filip unassigns himself due to leave |
| 2026-01-19 | PR #2081 opened (tchap) — MCO annotation-based upgrade detection |
| 2026-01-27 | p0lyn0mial raises concerns: prefers `WithDegradedInertia`, wants broader solution |
| 2026-02-19 | tjungblu reports degraded inertia works well on CEO; opens KAS-O PR #2052 |
| 2026-02-23 | atiratree reviews, suggests improvements (maxUnavailable, cordon detection) |
| 2026-03-02 | Team proposes layered strategy; p0lyn0mial still prefers inertia-first |
| 2026-03-23 | Subin Modeel asks about 4.22 timeline; Ondra responds "blocked on review" |
| 2026-03-25 | sdodson raises design objection, asks for `#forum-ocp-arch` discussion |
| 2026-03-25 | `#forum-ocp-arch` consensus: operators must not detect upgrades |
| 2026-03-25 | Last activity on PR #2081 |
| 2026-06-11 | W. Trevor King marks OCPBUGS-38663 as **Release Blocker for 5.0.0** |

## What Would Unblock This

The design question is resolved — operators must not detect upgrades. The remaining work:

1. **Apply `WithDegradedInertia` to KAS-O, KCM-O, and KSO** — following the CEO pattern. KAS-O PR [#2052](https://github.com/openshift/cluster-kube-apiserver-operator/pull/2052) proposes 2-minute default + 3 minutes for `NodeControllerDegraded`. Equivalent PRs are needed for KCM-O and KSO.
2. **Decide on inertia values** — CEO uses 5 minutes for `NodeControllerDegraded`, PR #2052 proposes 3 minutes. Both are sufficient for cloud (11–21s blips) but insufficient for real bare-metal (10–20+ min reboots). Options:
   - Accept that inertia only solves the cloud case and bare-metal will continue to blip (pragmatic — BM CI doesn't reproduce the issue anyway)
   - Use a longer inertia (15–30 min) that covers bare-metal at the cost of delayed failure detection in all environments
   - **Set inertia based on environment/platform** (e.g., longer on bare-metal) — the `#forum-ocp-arch` thread mentioned this is acceptable, though it arguably leaks upgrade-awareness intent through the platform type
   - Revisit the `#forum-ocp-arch` decision with the bare-metal data as evidence that inertia alone is insufficient
3. **Rework or close PR [#2081](https://github.com/openshift/library-go/pull/2081)** — the MCO annotation detection approach is ruled out per current consensus.
4. **Cover `StaticPodsDegraded` in the inertia config** — not just `NodeControllerDegraded`. KCM shows the compound reason `NodeController_MasterNodesReady::StaticPods_Error` in CI.

## Impact if Not Fixed

- CI upgrade tests require exceptions for all three operators (currently ~46% of upgrade runs trigger the blip)
- Users see spurious `Degraded=True` on kube-apiserver, kube-controller-manager, and kube-scheduler during every upgrade
- Blocks OCPBUILD-176 and OCPSTRAT-2949 (Release Blocker for 5.0.0)
