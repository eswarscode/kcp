# KCP Syncer and Logical Cluster Deletion - Deep Dive

## Table of Contents
1. [Syncer Architecture](#syncer-architecture)
2. [Syncer Components](#syncer-components)
3. [Syncer Request Flow](#syncer-request-flow)
4. [State Machine & Labels](#state-machine--labels)
5. [Namespace Mapping](#namespace-mapping)
6. [Logical Cluster Deletion](#logical-cluster-deletion)
7. [Resource Cleanup Process](#resource-cleanup-process)
8. [Complete Flow Examples](#complete-flow-examples)

---

## Syncer Architecture

### Overview

The **syncer** is a critical component in KCP that runs on each physical Kubernetes cluster (SyncTarget). It acts as a bidirectional synchronization bridge between KCP's logical clusters and physical Kubernetes clusters.

**Key Responsibilities:**
1. **Spec Synchronization (Downstream)**: Copy resource specifications from KCP → Physical cluster
2. **Status Synchronization (Upstream)**: Copy resource status from Physical cluster → KCP
3. **Upsync**: Allow physical cluster to create resources that sync back to KCP
4. **API Negotiation**: Make physical cluster APIs available in KCP workspaces
5. **Heartbeat**: Maintain connection health with KCP

**Architecture Diagram:**

```
┌─────────────────────────────────────────────────────────────────┐
│                         KCP Server                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Logical Cluster: root:org:workspace                       │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐         │  │
│  │  │ Deployment │  │   Service  │  │ ConfigMap  │         │  │
│  │  │ my-app     │  │   my-svc   │  │  app-cfg   │         │  │
│  │  │            │  │            │  │            │         │  │
│  │  │ spec:      │  │ spec:      │  │ data:      │         │  │
│  │  │   replicas │  │   ports    │  │   key=val  │         │  │
│  │  │            │  │            │  │            │         │  │
│  │  │ labels:    │  │            │  │            │         │  │
│  │  │   state... │  │            │  │            │         │  │
│  │  │   =Sync    │  │            │  │            │         │  │
│  │  └────────────┘  └────────────┘  └────────────┘         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Virtual Workspaces (Syncer View)                          │  │
│  │  - Filters resources by placement labels                  │  │
│  │  - URL: https://kcp.io/syncer/target-1                    │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Watches via Virtual Workspace
                              │ Updates via API Server
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Syncer Process                                │
│  (Runs on Physical Cluster)                                     │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Spec Syncer  │  │Status Syncer │  │  Upsyncer    │         │
│  │              │  │              │  │              │         │
│  │ KCP→Cluster  │  │ Cluster→KCP  │  │ Cluster→KCP  │         │
│  │              │  │              │  │              │         │
│  │ Watches      │  │ Watches      │  │ Watches      │         │
│  │ upstream     │  │ downstream   │  │ downstream   │         │
│  │              │  │              │  │ with         │         │
│  │              │  │              │  │ Upsync label │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Namespace    │  │  Heartbeat   │  │ API Importer │         │
│  │ Controller   │  │  Controller  │  │              │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Creates/Updates/Deletes
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Physical Kubernetes Cluster                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Namespace: kcp-abc123xyz456                               │  │
│  │  (Hashed from upstream workspace info)                    │  │
│  │                                                            │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐         │  │
│  │  │ Deployment │  │   Service  │  │ ConfigMap  │         │  │
│  │  │ my-app     │  │   my-svc   │  │  app-cfg   │         │  │
│  │  │            │  │            │  │            │         │  │
│  │  │ spec:      │  │ spec:      │  │ data:      │         │  │
│  │  │   replicas │  │   ports    │  │   key=val  │         │  │
│  │  │            │  │            │  │            │         │  │
│  │  │ labels:    │  │ labels:    │  │ labels:    │         │  │
│  │  │   internal.│  │   internal.│  │   internal.│         │  │
│  │  │   ...=key  │  │   ...=key  │  │   ...=key  │         │  │
│  │  │            │  │            │  │            │         │  │
│  │  │ status:    │  │ status:    │  │            │         │  │
│  │  │   ready: 1 │  │   loadBal  │  │            │         │  │
│  │  └────────────┘  └────────────┘  └────────────┘         │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### File Structure

**Main Entry Point**: `pkg/syncer/syncer.go:84`

```
pkg/syncer/
├── syncer.go                    # Main syncer setup and initialization
├── spec/
│   ├── spec_controller.go       # Spec syncer controller (KCP → Cluster)
│   ├── spec_process.go          # Spec sync processing logic
│   └── mutators/                # Resource transformations
│       ├── deployment_mutator.go
│       └── secret_mutator.go
├── status/
│   ├── status_controller.go     # Status syncer controller (Cluster → KCP)
│   └── status_process.go        # Status sync processing logic
├── upsync/
│   └── upsync_controller.go     # Upsync controller (Cluster → KCP)
├── namespace/
│   └── namespace_controller.go  # Namespace lifecycle management
├── resourcesync/
│   └── resourcesync.go          # Core sync state machine
├── endpoints/
│   └── endpoints_controller.go  # Service endpoint aggregation
├── controllermanager/
│   └── manager.go               # Dynamic controller management
└── shared/
    └── shared.go                # Shared utilities and constants
```

---

## Syncer Components

### 1. Main Syncer Setup (`pkg/syncer/syncer.go:84`)

**Function**: `StartSyncer()`

**Responsibilities:**
1. Initialize KCP and physical cluster clients
2. Retrieve SyncTarget virtual workspace URLs
3. Set up informer factories for upstream and downstream
4. Start all sub-controllers
5. Begin heartbeat

**Initialization Flow:**

```go
func StartSyncer(ctx context.Context, cfg *SyncerConfig, ...) error {
    // 1. Create KCP client
    kcpSyncTargetClient := kcpBootstrapClusterClient.Cluster(cfg.SyncTargetPath)

    // 2. Wait for SyncTarget to have virtual workspace URLs
    err = wait.PollImmediateInfinite(5*time.Second, func() (bool, error) {
        syncTarget, err := kcpSyncTargetClient.WorkloadV1alpha1().SyncTargets().Get(...)
        if len(syncTarget.Status.VirtualWorkspaces) == 0 {
            return false, nil // Keep waiting
        }
        syncerVirtualWorkspaceURL = syncTarget.Status.VirtualWorkspaces[0].SyncerURL
        upsyncerVirtualWorkspaceURL = syncTarget.Status.VirtualWorkspaces[0].UpsyncerURL
        return true, nil
    })

    // 3. Create upstream clients pointing to virtual workspaces
    upstreamConfig := rest.CopyConfig(cfg.UpstreamConfig)
    upstreamConfig.Host = syncerVirtualWorkspaceURL
    upstreamSyncerClusterClient, _ := kcpdynamic.NewForConfig(upstreamConfig)

    upstreamUpsyncConfig := rest.CopyConfig(cfg.UpstreamConfig)
    upstreamUpsyncConfig.Host = upsyncerVirtualWorkspaceURL
    upstreamUpsyncerClusterClient, _ := kcpdynamic.NewForConfig(upstreamUpsyncConfig)

    // 4. Create downstream client
    downstreamDynamicClient, _ := dynamic.NewForConfig(cfg.DownstreamConfig)

    // 5. Create GVR source (discovers what resources to sync)
    syncTargetGVRSource, _ := resourcesync.NewSyncTargetGVRSource(...)

    // 6. Create informer factories
    // - ddsifForUpstreamSyncer: watches KCP resources to sync down
    // - ddsifForUpstreamUpsyncer: watches KCP resources in upsync mode
    // - ddsifForDownstream: watches physical cluster resources

    // 7. Create controllers
    specSyncer, _ := spec.NewSpecSyncer(...)       // KCP → Cluster
    statusSyncer, _ := status.NewStatusSyncer(...) // Cluster → KCP
    upSyncer, _ := upsync.NewUpSyncer(...)         // Cluster → KCP (upsync)
    downstreamNamespaceController, _ := namespace.NewDownstreamController(...)

    // 8. Start everything
    go apiImporter.Start(...)
    go syncTargetGVRSource.Start(...)
    go specSyncer.Start(...)
    go statusSyncer.Start(...)
    go upSyncer.Start(...)
    go downstreamNamespaceController.Start(...)

    // 9. Start heartbeat
    StartHeartbeat(ctx, kcpSyncTargetClient, cfg.SyncTargetName, cfg.SyncTargetUID)
}
```

**Key Configuration:**

```go
type SyncerConfig struct {
    UpstreamConfig                *rest.Config      // KCP connection
    DownstreamConfig              *rest.Config      // Physical cluster connection
    ResourcesToSync               sets.String       // GVRs to sync
    SyncTargetPath                logicalcluster.Path
    SyncTargetName                string
    SyncTargetUID                 string
    DownstreamNamespaceCleanDelay time.Duration
    DNSImage                      string
}
```

### 2. Spec Syncer (`pkg/syncer/spec/spec_controller.go`)

**Purpose**: Synchronize resource specifications from KCP down to the physical cluster.

**Watch Pattern:**

```go
// Watches UPSTREAM (KCP) resources
ddsifForUpstreamSyncer.AddEventHandler(
    ddsif.GVREventHandlerFuncs{
        AddFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
            // Resource created in KCP → create in cluster
            c.AddToQueue(gvr, obj, logger)
        },
        UpdateFunc: func(gvr schema.GroupVersionResource, oldObj, newObj interface{}) {
            // Resource updated in KCP → update in cluster
            if !deepEqualApartFromStatus(logger, oldUnstrob, newUnstrob) {
                c.AddToQueue(gvr, newObj, logger)
            }
        },
        DeleteFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
            // Resource deleted in KCP → delete in cluster
            c.AddToQueue(gvr, obj, logger)
        },
    },
)

// Watches DOWNSTREAM (Physical Cluster) for deletions
ddsifForDownstream.AddEventHandler(
    ddsif.GVREventHandlerFuncs{
        DeleteFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
            // Resource deleted in cluster → remove finalizer in KCP
            // Extract namespace locator to find upstream resource
            c.AddToQueue(gvr, obj, logger)
        },
    },
)
```

**Processing Logic** (`pkg/syncer/spec/spec_process.go:110`):

```go
func (c *Controller) process(ctx context.Context, gvr schema.GroupVersionResource, key string) error {
    // 1. Parse the key
    clusterName, upstreamNamespace, name, _ := kcpcache.SplitMetaClusterNamespaceKey(key)

    // 2. Calculate downstream namespace (hash of workspace info)
    desiredNSLocator := shared.NewNamespaceLocator(clusterName, syncTargetClusterName, ...)
    downstreamNamespace, _ := shared.PhysicalClusterNamespaceName(desiredNSLocator)

    // 3. Get upstream resource from KCP
    obj, err := upstreamSyncerLister.ByCluster(clusterName).ByNamespace(upstreamNamespace).Get(name)
    if apierrors.IsNotFound(err) {
        // Deleted upstream → delete downstream
        c.downstreamClient.Resource(gvr).Namespace(downstreamNamespace).Delete(...)
        return nil
    }

    // 4. Ensure downstream namespace exists
    if downstreamNamespace != "" {
        c.ensureDownstreamNamespaceExists(ctx, downstreamNamespace, upstreamObj)
    }

    // 5. Add syncer finalizer to upstream resource
    if added, _ := c.ensureSyncerFinalizer(ctx, gvr, upstreamObj); added {
        return nil // Will trigger new reconcile
    }

    // 6. Apply resource to downstream cluster
    return c.applyToDownstream(ctx, gvr, downstreamNamespace, upstreamObj)
}
```

**Apply to Downstream** (`pkg/syncer/spec/spec_process.go:396`):

```go
func (c *Controller) applyToDownstream(..., upstreamObj *unstructured.Unstructured) error {
    // 1. Check if resource should be deleted
    intendedToBeRemovedFromLocation := upstreamObj.GetAnnotations()[
        workloadv1alpha1.InternalClusterDeletionTimestampAnnotationPrefix+c.syncTargetKey
    ] != ""

    if intendedToBeRemovedFromLocation && !stillOwnedByExternalActorForLocation {
        // Delete from downstream
        c.downstreamClient.Resource(gvr).Namespace(downstreamNamespace).Delete(...)
        // Remove finalizer from upstream
        return shared.EnsureUpstreamFinalizerRemoved(...)
    }

    // 2. Transform the resource
    downstreamObj := upstreamObj.DeepCopy()

    // Run resource-specific mutations
    if mutator, ok := c.mutators[gvr]; ok {
        mutator(downstreamObj)
    }

    // 3. Clean up metadata
    downstreamObj.SetName(transformedName)
    downstreamObj.SetUID("")
    downstreamObj.SetResourceVersion("")
    downstreamObj.SetNamespace(downstreamNamespace)
    downstreamObj.SetManagedFields(nil)
    downstreamObj.SetDeletionTimestamp(nil)
    downstreamObj.SetOwnerReferences(nil)
    downstreamObj.SetFinalizers(nil)

    // Strip cluster name annotation
    annotations := downstreamObj.GetAnnotations()
    delete(annotations, logicalcluster.AnnotationKey)

    // For cluster-scoped resources, add namespace locator
    if downstreamNamespace == "" {
        namespaceLocator := shared.NewNamespaceLocator(...)
        annotations[shared.NamespaceLocatorAnnotation] = string(namespaceLocatorJSON)
    }
    downstreamObj.SetAnnotations(annotations)

    // 4. Replace labels
    labels := downstreamObj.GetLabels()
    delete(labels, workloadv1alpha1.ClusterResourceStateLabelPrefix+c.syncTargetKey)
    labels[workloadv1alpha1.InternalDownstreamClusterLabel] = c.syncTargetKey
    downstreamObj.SetLabels(labels)

    // 5. Apply using Server-Side Apply (SSA)
    data, _ := json.Marshal(downstreamObj)
    _, err = c.downstreamClient.Resource(gvr).Namespace(downstreamNamespace).Patch(
        ctx,
        downstreamObj.GetName(),
        types.ApplyPatchType,
        data,
        metav1.PatchOptions{FieldManager: "syncer", Force: true},
    )

    return err
}
```

**Resource Mutations:**

```go
// Example: Deployment Mutator
type DeploymentMutator struct {}

func (m *DeploymentMutator) Mutate(obj *unstructured.Unstructured) error {
    // Inject DNS configuration
    // Inject service account tokens
    // Transform volume mounts
    // etc.
}
```

### 3. Status Syncer (`pkg/syncer/status/status_controller.go`)

**Purpose**: Synchronize resource status from physical cluster back to KCP.

**Watch Pattern:**

```go
// Watches DOWNSTREAM (Physical Cluster) resources
ddsifForDownstream.AddEventHandler(
    ddsif.GVREventHandlerFuncs{
        AddFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
            // New resource in cluster → sync status to KCP
            c.AddToQueue(gvr, obj, logger)
        },
        UpdateFunc: func(gvr schema.GroupVersionResource, oldObj, newObj interface{}) {
            // Resource status changed → sync to KCP
            if !deepEqualFinalizersAndStatus(oldUnstrob, newUnstrob) {
                c.AddToQueue(gvr, newObj, logger)
            }
        },
        DeleteFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
            // Resource deleted in cluster → remove finalizer in KCP
            c.AddToQueue(gvr, obj, logger)
        },
    },
)
```

**Processing Logic** (`pkg/syncer/status/status_process.go:54`):

```go
func (c *Controller) process(ctx context.Context, gvr schema.GroupVersionResource, key string) error {
    // 1. Parse downstream key
    downstreamNamespace, downstreamName, _ := cache.SplitMetaNamespaceKey(key)

    // 2. Get namespace locator (tells us which upstream workspace)
    if downstreamNamespace != "" {
        nsObj, _ := downstreamNamespaceLister.Get(downstreamNamespace)
        namespaceLocator, _ := shared.LocatorFromAnnotations(nsObj.GetAnnotations())
    } else {
        // For cluster-scoped resources, locator is on resource itself
        obj, _ := downstreamLister.Get(downstreamName)
        namespaceLocator, _ := shared.LocatorFromAnnotations(obj.GetAnnotations())
    }

    // 3. Check if resource exists
    obj, err := downstreamLister.ByNamespace(downstreamNamespace).Get(downstreamName)
    if apierrors.IsNotFound(err) {
        // Resource gone → remove finalizer from upstream
        return shared.EnsureUpstreamFinalizerRemoved(...)
    }

    // 4. Extract upstream coordinates from namespace locator
    upstreamNamespace := namespaceLocator.Namespace
    upstreamClusterName := namespaceLocator.ClusterName
    upstreamName := shared.GetUpstreamResourceName(gvr, downstreamName)

    // 5. Update status in upstream
    return c.updateStatusInUpstream(ctx, gvr, upstreamLister, upstreamNamespace,
                                    upstreamName, upstreamClusterName, downstreamObj)
}
```

**Update Status in Upstream** (`pkg/syncer/status/status_process.go:175`):

```go
func (c *Controller) updateStatusInUpstream(..., downstreamObj *unstructured.Unstructured) error {
    // 1. Extract status from downstream resource
    downstreamStatus, statusExists, _ := unstructured.NestedFieldCopy(
        downstreamObj.UnstructuredContent(), "status"
    )
    if !statusExists {
        return nil // Nothing to sync
    }

    // 2. Get existing upstream resource
    existingObj, _ := upstreamLister.ByCluster(upstreamClusterName).
                                     ByNamespace(upstreamNamespace).
                                     Get(upstreamName)

    newUpstream := existing.DeepCopy()

    // 3. Advanced scheduling mode: store status in annotation
    if c.advancedSchedulingEnabled {
        statusAnnotationValue, _ := json.Marshal(downstreamStatus)
        newUpstreamAnnotations := newUpstream.GetAnnotations()
        newUpstreamAnnotations[workloadv1alpha1.InternalClusterStatusAnnotationPrefix+c.syncTargetKey] =
            string(statusAnnotationValue)
        newUpstream.SetAnnotations(newUpstreamAnnotations)

        _, err = c.upstreamClient.Cluster(upstreamClusterName.Path()).
                                  Resource(gvr).
                                  Namespace(upstreamNamespace).
                                  Update(ctx, newUpstream, metav1.UpdateOptions{})
    } else {
        // 4. Normal mode: update status subresource
        unstructured.SetNestedField(newUpstream.UnstructuredContent(), downstreamStatus, "status")

        _, err = c.upstreamClient.Cluster(upstreamClusterName.Path()).
                                  Resource(gvr).
                                  Namespace(upstreamNamespace).
                                  UpdateStatus(ctx, newUpstream, metav1.UpdateOptions{})
    }

    return err
}
```

### 4. Upsyncer (`pkg/syncer/upsync/upsync_controller.go`)

**Purpose**: Allow resources created in the physical cluster to sync back to KCP.

**Use Cases:**
- PersistentVolumes provisioned by storage controllers
- Pods created by operators
- Endpoints managed by service controllers

**Watch Pattern:**

```go
// Watch DOWNSTREAM resources with Upsync label
ddsifForDownstream.AddEventHandler(ddsif.GVREventHandlerFuncs{
    AddFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        if obj.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+syncTargetKey] ==
           string(workloadv1alpha1.ResourceStateUpsync) {
            c.enqueueDownstream(gvr, obj, logger, obj["status"] != nil)
        }
    },
    UpdateFunc: func(gvr schema.GroupVersionResource, oldObj, newObj interface{}) {
        if newObj.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+syncTargetKey] ==
           string(workloadv1alpha1.ResourceStateUpsync) {
            c.enqueueDownstream(gvr, newObj, logger, statusChanged)
        }
    },
    DeleteFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        if obj.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+syncTargetKey] ==
           string(workloadv1alpha1.ResourceStateUpsync) {
            c.enqueueDownstream(gvr, obj, logger, false)
        }
    },
})

// Watch UPSTREAM resources to detect orphans
ddsifForUpstreamUpsyncer.AddEventHandler(ddsif.GVREventHandlerFuncs{
    AddFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        // Upstream resource exists but downstream was deleted
        if obj.GetAnnotations()[ResourceVersionAnnotation] != "" {
            c.enqueueUpstream(gvr, obj, logger, false)
        }
    },
})
```

**Reconcile Logic:**

```go
func (c *controller) reconcile(..., upstreamResource *unstructured.Unstructured, ...) (bool, error) {
    // Get downstream resource
    downstreamObj, _ := c.getDownstreamLister(gvr).ByNamespace(downstreamNamespace).Get(name)

    if downstreamObj == nil && upstreamResource != nil {
        // Downstream deleted but upstream still exists → delete upstream
        return c.deleteUpstream(...)
    }

    if downstreamObj != nil && upstreamResource == nil {
        // Downstream exists but no upstream → create upstream
        return c.createUpstream(...)
    }

    if downstreamObj != nil && upstreamResource != nil {
        // Both exist → update upstream from downstream
        return c.updateUpstream(...)
    }

    return false, nil
}
```

### 5. Namespace Controller (`pkg/syncer/namespace/`)

**Purpose**: Manage downstream namespace lifecycle.

**Responsibilities:**
1. Create namespaces for each unique upstream workspace+namespace combination
2. Add namespace locator annotations
3. Clean up empty namespaces after resources are deleted

**Namespace Naming:**

```go
// Hash-based naming to avoid collisions
func PhysicalClusterNamespaceName(locator *NamespaceLocator) (string, error) {
    // Hash of: cluster name + sync target UID + sync target name + namespace
    hash := sha256.Sum256([]byte(locator.String()))
    return "kcp-" + hex.EncodeToString(hash[:])[:12], nil
}
```

### 6. Heartbeat (`pkg/syncer/syncer.go:417`)

**Purpose**: Maintain connection health between syncer and KCP.

**Implementation:**

```go
func StartHeartbeat(ctx context.Context, kcpSyncTargetClient kcpclientset.Interface,
                    syncTargetName, syncTargetUID string) {
    go wait.UntilWithContext(ctx, func(ctx context.Context) {
        // Attempt to heartbeat every second until successful
        _ = wait.PollImmediateInfiniteWithContext(ctx, 1*time.Second, func(ctx context.Context) (bool, error) {
            // Use JSON Patch to update heartbeat atomically
            patchBytes := []byte(fmt.Sprintf(
                `[{"op":"test","path":"/metadata/uid","value":%q},`+
                `{"op":"replace","path":"/status/lastSyncerHeartbeatTime","value":%q}]`,
                syncTargetUID, time.Now().Format(time.RFC3339),
            ))

            syncTarget, err := kcpSyncTargetClient.WorkloadV1alpha1().SyncTargets().
                Patch(ctx, syncTargetName, types.JSONPatchType, patchBytes,
                      metav1.PatchOptions{}, "status")

            if err != nil {
                logger.Error(err, "failed to set status.lastSyncerHeartbeatTime")
                return false, nil // Retry
            }

            return true, nil
        })
    }, 20*time.Second) // Heartbeat every 20 seconds
}
```

**SyncTarget Heartbeat Controller** (KCP side):

Watches `SyncTarget.Status.LastSyncerHeartbeatTime` and marks the target as unavailable if heartbeat is missed.

---

## Syncer Request Flow

### Complete Synchronization Flow

#### Flow 1: Create Deployment in KCP

```
User creates Deployment in KCP workspace
         ↓
┌────────────────────────────────────────────────────────────────┐
│ KCP API Server                                                  │
│  1. Receives: POST /apis/apps/v1/deployments                   │
│  2. Validates & Persists to etcd                               │
│  3. Path: /deployments/root:org:ws/default/nginx               │
├────────────────────────────────────────────────────────────────┤
│ Placement Controller                                            │
│  4. Watches new Deployment                                     │
│  5. Evaluates placement (namespace has Placement resource)     │
│  6. Selects SyncTarget: "physical-cluster-1"                   │
│  7. Adds label:                                                │
│     state.workload.kcp.io/physical-cluster-1=Sync              │
├────────────────────────────────────────────────────────────────┤
│ Virtual Workspace (Syncer View)                                │
│  8. Filters resources with:                                    │
│     - state.workload.kcp.io/physical-cluster-1=Sync            │
│  9. Exposes to syncer at:                                      │
│     https://kcp.io/syncer/physical-cluster-1                   │
└────────────────────────────────────────────────────────────────┘
         ↓ Syncer watches this virtual workspace
┌────────────────────────────────────────────────────────────────┐
│ Syncer - Spec Controller                                       │
│  10. Informer detects new Deployment                           │
│  11. Adds to work queue                                        │
│  12. Reconcile:                                                │
│      a. Parse key: root:org:ws/default/nginx                   │
│      b. Calculate downstream namespace:                        │
│         hash(root:org:ws + target-uid + default)               │
│         = kcp-abc123xyz456                                     │
│      c. Ensure namespace exists in physical cluster            │
│      d. Add syncer finalizer to upstream Deployment            │
│      e. Transform Deployment:                                  │
│         - Remove KCP labels                                    │
│         - Add downstream label: internal...cluster=target-key  │
│         - Remove finalizers                                    │
│         - Remove owner references                              │
│         - Clear UID, resourceVersion                           │
│         - Run mutations (DNS injection, etc.)                  │
│      f. Apply to physical cluster using SSA                    │
└────────────────────────────────────────────────────────────────┘
         ↓ Creates in physical cluster
┌────────────────────────────────────────────────────────────────┐
│ Physical Kubernetes Cluster                                    │
│  13. Receives: PATCH /apis/apps/v1/namespaces/kcp-abc.../      │
│                deployments/nginx (Server-Side Apply)           │
│  14. Creates Deployment                                        │
│  15. Deployment Controller creates ReplicaSet                  │
│  16. ReplicaSet Controller creates Pods                        │
│  17. Pods scheduled and run                                    │
│  18. Deployment status updated:                                │
│      - availableReplicas: 1                                    │
│      - readyReplicas: 1                                        │
│      - conditions: [Available=True]                            │
└────────────────────────────────────────────────────────────────┘
         ↓ Status syncer watches downstream
┌────────────────────────────────────────────────────────────────┐
│ Syncer - Status Controller                                     │
│  19. Informer detects status change                            │
│  20. Adds to work queue                                        │
│  21. Reconcile:                                                │
│      a. Read downstream Deployment status                      │
│      b. Get namespace locator annotation from namespace        │
│      c. Determine upstream coordinates:                        │
│         - cluster: root:org:ws                                 │
│         - namespace: default                                   │
│         - name: nginx                                          │
│      d. Extract status from downstream                         │
│      e. Update upstream Deployment status subresource          │
└────────────────────────────────────────────────────────────────┘
         ↓ Updates KCP
┌────────────────────────────────────────────────────────────────┐
│ KCP API Server                                                  │
│  22. Receives: PUT /apis/apps/v1/deployments/nginx/status      │
│  23. Updates status in etcd                                    │
│  24. User sees: kubectl get deployment nginx                   │
│      - READY: 1/1                                              │
│      - AVAILABLE: 1                                            │
└────────────────────────────────────────────────────────────────┘
```

#### Flow 2: Delete Deployment from KCP

```
User deletes Deployment in KCP workspace
         ↓
┌────────────────────────────────────────────────────────────────┐
│ KCP API Server                                                  │
│  1. Receives: DELETE /apis/apps/v1/deployments/nginx           │
│  2. Checks finalizers:                                         │
│     - workload.kcp.io/syncer-physical-cluster-1                │
│  3. Sets deletionTimestamp instead of deleting                 │
│  4. Resource still exists with deletionTimestamp set           │
├────────────────────────────────────────────────────────────────┤
│ Placement Controller                                            │
│  5. Detects deletionTimestamp                                  │
│  6. Updates annotation:                                        │
│     deletion.internal.workload.kcp.io/physical-cluster-1=      │
│     <timestamp>                                                │
└────────────────────────────────────────────────────────────────┘
         ↓ Syncer sees annotation change
┌────────────────────────────────────────────────────────────────┐
│ Syncer - Spec Controller                                       │
│  7. Informer detects update (annotation change)                │
│  8. Reconcile detects:                                         │
│     intendedToBeRemovedFromLocation = true                     │
│     (deletion annotation exists)                               │
│  9. Deletes from physical cluster:                             │
│     DELETE /apis/apps/v1/namespaces/kcp-abc.../deployments/nginx│
└────────────────────────────────────────────────────────────────┘
         ↓ Deletes in physical cluster
┌────────────────────────────────────────────────────────────────┐
│ Physical Kubernetes Cluster                                    │
│  10. Deployment deleted                                        │
│  11. ReplicaSets deleted (owner reference)                     │
│  12. Pods deleted (owner reference)                            │
│  13. Deployment removed from storage                           │
└────────────────────────────────────────────────────────────────┘
         ↓ Status syncer detects deletion OR spec syncer continues
┌────────────────────────────────────────────────────────────────┐
│ Syncer - Spec Controller (continued)                           │
│  14. After successful deletion from downstream                 │
│  15. Calls EnsureUpstreamFinalizerRemoved()                    │
│  16. Gets upstream Deployment                                  │
│  17. Removes finalizer:                                        │
│      workload.kcp.io/syncer-physical-cluster-1                 │
│  18. Updates upstream Deployment                               │
└────────────────────────────────────────────────────────────────┘
         ↓ Finalizer removed
┌────────────────────────────────────────────────────────────────┐
│ KCP API Server                                                  │
│  19. Detects finalizers list is now empty                      │
│  20. Permanently deletes Deployment from etcd                  │
│  21. Resource fully removed                                    │
└────────────────────────────────────────────────────────────────┘
```

---

## State Machine & Labels

### State Label System

The syncer uses labels to coordinate state across KCP and physical clusters without requiring direct communication.

**Label Format:**

```yaml
labels:
  state.workload.kcp.io/<sync-target-key>: <state>
  internal.workload.kcp.io/cluster: <sync-target-key>
```

**States:**

| State | Location | Meaning | Who Sets |
|-------|----------|---------|----------|
| `Sync` | KCP (upstream) | Resource should sync from KCP → Cluster | Placement Controller |
| `Upsync` | KCP (upstream) | Resource should sync from Cluster → KCP | User/Operator |
| `Pending` | KCP (upstream) | Awaiting placement decision | Placement Controller |
| (none) | Physical cluster | Internal cluster label only | Spec Syncer |

**Additional Annotations:**

```yaml
annotations:
  # Deletion coordination
  deletion.internal.workload.kcp.io/<sync-target-key>: "<timestamp>"

  # Finalizer coordination
  finalizers.workload.kcp.io/<sync-target-key>: ""

  # Advanced scheduling - spec diff
  diff.spec.internal.workload.kcp.io/<sync-target-key>: "<json-patch>"

  # Advanced scheduling - status storage
  status.internal.workload.kcp.io/<sync-target-key>: "<json-status>"

  # Namespace locator (for finding upstream resource)
  kcp.io/namespace-locator: '{"cluster":"root:org:ws","syncTarget":{"uid":"..."},...}'
```

**Finalizers:**

```yaml
finalizers:
  - workload.kcp.io/syncer-<sync-target-key>
```

### State Transitions

```
┌─────────────┐
│   Created   │ User creates Deployment in KCP
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Pending   │ state.workload.kcp.io/<target>="" (or missing)
└──────┬──────┘ Waiting for placement decision
       │
       │ Placement Controller evaluates
       │
       ▼
┌─────────────┐
│    Sync     │ state.workload.kcp.io/<target>=Sync
└──────┬──────┘ Label added by Placement Controller
       │
       │ Spec Syncer watches via Virtual Workspace
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Syncing to Physical Cluster                   │
│  1. Spec Syncer creates resource downstream      │
│  2. Adds finalizer to upstream                   │
│  3. Namespace Controller ensures namespace       │
│  4. Resource created in physical cluster         │
└──────┬───────────────────────────────────────────┘
       │
       │ Physical cluster operates on resource
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Active / Running                              │
│  - Physical cluster manages workload             │
│  - Status Syncer copies status back to KCP       │
│  - Spec changes in KCP propagate down            │
│  - Status changes in cluster propagate up        │
└──────┬───────────────────────────────────────────┘
       │
       │ User deletes in KCP OR eviction triggered
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Deletion Initiated                            │
│  1. deletionTimestamp set on upstream            │
│  2. Placement Controller adds annotation:        │
│     deletion.internal.../<target>=<timestamp>    │
└──────┬───────────────────────────────────────────┘
       │
       │ Spec Syncer detects deletion annotation
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Deleting from Physical Cluster                │
│  1. Spec Syncer deletes downstream resource      │
│  2. Physical cluster cascades deletion           │
│  3. Spec Syncer removes finalizer from upstream  │
└──────┬───────────────────────────────────────────┘
       │
       │ KCP sees finalizer removed
       │
       ▼
┌─────────────┐
│   Deleted   │ Resource permanently removed from etcd
└─────────────┘
```

### Upsync State Machine

```
┌──────────────────────────────────────────────────┐
│    Resource Created in Physical Cluster          │
│  - By operator, controller, or admin             │
│  - Examples: PersistentVolume, Pod               │
└──────┬───────────────────────────────────────────┘
       │
       │ Operator/controller adds label
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Upsync Label Added                            │
│  labels:                                         │
│    state.workload.kcp.io/<target>=Upsync         │
│    internal.workload.kcp.io/cluster=<target-key> │
│  annotations:                                    │
│    kcp.io/namespace-locator=<json>               │
└──────┬───────────────────────────────────────────┘
       │
       │ Upsyncer watches downstream resources
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Upsyncing to KCP                              │
│  1. Upsyncer creates resource in KCP via         │
│     Upsyncer Virtual Workspace                   │
│  2. Adds annotation with downstream RV           │
│  3. Resource appears in KCP workspace            │
└──────┬───────────────────────────────────────────┘
       │
       │ Resource exists in both places
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Active Upsync Mode                            │
│  - Changes in cluster → propagate to KCP         │
│  - Status Syncer skips (in Upsync mode)          │
│  - Spec changes in KCP do NOT propagate down     │
└──────┬───────────────────────────────────────────┘
       │
       │ Resource deleted downstream
       │
       ▼
┌──────────────────────────────────────────────────┐
│    Cleanup in KCP                                │
│  1. Upsyncer detects deletion                    │
│  2. Deletes resource from KCP                    │
└──────────────────────────────────────────────────┘
```

---

## Namespace Mapping

### Problem

KCP has logical clusters with namespaces: `root:org:workspace / default`
Physical cluster needs unique namespaces but can't have slashes or very long names.

### Solution: Namespace Locator

**Namespace Locator Structure:**

```go
type NamespaceLocator struct {
    ClusterName   logicalcluster.Name // e.g., "root:org:workspace"
    SyncTarget    SyncTargetLocator   // UID + cluster name of SyncTarget
    Namespace     string              // e.g., "default"
}

type SyncTargetLocator struct {
    ClusterName string  // Where SyncTarget resource lives
    Name        string  // SyncTarget name
    UID         string  // SyncTarget UID
}
```

**Downstream Namespace Naming:**

```go
// Create unique hash-based name
func PhysicalClusterNamespaceName(locator *NamespaceLocator) (string, error) {
    // Serialize locator to canonical string
    locatorString := fmt.Sprintf("%s/%s/%s/%s/%s",
        locator.ClusterName,
        locator.SyncTarget.ClusterName,
        locator.SyncTarget.Name,
        locator.SyncTarget.UID,
        locator.Namespace,
    )

    // Hash to create deterministic name
    hash := sha256.Sum256([]byte(locatorString))
    shortHash := hex.EncodeToString(hash[:])[:12]

    return "kcp-" + shortHash, nil
}
```

**Example:**

```
Upstream:
  Cluster: root:org:my-workspace
  Namespace: default
  SyncTarget: physical-cluster-1 (UID: abc-123)

Downstream:
  Namespace: kcp-7a3f8c2d91e5
  (Hash of "root:org:my-workspace/root:org/physical-cluster-1/abc-123/default")
```

**Namespace Annotations:**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: kcp-7a3f8c2d91e5
  annotations:
    kcp.io/namespace-locator: |
      {
        "clusterName": "root:org:my-workspace",
        "syncTarget": {
          "clusterName": "root:org",
          "name": "physical-cluster-1",
          "uid": "abc-123"
        },
        "namespace": "default"
      }
  labels:
    internal.workload.kcp.io/cluster: root_org_my-workspace--physical-cluster-1
    kcp.io/tenant-id: <hash>  # For network policy isolation
```

**Finding Upstream Resource from Downstream:**

```go
// Status syncer needs to find which KCP resource to update
func FindUpstreamResource(downstreamObj *unstructured.Unstructured) {
    // For namespaced resources
    downstreamNamespace := downstreamObj.GetNamespace()
    namespaceObj := namespaceLister.Get(downstreamNamespace)
    locator := LocatorFromAnnotations(namespaceObj.GetAnnotations())

    // Upstream coordinates
    upstreamCluster := locator.ClusterName       // root:org:my-workspace
    upstreamNamespace := locator.Namespace       // default
    upstreamName := downstreamObj.GetName()      // my-app

    // For cluster-scoped resources
    locator := LocatorFromAnnotations(downstreamObj.GetAnnotations())
    // Same extraction logic
}
```

---

## Logical Cluster Deletion

### Overview

When a LogicalCluster is deleted in KCP, all resources within it must be cleaned up before the cluster itself can be removed. This is handled by the **LogicalCluster Deletion Controller**.

**File**: `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go`

### Components

1. **Deletion Controller**: Orchestrates the deletion process
2. **Resource Deleter**: Deletes all resources in the cluster
3. **Owner Cleanup**: Removes finalizers from owner resources

### Deletion Flow

```
┌──────────────────────────────────────────────────────────────┐
│ User Deletes LogicalCluster                                  │
│  kubectl delete logicalcluster my-cluster                    │
└──────┬───────────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ KCP API Server                                               │
│  1. Receives DELETE request                                  │
│  2. Checks finalizers:                                       │
│     - core.kcp.io/logicalcluster-deletion                    │
│  3. Sets deletionTimestamp (doesn't delete yet)              │
│  4. Resource stays in etcd with deletionTimestamp set        │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ LogicalCluster Deletion Controller watches
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ LogicalCluster Deletion Controller                           │
│  Filters for resources with deletionTimestamp != nil         │
│                                                               │
│  File: logicalcluster_deletion_controller.go:86              │
│                                                               │
│  Informer Filter:                                            │
│    FilterFunc: func(obj) bool {                              │
│      return !obj.DeletionTimestamp.IsZero()                  │
│    }                                                          │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ Enqueues LogicalCluster for processing
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ Controller.process()                                         │
│  File: logicalcluster_deletion_controller.go:214             │
│                                                               │
│  1. Get LogicalCluster from lister                           │
│  2. Check if deletionTimestamp is set                        │
│  3. Call deleter.Delete(logicalCluster)                      │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ Delegates to resource deleter
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ WorkspaceResourcesDeleter.Delete()                           │
│  File: deletion/logicalcluster_resource_deletor.go:103       │
│                                                               │
│  1. Check if LogicalCluster has finalizers                   │
│     - If empty, return (nothing to do)                       │
│                                                               │
│  2. Call deleteAllContent(logicalCluster)                    │
│     - Returns: estimate, message, error                      │
│                                                               │
│  3. If estimate > 0:                                         │
│     - Return ResourcesRemainingError                         │
│     - Controller will requeue after estimate/2 + 1 seconds   │
│                                                               │
│  4. If estimate == 0:                                        │
│     - All resources deleted                                  │
│     - Return nil                                             │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ Delete all content
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ deleteAllContent()                                           │
│  File: deletion/logicalcluster_resource_deletor.go:340       │
│                                                               │
│  PHASE 1: Discovery                                          │
│  1. Discover all API resources in the logical cluster        │
│     resources, err := discoverResourcesFn(clusterPath)       │
│                                                               │
│  2. Filter deletable resources:                              │
│     - Must support "delete" verb                             │
│     - Exclude: logicalclusters (trigger)                     │
│     - Exclude: clusterroles, clusterrolebindings (keep for debug)│
│     - Exclude: virtual/projected resources                   │
│     - Exclude: namespace-scoped (deleted with namespace)     │
│     - Only cluster-scoped resources remain                   │
│                                                               │
│  3. Convert to GroupVersionResources map                     │
│     gvrs = {                                                 │
│       {Group: "apps", Version: "v1", Resource: "deployments"}: │
│         ["list", "delete", "deletecollection"],             │
│       ...                                                    │
│     }                                                         │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ For each GVR
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ deleteAllContentForGroupVersionResource()                    │
│  File: deletion/logicalcluster_resource_deletor.go:248       │
│                                                               │
│  For GVR (e.g., deployments.apps/v1):                        │
│                                                               │
│  1. Estimate graceful termination time                       │
│     estimate = 5 seconds (default)                           │
│                                                               │
│  2. Try bulk deleteCollection first                          │
│     if "deletecollection" in verbs:                          │
│       DELETE /apis/apps/v1/deployments?namespace=*           │
│       (Background propagation)                               │
│                                                               │
│  3. If deleteCollection not supported:                       │
│     - List all resources                                     │
│     - Delete each individually                               │
│                                                               │
│  4. Verify deletion                                          │
│     - List resources again                                   │
│     - Count remaining items                                  │
│                                                               │
│  5. Analyze remaining resources                              │
│     for each remaining resource:                             │
│       - Check for finalizers                                 │
│       - Track: finalizersToNumRemaining map                  │
│                                                               │
│  6. Return metadata:                                         │
│     {                                                        │
│       finalizerEstimateSeconds: 15,  // if finalizers present│
│       numRemaining: 5,                                       │
│       finalizersToNumRemaining: {                            │
│         "foregroundDeletion": 3,                             │
│         "custom.io/my-finalizer": 2,                         │
│       }                                                      │
│     }                                                         │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ Aggregate results from all GVRs
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ Aggregation & Condition Updates                              │
│                                                               │
│  1. Collect all remaining resources:                         │
│     gvrToNumRemaining: {                                     │
│       deployments.apps: 5,                                   │
│       services.core: 2,                                      │
│     }                                                         │
│                                                               │
│  2. Collect all blocking finalizers:                         │
│     finalizersToNumRemaining: {                              │
│       "foregroundDeletion": 7,                               │
│       "workload.kcp.io/syncer-target1": 3,                   │
│     }                                                         │
│                                                               │
│  3. Update LogicalCluster conditions:                        │
│                                                               │
│     If resources remain:                                     │
│       conditions.MarkFalse(                                  │
│         logicalCluster,                                      │
│         WorkspaceContentDeleted,                             │
│         "SomeResourcesRemain",                               │
│         "Some resources are remaining: deployments.apps      │
│          has 5 instances; Some content has finalizers:       │
│          foregroundDeletion in 7 instances"                  │
│       )                                                      │
│                                                               │
│     If all deleted:                                          │
│       conditions.MarkTrue(logicalCluster, WorkspaceContentDeleted)│
│                                                               │
│  4. Return maximum estimate across all GVRs                  │
│     estimate = max(15, 5, 10, ...) = 15 seconds              │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ If resources remain
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ ResourcesRemainingError Returned                             │
│  File: logicalcluster_deletion_controller.go:198             │
│                                                               │
│  1. Controller catches ResourcesRemainingError               │
│  2. Calculate wait duration:                                 │
│     duration = estimate/2 + 1 second                         │
│  3. Requeue after duration:                                  │
│     c.queue.AddAfter(key, duration)                          │
│  4. Controller logs:                                         │
│     "content remaining in logical cluster after a wait,      │
│      waiting more to continue"                               │
└──────────────────────────────────────────────────────────────┘
       │
       │ ⟲ Retry loop continues until all resources deleted
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ All Resources Deleted                                        │
│  deleteAllContent() returns estimate=0, message="", err=nil  │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ Proceed to finalization
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ Controller.finalizeWorkspace()                               │
│  File: logicalcluster_deletion_controller.go:260             │
│                                                               │
│  1. Find and remove deletion finalizer:                      │
│     for i, finalizer := range ws.Finalizers:                 │
│       if finalizer == "core.kcp.io/logicalcluster-deletion": │
│         ws.Finalizers = remove(ws.Finalizers, i)             │
│                                                               │
│  2. Delete ClusterRoles (manual cleanup):                    │
│     kubeClient.RbacV1().ClusterRoles().                      │
│       DeleteCollection(BackgroundDeletion, ListAll)          │
│                                                               │
│  3. Delete ClusterRoleBindings (manual cleanup):             │
│     kubeClient.RbacV1().ClusterRoleBindings().               │
│       DeleteCollection(BackgroundDeletion, ListAll)          │
│                                                               │
│  4. Handle Owner resource (if exists):                       │
│     if ws.Spec.Owner != nil:                                 │
│       a. Get owner resource (e.g., Workspace)                │
│       b. Remove finalizer from owner:                        │
│          logicalcluster.kcp.io/finalizer                     │
│       c. If DirectlyDeletable:                               │
│          Delete owner resource                               │
│                                                               │
│  5. Update LogicalCluster (removes finalizer):               │
│     kcpClient.CoreV1alpha1().LogicalClusters().              │
│       Update(ctx, ws, UpdateOptions)                         │
└──────┬───────────────────────────────────────────────────────┘
       │
       │ Finalizer removed
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ KCP API Server - Final Deletion                             │
│                                                               │
│  1. Detects finalizers list is empty                         │
│  2. Permanently deletes LogicalCluster from etcd             │
│  3. Resource fully removed from system                       │
│  4. All associated data cleaned up                           │
└──────────────────────────────────────────────────────────────┘
```

---

## Resource Cleanup Process

### Discovery Phase

**Code**: `deletion/logicalcluster_resource_deletor.go:350`

```go
// Discover all API resources in the logical cluster
resources, err := d.discoverResourcesFn(logicalcluster.From(ws).Path())
// Returns: []*metav1.APIResourceList

// Filter to get only deletable resources
deletableResources := discovery.FilteredBy(and{
    // Must support delete verb
    discovery.SupportsAllVerbs{Verbs: []string{"delete"}},

    // Don't delete the LogicalCluster itself
    isNotGroupResource{group: core.GroupName, resource: "logicalclusters"},

    // Keep for debugging
    isNotGroupResource{group: rbac.GroupName, resource: "clusterroles"},
    isNotGroupResource{group: rbac.GroupName, resource: "clusterrolebindings"},

    // Don't delete virtual/projected resources
    isNotVirtualResource{},

    // Skip namespace-scoped (deleted with namespace)
    isNotNamespaceScoped{},
}, resources)
```

**Filter Implementations:**

```go
// isNotGroupResource - exclude specific resources
type isNotGroupResource struct {
    group    string
    resource string
}

func (ngr isNotGroupResource) Match(groupVersion string, r *metav1.APIResource) bool {
    gv, _ := schema.ParseGroupVersion(groupVersion)
    return !(gv.Group == ngr.group && r.Name == ngr.resource)
}

// isNotVirtualResource - exclude projections
type isNotVirtualResource struct{}

func (vr isNotVirtualResource) Match(groupVersion string, r *metav1.APIResource) bool {
    gv, _ := schema.ParseGroupVersion(groupVersion)
    gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: r.Name}
    return !projection.Includes(gvr)
}

// isNotNamespaceScoped - only cluster-scoped
type isNotNamespaceScoped struct{}

func (n isNotNamespaceScoped) Match(groupVersion string, r *metav1.APIResource) bool {
    return !r.Namespaced
}
```

### Deletion Strategies

**1. DeleteCollection (Bulk Delete)**

```go
func (d *logicalClusterResourcesDeleter) deleteCollection(
    ctx context.Context,
    clusterName logicalcluster.Name,
    gvr schema.GroupVersionResource,
    verbs sets.String,
) (bool, error) {
    // Check if deleteCollection is supported
    if !verbs.Has("deletecollection") {
        return false, nil
    }

    // Delete all resources of this type at once
    background := metav1.DeletePropagationBackground
    opts := metav1.DeleteOptions{PropagationPolicy: &background}

    err := d.metadataClusterClient.
        Resource(gvr).
        Cluster(clusterName.Path()).
        DeleteCollection(ctx, opts, metav1.ListOptions{})

    return true, err
}
```

**2. Delete Each Item (Fallback)**

```go
func (d *logicalClusterResourcesDeleter) deleteEachItem(
    ctx context.Context,
    clusterName logicalcluster.Name,
    gvr schema.GroupVersionResource,
    verbs sets.String,
) error {
    // List all resources
    unstructuredList, listSupported, err := d.listCollection(ctx, clusterName, gvr, verbs)
    if !listSupported {
        return nil
    }

    // Delete each individually
    for _, item := range unstructuredList.Items {
        background := metav1.DeletePropagationBackground
        opts := metav1.DeleteOptions{PropagationPolicy: &background}

        err = d.metadataClusterClient.
            Cluster(clusterName.Path()).
            Resource(gvr).
            Namespace(item.GetNamespace()).
            Delete(ctx, item.GetName(), opts)

        if err != nil && !errors.IsNotFound(err) && !errors.IsMethodNotSupported(err) {
            return err
        }
    }

    return nil
}
```

### Finalizer Handling

**Detecting Resources with Finalizers:**

```go
// After deletion attempt, list remaining resources
unstructuredList, listSupported, err := d.listCollection(ctx, clusterName, gvr, verbs)

// Analyze finalizers
finalizersToNumRemaining := map[string]int{}
for _, item := range unstructuredList.Items {
    for _, finalizer := range item.GetFinalizers() {
        finalizersToNumRemaining[finalizer]++
    }
}

// If finalizers exist, return estimate
if len(finalizersToNumRemaining) > 0 {
    return gvrDeletionMetadata{
        finalizerEstimateSeconds: 15,  // Default estimate
        numRemaining:             len(unstructuredList.Items),
        finalizersToNumRemaining: finalizersToNumRemaining,
    }, nil
}
```

**Common Finalizers:**

- `foregroundDeletion`: Standard Kubernetes cascading delete
- `workload.kcp.io/syncer-<target>`: Syncer cleanup
- `kubernetes.io/pv-protection`: PersistentVolume protection
- Custom finalizers from controllers

### Owner Resource Cleanup

**Purpose**: If the LogicalCluster has an owner (e.g., Workspace), clean up the relationship.

```go
if ws.Spec.Owner != nil {
    // Parse owner GVR
    gvr := schema.GroupVersionResource{
        Resource: ws.Spec.Owner.Resource,
    }
    comps := strings.SplitN(ws.Spec.Owner.APIVersion, "/", 2)
    if len(comps) == 2 {
        gvr.Group = comps[0]
        gvr.Version = comps[1]
    } else {
        gvr.Version = comps[0]
    }

    // Get owner resource
    clusterPath := logicalcluster.NewPath(ws.Spec.Owner.Cluster)
    obj, err := c.dynamicFrontProxyClient.
        Cluster(clusterPath).
        Resource(gvr).
        Namespace(ws.Spec.Owner.Namespace).
        Get(ctx, ws.Spec.Owner.Name, metav1.GetOptions{})

    // Check UID matches (owner hasn't changed)
    if obj.GetUID() != ws.Spec.Owner.UID {
        // Owner changed, skip cleanup
        return fmt.Errorf("owner UID mismatch")
    }

    // Remove finalizer from owner
    finalizers := sets.NewString(obj.GetFinalizers()...)
    if finalizers.Has(corev1alpha1.LogicalClusterFinalizer) {
        finalizers.Delete(corev1alpha1.LogicalClusterFinalizer)
        obj.SetFinalizers(finalizers.List())

        _, err = c.dynamicFrontProxyClient.
            Cluster(clusterPath).
            Resource(gvr).
            Namespace(ws.Spec.Owner.Namespace).
            Update(ctx, obj, metav1.UpdateOptions{})
    }

    // Delete owner if DirectlyDeletable
    if obj.GetDeletionTimestamp().IsZero() && ws.Spec.DirectlyDeletable {
        err := c.dynamicFrontProxyClient.
            Cluster(clusterPath).
            Resource(gvr).
            Namespace(ws.Spec.Owner.Namespace).
            Delete(ctx, ws.Spec.Owner.Name, metav1.DeleteOptions{
                Preconditions: &metav1.Preconditions{UID: &ws.Spec.Owner.UID},
            })
    }
}
```

### Error Handling & Retry Logic

**Retry on Resources Remaining:**

```go
// In controller.process()
err := c.process(ctx, key)

var estimate *deletion.ResourcesRemainingError
if errors.As(err, &estimate) {
    // Calculate wait time
    t := estimate.Estimate/2 + 1
    duration := time.Duration(t) * time.Second

    logger.V(2).Error(err,
        "content remaining in logical cluster after a wait, waiting more to continue",
        "duration", time.Since(startTime),
        "waiting", duration,
    )

    // Requeue after calculated duration
    c.queue.AddAfter(key, duration)
} else if err != nil {
    // Other errors - use rate limiting
    c.queue.AddRateLimited(key)
    runtime.HandleError(fmt.Errorf("deletion of logical cluster %v failed: %w", key, err))
}
```

**Graceful Termination Estimation:**

```go
func (d *logicalClusterResourcesDeleter) estimateGracefulTermination(
    ctx context.Context,
    gvr schema.GroupVersionResource,
    clusterName logicalcluster.Name,
    clusterDeletedAt metav1.Time,
) (int64, error) {
    // Default estimate for resources that may need graceful termination
    estimate := int64(5)

    // Could be enhanced to check:
    // - Pod termination grace periods
    // - StatefulSet ordinal deletion
    // - Job completion

    return estimate, nil
}
```

---

## Complete Flow Examples

### Example 1: Full Deployment Lifecycle with Syncer

**Setup:**
- KCP Workspace: `root:org:my-workspace`
- SyncTarget: `physical-cluster-1` (UID: `abc-123-xyz`)
- User creates Deployment: `my-app`

**Step-by-Step:**

```yaml
# Step 1: User creates Deployment in KCP
$ kubectl --context kcp create -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
  namespace: default
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
EOF

# In KCP etcd:
/deployments/root:org:my-workspace/default/my-app
  spec:
    replicas: 3
    ...
  metadata:
    labels: {}
    finalizers: []
```

```
# Step 2: Placement Controller adds label
# (Assumes Placement resource exists for namespace)

In KCP:
  labels:
    state.workload.kcp.io/physical-cluster-1: Sync
```

```
# Step 3: Spec Syncer picks up from Virtual Workspace
# Virtual Workspace filters to only show resources with:
#   state.workload.kcp.io/physical-cluster-1=Sync

Syncer watches: https://kcp.io/syncer/physical-cluster-1

Spec Controller reconcile:
  1. Parse key: root:org:my-workspace/default/my-app
  2. Create namespace locator:
     {
       clusterName: "root:org:my-workspace",
       syncTarget: {
         clusterName: "root:org",
         name: "physical-cluster-1",
         uid: "abc-123-xyz"
       },
       namespace: "default"
     }
  3. Calculate downstream namespace:
     hash("root:org:my-workspace/root:org/physical-cluster-1/abc-123-xyz/default")
     = "kcp-7a3f8c2d91e5"
```

```yaml
# Step 4: Create namespace in physical cluster

In Physical Cluster:
apiVersion: v1
kind: Namespace
metadata:
  name: kcp-7a3f8c2d91e5
  annotations:
    kcp.io/namespace-locator: |
      {"clusterName":"root:org:my-workspace",...}
  labels:
    internal.workload.kcp.io/cluster: root_org_my-workspace--physical-cluster-1
    kcp.io/tenant-id: abc123hash
```

```yaml
# Step 5: Add finalizer to upstream Deployment

In KCP:
  finalizers:
    - workload.kcp.io/syncer-physical-cluster-1
```

```yaml
# Step 6: Transform and apply to physical cluster

In Physical Cluster:
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
  namespace: kcp-7a3f8c2d91e5
  labels:
    internal.workload.kcp.io/cluster: root_org_my-workspace--physical-cluster-1
  # No cluster annotation
  # No finalizers
  # No owner references
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
```

```
# Step 7: Physical cluster creates Pods

Physical Cluster Deployment Controller:
  - Creates ReplicaSet
  - ReplicaSet creates 3 Pods
  - Pods scheduled and started
  - Deployment status updated:
      availableReplicas: 3
      readyReplicas: 3
      conditions:
        - type: Available
          status: "True"
```

```yaml
# Step 8: Status Syncer syncs status back

Status Controller watches downstream:
  1. Detects Deployment status change
  2. Reads namespace: kcp-7a3f8c2d91e5
  3. Gets namespace locator from annotation
  4. Determines upstream:
     - cluster: root:org:my-workspace
     - namespace: default
     - name: my-app
  5. Extracts status from downstream
  6. Updates upstream status subresource

In KCP:
status:
  availableReplicas: 3
  readyReplicas: 3
  replicas: 3
  conditions:
    - type: Available
      status: "True"
      lastUpdateTime: "2025-10-29T10:00:00Z"
```

```
# Step 9: User scales Deployment

$ kubectl --context kcp scale deployment my-app --replicas=5

In KCP:
  spec:
    replicas: 5  # Changed
```

```
# Step 10: Spec Syncer propagates change

Spec Controller detects update (spec changed):
  1. Upstream Deployment changed
  2. Transform and apply to physical cluster
  3. Uses Server-Side Apply (no full replacement)

In Physical Cluster:
  spec:
    replicas: 5  # Updated
```

```
# Step 11: Status updates flow back

Physical Cluster:
  - ReplicaSet scales to 5
  - New Pods created
  - Status: availableReplicas: 5

Status Syncer:
  - Detects status change
  - Updates KCP

In KCP:
  status:
    availableReplicas: 5
    readyReplicas: 5
```

```
# Step 12: User deletes Deployment

$ kubectl --context kcp delete deployment my-app

In KCP:
  - deletionTimestamp set
  - Finalizer present, so not deleted yet
```

```
# Step 13: Placement Controller adds deletion annotation

In KCP:
  annotations:
    deletion.internal.workload.kcp.io/physical-cluster-1: "2025-10-29T10:30:00Z"
```

```
# Step 14: Spec Syncer deletes from physical cluster

Spec Controller detects deletion annotation:
  1. intendedToBeRemovedFromLocation = true
  2. Deletes from physical cluster:
     DELETE /apis/apps/v1/namespaces/kcp-7a3f8c2d91e5/deployments/my-app
  3. Physical cluster cascades deletion:
     - ReplicaSet deleted
     - Pods deleted
  4. Spec Syncer removes finalizer from KCP
```

```
# Step 15: Final cleanup

In KCP:
  - Finalizer removed
  - KCP API Server permanently deletes from etcd
  - Resource fully removed
```

### Example 2: Logical Cluster Deletion

**Setup:**
- LogicalCluster: `root:org:project-x`
- Contains: 10 Deployments, 5 Services, 3 ConfigMaps, 2 Secrets
- Some resources have finalizers

**Detailed Flow:**

```
$ kubectl delete logicalcluster project-x

┌─────────────────────────────────────────────────┐
│ T+0s: Deletion Initiated                        │
├─────────────────────────────────────────────────┤
│ - deletionTimestamp set                         │
│ - Finalizer: core.kcp.io/logicalcluster-deletion│
│ - Controller enqueued                           │
└─────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────┐
│ T+1s: Discovery Phase                           │
├─────────────────────────────────────────────────┤
│ - Discover APIs: 50 resource types              │
│ - Filter: 20 deletable cluster-scoped           │
│ - GVRs: deployments, services, configmaps, etc. │
└─────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────┐
│ T+2s: Deletion Attempt 1                        │
├─────────────────────────────────────────────────┤
│ For deployments.apps/v1:                        │
│   - DeleteCollection attempted                  │
│   - 10 Deployments deleted                      │
│   - List remaining: 10 (still there)            │
│   - Finalizers detected:                        │
│     * workload.kcp.io/syncer-target1: 10        │
│   - Estimate: 15 seconds                        │
│                                                  │
│ For services.core/v1:                           │
│   - DeleteCollection attempted                  │
│   - 5 Services deleted                          │
│   - List remaining: 0 (all gone)                │
│   - Estimate: 0 seconds                         │
│                                                  │
│ Overall:                                        │
│   - Resources remaining: 10                     │
│   - Max estimate: 15 seconds                    │
│   - Condition: WorkspaceContentDeleted=False    │
│   - Message: "Some resources are remaining:     │
│              deployments.apps has 10 instances; │
│              finalizers: syncer-target1 in 10"  │
└─────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────┐
│ T+2s: Requeue                                   │
├─────────────────────────────────────────────────┤
│ - ResourcesRemainingError returned              │
│ - Wait: 15/2 + 1 = 8 seconds                    │
│ - Requeue after 8 seconds                       │
└─────────────────────────────────────────────────┘
         ↓
         ... 8 seconds pass ...
         ... Syncer removes finalizers ...
         ↓
┌─────────────────────────────────────────────────┐
│ T+10s: Deletion Attempt 2                       │
├─────────────────────────────────────────────────┤
│ For deployments.apps/v1:                        │
│   - DeleteCollection attempted                  │
│   - List remaining: 3 (7 cleaned up)            │
│   - Finalizers:                                 │
│     * workload.kcp.io/syncer-target1: 3         │
│   - Estimate: 15 seconds                        │
│                                                  │
│ Overall:                                        │
│   - Resources remaining: 3                      │
│   - Requeue after 8 seconds                     │
└─────────────────────────────────────────────────┘
         ↓
         ... 8 seconds pass ...
         ↓
┌─────────────────────────────────────────────────┐
│ T+18s: Deletion Attempt 3                       │
├─────────────────────────────────────────────────┤
│ For all GVRs:                                   │
│   - List remaining: 0 (all gone!)               │
│   - Estimate: 0 seconds                         │
│                                                  │
│ Condition: WorkspaceContentDeleted=True         │
└─────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────┐
│ T+18s: Finalization                             │
├─────────────────────────────────────────────────┤
│ 1. Delete ClusterRoles:                         │
│    - DELETE /apis/rbac.authorization.k8s.io/v1/ │
│             clusterroles?all                    │
│                                                  │
│ 2. Delete ClusterRoleBindings:                  │
│    - DELETE /apis/rbac.authorization.k8s.io/v1/ │
│             clusterrolebindings?all             │
│                                                  │
│ 3. Handle Owner (Workspace):                    │
│    - Get Workspace resource                     │
│    - Remove finalizer: logicalcluster.kcp.io... │
│    - Delete Workspace (if DirectlyDeletable)    │
│                                                  │
│ 4. Remove LogicalCluster finalizer              │
└─────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────┐
│ T+19s: Permanent Deletion                       │
├─────────────────────────────────────────────────┤
│ - KCP API Server detects finalizers empty       │
│ - Deletes from etcd                             │
│ - LogicalCluster fully removed                  │
│ - All data cleaned up                           │
└─────────────────────────────────────────────────┘
```

**Timeline Summary:**
- T+0s: Deletion requested
- T+1s: Discovery complete
- T+2s: First deletion attempt (10 resources blocked by finalizers)
- T+10s: Second deletion attempt (3 resources still blocked)
- T+18s: Third deletion attempt (all clear)
- T+19s: Finalization and permanent deletion

**Total Time:** ~19 seconds (depends on finalizer cleanup speed)

---

## Summary

### Syncer Summary

The **syncer** is a sophisticated bidirectional synchronization system that:

1. **Watches** KCP resources via virtual workspaces filtered by placement labels
2. **Transforms** resources from KCP's multi-tenant model to physical cluster format
3. **Applies** resources using Server-Side Apply for idempotent operations
4. **Syncs status** back from physical clusters to KCP workspaces
5. **Coordinates** deletion using finalizers and annotations
6. **Supports upsync** for cluster-originated resources
7. **Maintains health** via heartbeat mechanism

**Key Files:**
- `pkg/syncer/syncer.go:84` - Main setup
- `pkg/syncer/spec/spec_process.go:110` - Spec sync
- `pkg/syncer/status/status_process.go:54` - Status sync
- `pkg/syncer/upsync/upsync_controller.go` - Upsync

### Logical Cluster Deletion Summary

The **logical cluster deletion** process is a careful, multi-phase operation that:

1. **Discovers** all resources in the cluster
2. **Filters** to deletable cluster-scoped resources
3. **Attempts bulk deletion** with fallback to individual deletes
4. **Monitors finalizers** and waits for cleanup
5. **Retries** with exponential backoff until all resources gone
6. **Finalizes** by removing RBAC and owner relationships
7. **Completes** by removing the LogicalCluster itself

**Key Files:**
- `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go:214` - Main controller
- `pkg/reconciler/core/logicalclusterdeletion/deletion/logicalcluster_resource_deletor.go:103` - Resource deletion

Both systems work together to provide robust multi-tenant resource management in KCP.

---

**Document Version**: 1.0
**Last Updated**: 2025-10-29
**KCP Version**: Based on latest main branch analysis
