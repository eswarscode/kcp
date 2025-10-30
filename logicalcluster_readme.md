# LogicalCluster Architecture in KCP

## Table of Contents
- [Overview](#overview)
- [What is a LogicalCluster?](#what-is-a-logicalcluster)
- [Virtual Workspaces](#virtual-workspaces)
- [LogicalCluster Deletion Process](#logicalcluster-deletion-process)
- [Syncer Watch Mechanisms](#syncer-watch-mechanisms)
- [Key File Locations](#key-file-locations)

---

## Overview

KCP implements multi-tenancy through **LogicalClusters** - a storage-level abstraction that allows multiple isolated Kubernetes-like clusters to share a single API server and etcd instance. Each LogicalCluster provides complete resource isolation while maintaining efficient resource utilization.

---

## What is a LogicalCluster?

### Type Definition

**Location**: `pkg/apis/core/v1alpha1/logicalcluster_types.go:41-169`

A LogicalCluster is a Kubernetes custom resource that represents an isolated namespace of API resources within KCP. Key characteristics:

```go
type LogicalCluster struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   LogicalClusterSpec   `json:"spec,omitempty"`
    Status LogicalClusterStatus `json:"status,omitempty"`
}

const LogicalClusterName = "cluster"  // Singleton - each workspace has exactly one
const LogicalClusterFinalizer = "core.kcp.io/logicalcluster"
```

### Specification (`LogicalClusterSpec`)

```go
type LogicalClusterSpec struct {
    // DirectlyDeletable allows user-initiated deletion
    DirectlyDeletable bool

    // Owner references the controlling resource (typically a Workspace)
    Owner *LogicalClusterOwner

    // Initializers lists controllers that must complete before cluster is ready
    Initializers []corev1alpha1.LogicalClusterInitializer
}
```

**Key Fields**:
- **DirectlyDeletable**: When `true`, allows users to delete the cluster directly
- **Owner**: References the parent Workspace resource for lifecycle management
- **Initializers**: Controllers (e.g., API exporters) that must finish initialization

### Status (`LogicalClusterStatus`)

```go
type LogicalClusterStatus struct {
    // URL is the Kubernetes API endpoint
    URL string

    // Phase: Scheduling → Initializing → Ready
    Phase corev1alpha1.LogicalClusterPhaseType

    // Conditions tracks current processing state
    Conditions conditionsv1alpha1.Conditions

    // Initializers still running
    Initializers []corev1alpha1.LogicalClusterInitializer
}
```

**Lifecycle Phases**:
1. **Scheduling**: Cluster is being assigned to a shard
2. **Initializing**: Initializers are running (e.g., setting up default APIs)
3. **Ready**: Fully initialized and accepting API requests

### Design Pattern: Storage-Level Isolation

LogicalClusters achieve isolation through **cluster name prefixing** in storage:
- Each resource key in etcd includes the cluster identifier
- Resources in one cluster are completely invisible to other clusters
- Single API server routes requests based on cluster context
- No network overhead - purely storage-level partitioning

**Example**: A Deployment in workspace `root:org:team` is stored as:
```
/registry/deployments/root:org:team/default/my-deployment
```

---

## Virtual Workspaces

Virtual workspaces are **dynamic API endpoints** that provide transformed views of resources. They enable specialized access patterns for different use cases.

### 1. Syncer Virtual Workspace

**Location**: `pkg/virtual/syncer/builder/build.go:78-98`

**Purpose**: Serve resources from a downstream physical cluster back to KCP for status synchronization.

```go
Name: "syncer"
FilteredResourceState: workloadv1alpha1.ResourceStateSync
```

**How it works**:
1. **Filters resources** to only those marked for sync (excludes Pods, Endpoints by default)
2. **Transforms specs** using `SyncerResourceTransformer` to show diff between desired and actual state
3. **Read-only view** - syncer watches these resources to pull spec changes from KCP
4. **URL format**: `https://kcp-server/services/syncer/<synctarget-uid>/clusters/<workspace>`

**Use Case Example**: Deploying applications to physical clusters

```yaml
# In KCP workspace root:org:team
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
  labels:
    state.workload.kcp.io/my-cluster: Sync  # Marks for sync
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: nginx
        image: nginx:1.21
```

**What happens**:
1. Syncer watches the **syncer virtual workspace** via informers
2. Sees the Deployment marked with `state.workload.kcp.io/my-cluster: Sync`
3. Creates/updates the Deployment in the physical cluster
4. Pulls status back from physical cluster
5. Updates status in KCP via **upsyncer virtual workspace**

### 2. Upsyncer Virtual Workspace

**Location**: `pkg/virtual/syncer/builder/build.go:99-112`

**Purpose**: Accept upstream resources originating from the physical cluster (e.g., PersistentVolumes, Pods).

```go
Name: "upsyncer"
FilteredResourceState: workloadv1alpha1.ResourceStateUpsync
AllowedResourceNames: []string{"persistentvolumes", "pods"}
```

**How it works**:
1. **Restricted resource types** - only cluster-originating resources allowed
2. **Applies transformations** via `UpsyncerResourceTransformer`
3. **Write operations** validated with label selector checks
4. **URL format**: `https://kcp-server/services/upsyncer/<synctarget-uid>/clusters/<workspace>`

**Use Case Example**: Exposing physical cluster PersistentVolumes to KCP

```yaml
# Physical cluster has local storage PV
apiVersion: v1
kind: PersistentVolume
metadata:
  name: local-pv-1
spec:
  capacity:
    storage: 100Gi
  storageClassName: local-storage
  local:
    path: /mnt/disks/ssd1
```

**What happens**:
1. Syncer detects PV in physical cluster
2. Creates corresponding PV in KCP via **upsyncer virtual workspace**
3. Marks it with `state.workload.kcp.io/my-cluster: Upsync`
4. Application in KCP can now claim this PV via PVC

### 3. Initializing Workspaces Virtual Workspace

**Location**: `pkg/virtual/initializingworkspaces/builder/build.go:63-296`

**Purpose**: Provide access to workspace content during initialization phase, before workspace is fully ready.

**Three Variants**:

#### a) Wildcard LogicalClusters Access
**Lines**: `92-122`

```go
// Allows access to logicalclusters across multiple paths
// URL: /clusters/*/apis/core.kcp.io/v1alpha1/logicalclusters
```

**Use Case**: Initializer controllers discovering workspaces across organization hierarchy

#### b) Specific LogicalCluster Access
**Lines**: `124-159`

```go
// Access specific logical cluster resource
// URL: /clusters/root:org:team/apis/core.kcp.io/v1alpha1/logicalclusters/cluster
```

**Use Case**: Reading LogicalCluster spec to determine initialization requirements

#### c) Workspace Content Proxy
**Lines**: `161-289`

```go
// Proxies to real workspace content with initializer permissions
// URL: /clusters/root:org:team/apis/apps/v1/deployments
```

**Use Case**: Initializer creating default resources (e.g., APIBindings, RoleBindings)

**How it works**:
1. **Permission enforcement**: Lines `233-237` - checks if user is listed as initializer
2. **User impersonation**: Lines `254-260` - uses workspace owner identity for authorization
3. **API discovery**: Allows initializers to discover available APIs in the workspace
4. **Resource creation**: Initializers can create resources even when workspace is "Initializing"

**Example**: APIExport Initializer

```yaml
# LogicalCluster during initialization
apiVersion: core.kcp.io/v1alpha1
kind: LogicalCluster
metadata:
  name: cluster
spec:
  initializers:
  - name: "apis.kcp.io/apiexportidentity"
status:
  phase: Initializing
  initializers:
  - name: "apis.kcp.io/apiexportidentity"  # Still running
```

**Initializer process**:
1. Watches LogicalClusters via wildcard virtual workspace
2. Sees `apis.kcp.io/apiexportidentity` in initializers list
3. Accesses workspace content via initializing workspace proxy
4. Creates required APIBinding resources
5. Removes itself from `status.initializers`
6. Workspace transitions to Ready when all initializers complete

---

## LogicalCluster Deletion Process

**Controller Location**: `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go:54-330`

The deletion process is a multi-phase operation that ensures all resources are properly cleaned up before removing the LogicalCluster.

### High-Level Flow

```
User deletes Workspace
    ↓
Workspace controller sets LogicalCluster DeletionTimestamp
    ↓
Deletion controller watches LogicalClusters with DeletionTimestamp
    ↓
Phase 1: Delete all resources in cluster
    ↓
Phase 2: Finalization (remove RBAC, clean up owner, remove finalizers)
    ↓
LogicalCluster removed from etcd
```

### Step-by-Step Deletion Process

#### Step 1: Triggering Deletion

**User action**:
```bash
kubectl delete workspace my-workspace
```

**What happens** (`pkg/reconciler/tenancy/workspace/workspace_controller.go`):
1. Workspace gets `DeletionTimestamp` set
2. Workspace controller sets DeletionTimestamp on owned LogicalCluster
3. LogicalCluster has finalizer `core.kcp.io/logicalcluster` preventing immediate deletion

#### Step 2: Deletion Controller Activation

**Location**: `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go:86-99`

```go
// Only watches LogicalClusters with deletion timestamp
logicalClusterInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
    FilterFunc: func(obj interface{}) bool {
        switch obj := obj.(type) {
        case *corev1alpha1.LogicalCluster:
            return !obj.DeletionTimestamp.IsZero()  // Only deleted resources
        default:
            return false
        }
    },
    Handler: cache.ResourceEventHandlerFuncs{
        AddFunc:    func(obj interface{}) { enqueue(obj) },
        UpdateFunc: func(_, obj interface{}) { enqueue(obj) },
    },
})
```

**Controller enqueues the LogicalCluster for processing.**

#### Step 3: Resource Deletion Phase

**Location**: `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go:214-257`

```go
func (c *Controller) process(ctx context.Context, key string) error {
    // Get LogicalCluster
    logicalCluster, err := c.logicalClusterLister.Cluster(clusterName).Get(name)
    if apierrors.IsNotFound(err) {
        return nil  // Already deleted
    }

    logger.V(2).Info("deleting logical cluster")
    startTime := time.Now()

    // Delete all resources in the cluster
    deleteErr := c.deleter.Delete(ctx, logicalClusterCopy)

    if deleteErr == nil {
        logger.V(2).Info("finished deleting logical cluster content",
            "duration", time.Since(startTime))
        return c.finalizeWorkspace(ctx, logicalClusterCopy)
    }

    // Handle retry with estimate
    var estimate *deletion.ResourcesRemainingError
    if errors.As(deleteErr, &estimate) {
        t := estimate.Estimate/2 + 1
        duration := time.Duration(t) * time.Second
        c.queue.AddAfter(key, duration)  // Retry later
    }
    return deleteErr
}
```

**Resource Deleter** (`pkg/reconciler/core/logicalclusterdeletion/deletion/logicalcluster_resource_deletor.go:89-143`):

```go
func (d *logicalClusterResourcesDeleter) Delete(ctx context.Context,
    logicalCluster *corev1alpha1.LogicalCluster) error {

    // Check if already finalized
    if len(logicalCluster.Finalizers) == 0 {
        return nil
    }

    // Discover all API resources in the cluster
    discoveryClient := d.metadataClusterClient.Cluster(clusterName.Path()).Discovery()
    apiResourceLists, err := d.discoverResourcesFn(clusterName.Path())

    // Delete all resources
    estimate, message, err := d.deleteAllContent(ctx, logicalCluster)

    // If resources still exist, return error with retry estimate
    if estimate > 0 {
        return &ResourcesRemainingError{
            Estimate: estimate,  // Seconds to wait
            Message:  message,
        }
    }

    return nil
}
```

**Deletion strategy**:
1. **API Discovery**: Discovers all resource types (GVRs) via API server
2. **Grouping**: Groups resources by GVR for efficient batch operations
3. **DeleteCollection**: Uses `DeleteCollection` API when available for bulk deletion
4. **Fallback**: Falls back to list-then-delete for resources without `DeleteCollection`
5. **Retry estimation**: Counts remaining resources and estimates time until deletion completes
6. **Rate limiting**: Re-enqueues with backoff if resources still exist

**Example deletion log**:
```
Deleting logical cluster root:org:team:cluster
Discovered 45 API resource types
Deleting deployments.apps: 12 resources
Deleting services: 8 resources
Deleting configmaps: 23 resources
...
Resources remaining: 5 (estimate 10s until complete)
Deletion complete after 15s
```

#### Step 4: Finalization Phase

**Location**: `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go:260-330`

Once all resources are deleted, the controller performs finalization:

```go
func (c *Controller) finalizeWorkspace(ctx context.Context,
    ws *corev1alpha1.LogicalCluster) error {

    for i := range ws.Finalizers {
        if ws.Finalizers[i] == deletion.LogicalClusterDeletionFinalizer {

            // 1. DELETE CLUSTER-SCOPED RBAC RESOURCES
            // Lines 270-277
            err = c.kubeClusterClient.Cluster(clusterName.Path()).
                RbacV1().ClusterRoles().DeleteCollection(ctx,
                    metav1.DeleteOptions{}, metav1.ListOptions{})

            err = c.kubeClusterClient.Cluster(clusterName.Path()).
                RbacV1().ClusterRoleBindings().DeleteCollection(ctx,
                    metav1.DeleteOptions{}, metav1.ListOptions{})

            // 2. CLEAN UP OWNER RESOURCE
            // Lines 279-321
            if ws.Spec.Owner != nil {
                // Parse owner reference
                gvr := schema.GroupVersionResource{...}

                // Get owner resource (e.g., Workspace)
                obj, err := c.dynamicFrontProxyClient.
                    Cluster(clusterPath).Resource(gvr).
                    Namespace(ws.Spec.Owner.Namespace).
                    Get(ctx, ws.Spec.Owner.Name, metav1.GetOptions{})

                // Remove LogicalCluster finalizer from owner
                if finalizers.Has(corev1alpha1.LogicalClusterFinalizer) {
                    finalizers.Delete(corev1alpha1.LogicalClusterFinalizer)
                    obj.SetFinalizers(finalizers.List())

                    c.dynamicFrontProxyClient.Cluster(clusterPath).
                        Resource(gvr).Namespace(ws.Spec.Owner.Namespace).
                        Update(ctx, obj, metav1.UpdateOptions{})
                }

                // Delete owner if marked as directly deletable
                if obj.GetDeletionTimestamp().IsZero() && ws.Spec.DirectlyDeletable {
                    c.dynamicFrontProxyClient.Cluster(clusterPath).
                        Resource(gvr).Namespace(ws.Spec.Owner.Namespace).
                        Delete(ctx, ws.Spec.Owner.Name,
                            metav1.DeleteOptions{})
                }
            }

            // 3. REMOVE FINALIZER FROM LOGICALCLUSTER
            // Line 324
            ws.Finalizers = append(ws.Finalizers[:i], ws.Finalizers[i+1:]...)
            _, err = c.kcpClusterClient.CoreV1alpha1().LogicalClusters().
                Cluster(clusterName.Path()).Update(ctx, ws, metav1.UpdateOptions{})

            return err
        }
    }
    return nil
}
```

**Finalization steps**:
1. **Delete ClusterRoles and ClusterRoleBindings**: Remove cluster-scoped RBAC
2. **Owner cleanup**:
   - Remove `LogicalCluster` finalizer from owner (Workspace)
   - If `DirectlyDeletable=true`, delete the owner Workspace
3. **Remove finalizer**: Remove `core.kcp.io/logicalcluster` finalizer from LogicalCluster
4. **Kubernetes garbage collection**: Automatically deletes LogicalCluster from etcd

#### Step 5: Complete Deletion

After finalizer removal:
1. Kubernetes API server removes LogicalCluster from etcd
2. All storage keys with cluster prefix are now inaccessible
3. Deletion is complete

### Deletion Timeline Example

```
T+0s:   kubectl delete workspace my-workspace
T+0.1s: Workspace controller sets LogicalCluster DeletionTimestamp
T+0.2s: Deletion controller begins resource deletion
T+5s:   Resource deletion phase completes (45 resource types deleted)
T+5.1s: Finalization phase begins
T+5.2s: ClusterRoles and ClusterRoleBindings deleted
T+5.3s: Owner Workspace finalizer removed
T+5.4s: LogicalCluster finalizer removed
T+5.5s: LogicalCluster removed from etcd
T+5.6s: Deletion complete
```

---

## Syncer Watch Mechanisms

The syncer is the component that synchronizes resources between KCP (control plane) and physical Kubernetes clusters. It uses Kubernetes **informers** with list-watch mechanisms for continuous synchronization.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         KCP Server                          │
│                                                             │
│  ┌────────────────┐           ┌────────────────┐          │
│  │ Syncer Virtual │           │ Upsyncer       │          │
│  │ Workspace      │           │ Virtual        │          │
│  │ (spec sync)    │           │ Workspace      │          │
│  └────────┬───────┘           └───────▲────────┘          │
│           │                           │                    │
└───────────┼───────────────────────────┼────────────────────┘
            │                           │
            │ List-Watch                │ Patch Status
            │ (HTTP/1.1)                │ (HTTP/1.1)
            │                           │
┌───────────▼───────────────────────────┴────────────────────┐
│                      Syncer Process                        │
│                                                            │
│  ┌──────────────────────────────────────────────────┐    │
│  │     Spec Syncer (watches KCP resources)          │    │
│  │  - ddsifForUpstreamSyncer (dynamic informers)    │    │
│  │  - Watches resources marked with Sync label      │    │
│  │  - Queue → Apply to downstream cluster           │    │
│  └──────────────────────────────────────────────────┘    │
│                                                            │
│  ┌──────────────────────────────────────────────────┐    │
│  │   Status Syncer (watches downstream resources)    │    │
│  │  - ddsifForDownstream (dynamic informers)        │    │
│  │  - Watches resource status changes               │    │
│  │  - Queue → Patch status to KCP via upsyncer      │    │
│  └──────────────────────────────────────────────────┘    │
│                                                            │
│  ┌──────────────────────────────────────────────────┐    │
│  │           Heartbeat (every 20 seconds)            │    │
│  │  - Patches SyncTarget lastSyncerHeartbeatTime    │    │
│  └──────────────────────────────────────────────────┘    │
└────────────────────────┬───────────────────────────────────┘
                         │
                         │ Apply/Watch
                         │
              ┌──────────▼──────────┐
              │ Physical Kubernetes │
              │      Cluster        │
              └─────────────────────┘
```

### Communication Protocol: HTTP/1.1 List-Watch

Kubernetes informers use the **list-watch pattern** over HTTP/1.1:

1. **LIST**: Initial HTTP GET request fetches all resources
2. **WATCH**: HTTP GET with `?watch=true` parameter keeps connection open
3. **Streaming**: Server streams JSON events as resources change
4. **Reconnection**: Client reconnects on connection drop or timeout

**Example HTTP watch request**:
```http
GET /apis/apps/v1/deployments?watch=true&resourceVersion=12345 HTTP/1.1
Host: kcp-server
Connection: keep-alive
```

**Response stream**:
```json
{"type":"ADDED","object":{"kind":"Deployment","metadata":{...}}}
{"type":"MODIFIED","object":{"kind":"Deployment","metadata":{...}}}
{"type":"DELETED","object":{"kind":"Deployment","metadata":{...}}}
```

### Syncer Initialization

**Location**: `pkg/syncer/syncer.go:84-415`

#### Step 1: Retrieve Virtual Workspace URLs

**Lines**: `117-139`

```go
// Poll for SyncTarget until virtual workspace URLs are available
var syncerVirtualWorkspaceURL string
var upsyncerVirtualWorkspaceURL string

err = wait.PollImmediateInfinite(5*time.Second, func() (bool, error) {
    syncTarget, err := kcpSyncTargetClient.WorkloadV1alpha1().
        SyncTargets().Get(ctx, cfg.SyncTargetName, metav1.GetOptions{})
    if err != nil {
        klog.Errorf("Failed to get synctarget: %v", err)
        return false, err
    }

    // Wait until virtual workspace URLs are populated
    if len(syncTarget.Status.VirtualWorkspaces) == 0 {
        klog.V(2).Infof("Waiting for virtual workspace URLs...")
        return false, nil
    }

    syncerVirtualWorkspaceURL = syncTarget.Status.VirtualWorkspaces[0].SyncerURL
    upsyncerVirtualWorkspaceURL = syncTarget.Status.VirtualWorkspaces[0].UpsyncerURL

    klog.Infof("Got syncer URL: %s", syncerVirtualWorkspaceURL)
    klog.Infof("Got upsyncer URL: %s", upsyncerVirtualWorkspaceURL)

    return true, nil
})
```

**What happens**:
1. Syncer polls KCP every 5 seconds for SyncTarget resource
2. Waits for `status.virtualWorkspaces[]` to be populated by KCP controller
3. Extracts syncer and upsyncer virtual workspace URLs
4. Uses these URLs for all subsequent API requests

**Example URLs**:
```
syncerURL:   https://kcp.example.com/services/syncer/abc-123/clusters/root:org:team
upsyncerURL: https://kcp.example.com/services/upsyncer/abc-123/clusters/root:org:team
```

#### Step 2: Create Informer Factories

**Lines**: `100-104`

```go
// Watch SyncTarget for configuration changes
kcpSyncTargetInformerFactory := kcpinformers.NewSharedScopedInformerFactoryWithOptions(
    kcpSyncTargetClient,
    resyncPeriod,  // 10 * time.Hour
    kcpinformers.WithTweakListOptions(
        func(listOptions *metav1.ListOptions) {
            // Only watch our specific SyncTarget
            listOptions.FieldSelector = fields.OneTermEqualSelector(
                "metadata.name", cfg.SyncTargetName).String()
        },
    ),
)
```

**Informer factory characteristics**:
- **Resync period**: 10 hours (line 64) - full list every 10 hours to detect drift
- **Field selector**: Only watches the specific SyncTarget by name
- **Shared informer**: Multiple controllers share same watch connection

### Dynamic Discovering Informer Factory

**Location**: `pkg/informer/informer.go:107-462`

The core innovation in KCP syncer is the **GenericDiscoveringDynamicSharedInformerFactory** (DDSIF), which automatically:
1. Discovers new resource types (CRDs) as they're added
2. Creates informers for new resources dynamically
3. Stops informers for removed resources
4. Notifies subscribers of GVR changes

#### Architecture

```go
type GenericDiscoveringDynamicSharedInformerFactory struct {
    // Creates new informer for a GVR
    newInformer func(gvr schema.GroupVersionResource,
        resyncPeriod time.Duration,
        indexers cache.Indexers) GenericInformer

    // Filters which objects to process
    filterFunc func(interface{}) bool

    // Source of truth for available GVRs
    gvrSource GVRSource

    // Notification channel for GVR changes
    updateCh chan struct{}

    // Active informers and their lifecycle
    informers        map[schema.GroupVersionResource]GenericInformer
    startedInformers map[schema.GroupVersionResource]bool
    informerStops    map[schema.GroupVersionResource]chan struct{}

    // Event handlers for all GVRs
    handlers atomic.Value  // []GVREventHandler

    // External subscribers to GVR changes
    subscribers map[string]chan<- struct{}
}
```

**Lines**: `111-135`

#### Worker Loop: Continuous Discovery

**Location**: `pkg/informer/informer.go:426-462`

```go
func (d *GenericDiscoveringDynamicSharedInformerFactory) StartWorker(ctx context.Context) {
    logger := klog.FromContext(ctx)

    // Wait for GVR source to be ready (usually API discovery client)
    if !cache.WaitForNamedCacheSync("kcp-ddsif-gvr-source",
        ctx.Done(), d.gvrSource.Ready) {
        logger.Error(nil, "GVR source never synced")
        return
    }

    // Initial discovery
    logger.V(3).Info("performing initial informer update")
    d.updateInformers()

    // Watch for GVR changes
    logger.V(3).Info("starting update loop")
    wait.UntilWithContext(ctx, func(ctx context.Context) {
        select {
        case <-ctx.Done():
            return
        case <-d.updateCh:  // Notified of GVR changes
        }

        logger.V(5).Info("notification received")
        d.updateInformers()  // Recalculate needed informers
    }, time.Second)  // Check at most once per second
}
```

**Lines**: `428-462`

**How it works**:
1. **Wait for GVR source**: Blocks until API discovery is ready
2. **Initial update**: Discovers all current GVRs and creates informers
3. **Watch loop**: Waits on `updateCh` for notifications
4. **Rate limiting**: Processes updates at most once per second
5. **Recalculation**: Calls `updateInformers()` to add/remove informers

#### Event Handler Registration

**Location**: `pkg/informer/informer.go:329-348`

When a new informer is created, it automatically gets event handlers:

```go
// Add event handlers to the informer
inf.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
    FilterFunc: d.filterFunc,  // e.g., only resources with Sync label
    Handler: cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            // Notify all registered handlers
            for _, h := range d.handlers.Load().([]GVREventHandler) {
                h.OnAdd(gvr, obj)
            }
        },
        UpdateFunc: func(oldObj, newObj interface{}) {
            for _, h := range d.handlers.Load().([]GVREventHandler) {
                h.OnUpdate(gvr, oldObj, newObj)
            }
        },
        DeleteFunc: func(obj interface{}) {
            for _, h := range d.handlers.Load().([]GVREventHandler) {
                h.OnDelete(gvr, obj)
            }
        },
    },
})
```

**Lines**: `329-348`

**Event flow**:
```
Physical cluster change
    ↓
Informer receives watch event over HTTP
    ↓
FilterFunc checks if resource matches criteria
    ↓
OnAdd/OnUpdate/OnDelete called for all handlers
    ↓
Handler enqueues item to work queue
    ↓
Worker processes queue item
```

### Spec Syncer: KCP → Physical Cluster

**Location**: `pkg/syncer/spec/spec_controller.go:87-360`

#### Controller Structure

```go
type Controller struct {
    queue workqueue.RateLimitingInterface

    // Upstream (KCP) clients
    upstreamClient kcpdynamic.ClusterInterface

    // Downstream (physical cluster) clients
    downstreamClient dynamic.Interface

    // Listers for checking current state
    getUpstreamLister   func(gvr schema.GroupVersionResource)
        (kcpcache.GenericClusterLister, error)
    getDownstreamLister func(gvr schema.GroupVersionResource)
        (cache.GenericLister, error)
}
```

**Lines**: `87-106`

#### Watching KCP Resources

**Lines**: `125-160`

```go
// Watch upstream (KCP) syncer virtual workspace for spec changes
ddsifForUpstreamSyncer.AddEventHandler(ddsif.GVREventHandlerFuncs{
    AddFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        unstrObj := obj.(*unstructured.Unstructured)

        // Only sync resources marked with Sync state label
        if unstrObj.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+
            syncTargetKey] != string(workloadv1alpha1.ResourceStateSync) {
            return
        }

        c.AddToQueue(gvr, obj, logger)
    },
    UpdateFunc: func(gvr schema.GroupVersionResource, oldObj, newObj interface{}) {
        oldUnstr := oldObj.(*unstructured.Unstructured)
        newUnstr := newObj.(*unstructured.Unstructured)

        // Only sync resources marked with Sync state
        if newUnstr.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+
            syncTargetKey] != string(workloadv1alpha1.ResourceStateSync) {
            return
        }

        // Skip if spec hasn't changed
        if equality.Semantic.DeepEqual(oldUnstr.Object["spec"],
            newUnstr.Object["spec"]) {
            return
        }

        c.AddToQueue(gvr, newUnstr, logger)
    },
    DeleteFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        // Enqueue for deletion from downstream cluster
        c.AddToQueue(gvr, obj, logger)
    },
})
```

**Lines**: `125-160`

**What happens**:
1. **Informer watches** syncer virtual workspace via HTTP list-watch
2. **Filter**: Only processes resources with label `state.workload.kcp.io/<synctarget>: Sync`
3. **Spec comparison**: Skips updates if spec unchanged (status-only updates ignored)
4. **Queue**: Enqueues GVR + object to work queue
5. **Worker**: Processes queue, applies to downstream cluster

**Example sync flow**:
```yaml
# In KCP workspace
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    state.workload.kcp.io/my-cluster: Sync  # Triggers spec syncer
spec:
  replicas: 5  # Change from 3 to 5
```

```
1. User updates Deployment in KCP (replicas: 3 → 5)
2. Informer receives MODIFIED event via watch stream
3. UpdateFunc compares old and new spec
4. Detects spec change (replicas different)
5. Enqueues to work queue: {gvr: deployments.apps, name: web}
6. Worker processes queue item
7. Reads current state from downstream cluster
8. Patches/updates downstream Deployment with new spec
9. Downstream cluster reconciles to 5 replicas
```

### Status Syncer: Physical Cluster → KCP

**Location**: `pkg/syncer/status/status_controller.go:69-236`

#### Controller Structure

```go
type Controller struct {
    queue workqueue.RateLimitingInterface

    // Upstream (KCP) client connected to upsyncer virtual workspace
    upstreamClient kcpdynamic.ClusterInterface

    // Downstream (physical cluster) client
    downstreamClient dynamic.Interface

    // Listers for efficient lookup
    getUpstreamLister   func(gvr schema.GroupVersionResource)
        (kcpcache.GenericClusterLister, error)
    getDownstreamLister func(gvr schema.GroupVersionResource)
        (cache.GenericLister, error)
}
```

**Lines**: `73-88`

#### Watching Downstream Resources

**Lines**: `114-162`

```go
// Watch downstream (physical cluster) resources for status changes
ddsifForDownstream.AddEventHandler(ddsif.GVREventHandlerFuncs{
    AddFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        unstrObj := obj.(*unstructured.Unstructured)

        // Skip resources created by upsync (they originated from cluster)
        if unstrObj.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+
            syncTargetKey] == string(workloadv1alpha1.ResourceStateUpsync) {
            return
        }

        c.AddToQueue(gvr, obj, logger)
    },
    UpdateFunc: func(gvr schema.GroupVersionResource, oldObj, newObj interface{}) {
        oldUnstr := oldObj.(*unstructured.Unstructured)
        newUnstr := newObj.(*unstructured.Unstructured)

        // Skip upsync resources
        if newUnstr.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+
            syncTargetKey] == string(workloadv1alpha1.ResourceStateUpsync) {
            return
        }

        // Only sync if status or finalizers changed
        if !deepEqualFinalizersAndStatus(oldUnstr, newUnstr) {
            c.AddToQueue(gvr, newUnstr, logger)
        }
    },
    DeleteFunc: func(gvr schema.GroupVersionResource, obj interface{}) {
        unstrObj := obj.(*unstructured.Unstructured)

        // Skip upsync resources
        if unstrObj.GetLabels()[workloadv1alpha1.ClusterResourceStateLabelPrefix+
            syncTargetKey] == string(workloadv1alpha1.ResourceStateUpsync) {
            return
        }

        c.AddToQueue(gvr, obj, logger)
    },
})
```

**Lines**: `114-162`

**What happens**:
1. **Informer watches** physical cluster resources via standard Kubernetes API
2. **Filter**: Excludes Upsync resources (those originated from cluster)
3. **Status comparison**: Only enqueues if status or finalizers changed
4. **Queue**: Enqueues for status update to KCP
5. **Worker**: Patches status to KCP via upsyncer virtual workspace

**Example status sync flow**:
```yaml
# Physical cluster
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
status:
  replicas: 5
  availableReplicas: 5  # Becomes ready
  conditions:
  - type: Available
    status: "True"
```

```
1. Deployment in physical cluster becomes Available
2. Informer receives MODIFIED event via watch stream
3. UpdateFunc detects status change (availableReplicas: 3 → 5)
4. Enqueues to work queue: {gvr: deployments.apps, name: web}
5. Worker processes queue item
6. Reads current upstream (KCP) resource
7. Merges downstream status into upstream resource
8. Patches status to KCP via upsyncer virtual workspace
9. KCP resource now shows updated status
```

#### Status Merge Logic

**Location**: `pkg/syncer/status/status_controller.go:179-236`

```go
func (c *Controller) reconcile(ctx context.Context, gvr schema.GroupVersionResource,
    namespace, name string) error {

    // Get downstream (physical cluster) resource
    downstreamObj, err := c.getDownstreamLister(gvr).
        ByNamespace(namespace).Get(name)

    // Get upstream (KCP) resource
    upstreamObj, err := c.getUpstreamLister(gvr).
        Cluster(syncTargetClusterName).
        ByNamespace(namespace).Get(name)

    // Extract status from downstream
    downstreamStatus := downstreamObj.Object["status"]

    // Extract status from upstream
    upstreamStatus := upstreamObj.Object["status"]

    // Only update if status changed
    if equality.Semantic.DeepEqual(upstreamStatus, downstreamStatus) {
        return nil  // No change needed
    }

    // Create copy with updated status
    upstreamObjCopy := upstreamObj.DeepCopy()
    upstreamObjCopy.Object["status"] = downstreamStatus

    // Patch status to KCP via upsyncer virtual workspace
    _, err = c.upstreamClient.
        Cluster(syncTargetClusterName).
        Resource(gvr).
        Namespace(namespace).
        UpdateStatus(ctx, upstreamObjCopy, metav1.UpdateOptions{})

    return err
}
```

**Lines**: `179-236`

### Heartbeat Mechanism

**Location**: `pkg/syncer/syncer.go:417-439`

The syncer maintains a heartbeat to prove it's still alive and syncing.

```go
const heartbeatInterval = 20 * time.Second

func StartHeartbeat(ctx context.Context,
    kcpSyncTargetClient kcpclientset.Interface,
    syncTargetName, syncTargetUID string) {

    go wait.UntilWithContext(ctx, func(ctx context.Context) {
        _ = wait.PollImmediateInfiniteWithContext(ctx, 1*time.Second,
            func(ctx context.Context) (bool, error) {

            // JSON Patch to update heartbeat timestamp
            patchBytes := []byte(fmt.Sprintf(
                `[{"op":"test","path":"/metadata/uid","value":%q},`+
                `{"op":"replace","path":"/status/lastSyncerHeartbeatTime","value":%q}]`,
                syncTargetUID,
                time.Now().Format(time.RFC3339)))

            // Patch SyncTarget status
            syncTarget, err := kcpSyncTargetClient.WorkloadV1alpha1().
                SyncTargets().Patch(ctx, syncTargetName, types.JSONPatchType,
                    patchBytes, metav1.PatchOptions{}, "status")

            if err != nil {
                klog.Errorf("failed to update heartbeat: %v", err)
            }

            return true, nil
        })
    }, heartbeatInterval)  // Every 20 seconds
}
```

**Lines**: `417-439`

**Heartbeat characteristics**:
- **Frequency**: Every 20 seconds
- **Method**: JSON Patch operation
- **Field**: `status.lastSyncerHeartbeatTime`
- **Purpose**: KCP controllers monitor this to detect dead syncers
- **UID test**: Ensures we're updating the correct SyncTarget instance

**Example SyncTarget status**:
```yaml
apiVersion: workload.kcp.io/v1alpha1
kind: SyncTarget
metadata:
  name: my-cluster
  uid: abc-123-def-456
status:
  lastSyncerHeartbeatTime: "2025-10-30T12:34:56Z"  # Updated every 20s
  virtualWorkspaces:
  - syncerURL: https://kcp.example.com/services/syncer/abc-123/...
    upsyncerURL: https://kcp.example.com/services/upsyncer/abc-123/...
```

### Communication Summary

| Component | Direction | Protocol | Mechanism | Frequency |
|-----------|-----------|----------|-----------|-----------|
| Spec Syncer | KCP → Physical | HTTP/1.1 List-Watch | Informer on syncer virtual workspace | Continuous watch stream |
| Status Syncer | Physical → KCP | HTTP/1.1 Patch | UpdateStatus via upsyncer virtual workspace | On status change |
| Heartbeat | Syncer → KCP | HTTP/1.1 JSON Patch | Patch SyncTarget status | Every 20 seconds |
| API Discovery | Syncer ↔ KCP | HTTP/1.1 GET | Discovery API | On GVR changes |
| Virtual Workspace URLs | Syncer ← KCP | HTTP/1.1 GET | Poll SyncTarget | Every 5s until populated |

**Key characteristics**:
1. **No proprietary protocol**: Uses standard Kubernetes API mechanisms
2. **Efficient**: Watch streams avoid polling, only sends changes
3. **Resilient**: Automatic reconnection on connection drop
4. **Scalable**: Virtual workspaces provide isolated views per syncer
5. **Firewall-friendly**: All communication over HTTPS outbound from syncer

---

## Key File Locations

### Core Types
| Component | File Path | Lines |
|-----------|-----------|-------|
| LogicalCluster Type | `pkg/apis/core/v1alpha1/logicalcluster_types.go` | 41-169 |
| Workspace Type | `pkg/apis/tenancy/v1alpha1/types_workspace.go` | 118-217 |
| SyncTarget Type | `pkg/apis/workload/v1alpha1/synctarget_types.go` | 29-176 |

### Virtual Workspaces
| Component | File Path | Lines |
|-----------|-----------|-------|
| Syncer/Upsyncer Virtual Workspaces | `pkg/virtual/syncer/builder/build.go` | 45-114 |
| Initializing Workspaces | `pkg/virtual/initializingworkspaces/builder/build.go` | 63-296 |

### Controllers
| Component | File Path | Lines |
|-----------|-----------|-------|
| LogicalCluster Controller | `pkg/reconciler/core/logicalcluster/logicalcluster_controller.go` | 43-189 |
| LogicalCluster Deletion Controller | `pkg/reconciler/core/logicalclusterdeletion/logicalcluster_deletion_controller.go` | 54-330 |
| Resource Deletor | `pkg/reconciler/core/logicalclusterdeletion/deletion/logicalcluster_resource_deletor.go` | 67-150+ |
| Workspace Controller | `pkg/reconciler/tenancy/workspace/workspace_controller.go` | 52-100+ |

### Syncer
| Component | File Path | Lines |
|-----------|-----------|-------|
| Syncer Entry Point | `pkg/syncer/syncer.go` | 84-415 |
| Spec Syncer (KCP → Physical) | `pkg/syncer/spec/spec_controller.go` | 87-360 |
| Status Syncer (Physical → KCP) | `pkg/syncer/status/status_controller.go` | 69-236 |
| Dynamic Informer Factory | `pkg/informer/informer.go` | 107-462 |

---

## Conclusion

KCP's LogicalCluster architecture provides:

1. **Efficient Multi-Tenancy**: Storage-level isolation without separate API servers
2. **Flexible Virtual Workspaces**: Dynamic API views for different access patterns
3. **Safe Deletion**: Multi-phase deletion with finalization guarantees
4. **Robust Synchronization**: Kubernetes-native watch mechanisms for real-time sync
5. **Scalability**: Dynamic informer discovery supports unlimited resource types

The syncer's continuous watch mechanisms, combined with virtual workspaces, enable seamless multi-cluster orchestration while maintaining strong isolation and consistency guarantees.
