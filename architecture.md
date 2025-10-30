# KCP Architecture Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Main Entry Points & Startup](#main-entry-points--startup)
3. [Core Architecture Components](#core-architecture-components)
4. [Controllers & Reconciliation Loops](#controllers--reconciliation-loops)
5. [Request Flow & Routing](#request-flow--routing)
6. [API Definitions & Custom Resources](#api-definitions--custom-resources)
7. [Admission Control](#admission-control)
8. [Testing Structure](#testing-structure)
9. [Key Directories](#key-directories)
10. [Flow Examples](#flow-examples)
11. [Key Design Patterns](#key-design-patterns)

---

## Project Overview

**KCP (Kubernetes Control Plane)** is a highly multi-tenant Kubernetes-like control plane built for SaaS service providers. It provides:

- A control plane for many independent, isolated "clusters" known as **workspaces**
- Multi-tenant operators that enable API service providers to offer APIs centrally
- Flexible scheduling of workloads to physical clusters
- Transparent movement of workloads among compatible physical clusters
- Advanced deployment strategies (affinity/anti-affinity, geographic replication, cross-cloud)

### Key Concept: Logical Clusters

KCP uses "logical clusters" - a storage-level concept that adds a cluster name attribute to object identifiers, allowing multiple isolated clusters to share a single API server and etcd instance. This is the foundation of KCP's multi-tenancy model.

---

## Main Entry Points & Startup

### Main Binaries (in `/cmd`)

#### 1. **`kcp`** (`cmd/kcp/kcp.go`)
Full-featured KCP server with tenancy management, multi-user support, and batteries-included components.
- **Entry point**: `kcp start` command
- **Builds**: Complete control plane via `tmcserver.NewServer()`
- **Features**: Full tenancy, workload scheduling, API management

#### 2. **`kcp-core`** (`cmd/kcp-core/kcpcore.go`)
Generic control plane core providing multi-tenant workspace hierarchy.
- **Entry point**: `kcp-core start` command
- **Creates**: Generic Kubernetes-like API servers via `server.NewServer()`
- **Use case**: Minimal control plane without batteries-included features

#### 3. **`syncer`** (`cmd/syncer/cmd`)
Synchronizer that runs on sync targets (physical clusters).
- **Purpose**: Synchronizes data between KCP and physical clusters
- **Deployment**: Runs in each physical cluster

#### 4. **Other Utilities**
- **`cluster-controller`**: Manages API resources and negotiations
- **`kubectl-kcp`** / **`kubectl-workspace`**: CLI plugins
- **`virtual-workspaces`**: Virtual workspace handler
- **`cache-server`**: Caching layer
- **`kcp-front-proxy`**: Front-end proxy for request routing

### Startup Flow

1. **Parse Configuration**: Root directory flag (default: `.kcp`)
2. **Initialize Options**: Server options with logging configuration
3. **Create Embedded etcd**: If configured, start embedded etcd server
4. **Build Server Config**: Complete server configuration from options
5. **Create Server**: Instantiate the server with all components
6. **Start Post-Start Hooks**: Initialize all controllers in dependency order

---

## Core Architecture Components

### 1. Logical Clusters & Multi-Tenancy

#### **LogicalCluster** (`pkg/apis/core/v1alpha1/logicalcluster_types.go`)
Storage-level abstraction enabling multiple isolated Kubernetes-like clusters to share a single API server instance.
- **Storage Pattern**: All etcd keys prefixed with cluster name: `/<resource>/<cluster>/<namespace>/<name>`
- **Isolation**: Complete resource isolation between logical clusters
- **Efficiency**: Share single API server and etcd instance

#### **Workspace** (`pkg/apis/tenancy/v1alpha1/types_workspace.go`)
User-facing API for managing logical clusters.
- **Types**: Defines what APIs are available (e.g., Kubernetes, Knative)
- **Quotas**: Resource limits per workspace
- **API Bindings**: Which external APIs the workspace can access
- **Phases**: Scheduling → Initializing → Ready

#### **WorkspaceType** (`pkg/apis/tenancy/v1alpha1/types_workspacetype.go`)
Defines templates for workspace initialization.
- **Default APIs**: APIs automatically bound to new workspaces
- **Optional APIs**: APIs available for manual binding
- **Extensions**: Additional initialization beyond API bindings

#### **Shard** (`pkg/apis/core/v1alpha1/shard_types.go`)
Failure domain representing a single API server + etcd instance.
- **Base URL**: External URL where shard is accessible
- **Virtual Workspace URLs**: URLs for virtual workspace access
- **External URL**: Public-facing URL for client access

### 2. API Management Subsystem

#### **APIExport** (`pkg/apis/apis/v1alpha1/types_apiexport.go`)
Service providers expose APIs through this resource.
- **Identity**: Unique identifier for the API export (immutable)
- **Claimed Resources**: APIs being exported (GroupVersionResources)
- **Permission Claims**: What permissions the API provider needs on consumer workspaces
- **Latest Resource Schemas**: Current API schemas being exported

#### **APIBinding** (`pkg/apis/apis/v1alpha1/types_apibinding.go`)
Workspaces bind to APIs from providers.
- **Reference**: Points to APIExport (by path and export name)
- **Permission Claims**: Accepted/rejected permission claims from provider
- **Bound Resources**: APIs that were successfully bound
- **Phases**: Binding → Ready

#### **APIResourceSchema** & **APIResource**
Define and manage API schemas and versioning.
- **Schema Definition**: OpenAPI v3 schema for resources
- **Versioning**: Support for multiple API versions
- **Conversion**: Conversion webhooks for version migration

#### **APIResourceImport**
Allows workspaces to import APIs from other workspaces.
- **Negotiation**: Syncer negotiates which APIs to import
- **Schema Compatibility**: Ensures compatible API versions

#### **Permission Claims**
Fine-grained access control mechanism for API providers.
- **Group/Resource**: What resources the provider needs access to
- **Identity Hash**: Tracks which APIExport the claim is for
- **State**: Accepted or pending user approval

### 3. Workload Scheduling & Distribution

#### **SyncTarget** (`pkg/apis/workload/v1alpha1/synctarget_types.go`)
Represents a physical cluster capable of running workloads.

**Key Features**:
- **Sync States**: Sync (normal), Upsync (push from cluster), Pending (not yet synced)
- **Unschedulability**: Can be marked as unavailable for new workloads
- **Eviction**: Can trigger removal of workloads from target
- **Supported API Exports**: Which APIs the cluster can handle
- **Cells**: Organizational grouping for sync targets

**Status Tracking**:
- **Virtual Workspaces**: URLs for accessing syncer views
- **Capacity & Allocatable**: Resource availability (CPU, memory, etc.)
- **Last Sync Time**: Heartbeat tracking

#### **Location** (`pkg/apis/scheduling/v1alpha1/`)
Collection of SyncTargets describing runtime characteristics.
- **Labels**: GPU availability, storage types, compliance zones, geography
- **Purpose**: Logical grouping for placement decisions
- **Instance Selector**: Which SyncTargets belong to this location

#### **Placement** (`pkg/apis/scheduling/v1alpha1/`)
Determines which Location a workload lands on.
- **Location Selectors**: Requirements for location selection
- **Namespace Selector**: Which namespaces this placement applies to
- **Selected Location**: Resolved target location

#### **Partition & PartitionSet**
Advanced scheduling mechanisms for workload distribution.
- **Dimensions**: Define how to partition workload distribution
- **Strategies**: Various distribution algorithms

### 4. Syncer & Workload Sync Protocol

The **syncer** (`pkg/syncer/syncer.go`) runs on each physical cluster and performs bidirectional synchronization.

#### **Syncer Responsibilities**

1. **API Negotiation**: Make physical cluster APIs accessible in workspaces
2. **Spec Sync (Downstream)**: Synchronize resource specs from KCP → cluster
3. **Status Sync (Upstream)**: Synchronize status and state from cluster → KCP
4. **Resource Lifecycle**: Manage create, update, delete operations
5. **State Machine**: Track resource sync state through labels

#### **Syncer Components**

Located in `pkg/syncer/`:
- **`spec`**: Downstream resource synchronization (KCP → cluster)
- **`status`**: Upstream status syncing (cluster → KCP)
- **`upsync`**: Dedicated syncing from cluster → KCP for cluster-originated resources
- **`namespace`**: Namespace lifecycle management and mapping
- **`resourcesync`**: Core synchronization engine and state machine
- **`endpoints`**: Service endpoint management and aggregation
- **`controllermanager`**: Manages all syncer-side controllers

#### **State Management via Labels**

Resources use labels for async state coordination:

```yaml
labels:
  state.workload.kcp.io/<sync-target-name>: Sync  # or Upsync, Pending
  deletion.internal.workload.kcp.io/<sync-target-name>: "1234567890"  # deletion timestamp
  finalizers.workload.kcp.io/<sync-target-name>: ""  # finalizer coordination
```

#### **Sync State Machine**

```
[KCP Resource Created]
         ↓
    [Pending] ← Placement controller assigns to SyncTarget
         ↓
      [Sync] ← Syncer creates in physical cluster
         ↓
   [Running/Active] ← Syncer updates status from cluster
         ↓
    [Deleted] ← Deletion triggers finalizer cleanup
```

---

## Controllers & Reconciliation Loops

All controllers follow the Kubernetes controller pattern:
1. **Watch**: Monitor resources via informers
2. **Enqueue**: Add changed resources to work queue
3. **Reconcile**: Process work items and update state
4. **Requeue**: Schedule retry on errors or recheck

### Controller Organization

Controllers are organized in `pkg/reconciler/` by domain:

```
pkg/reconciler/
├── tenancy/          # Workspace and type management
├── workload/         # Workload placement and syncing
├── scheduling/       # Placement decisions
├── apis/             # APIBinding and APIExport
├── cache/            # Cache management
├── topology/         # Partition management
└── core/             # Core infrastructure
```

### All Controllers (44 Total)

#### **TENANCY CONTROLLERS (7)**

1. **`workspace`** (`pkg/reconciler/tenancy/workspace/`)
   - **Purpose**: Manages workspace lifecycle
   - **Reconciles**: Workspace resources
   - **Actions**:
     - Creates underlying LogicalCluster
     - Initializes workspace with APIs from WorkspaceType
     - Sets up initial APIBindings
     - Manages workspace phase transitions (Scheduling → Initializing → Ready)
   - **Key File**: `workspace_controller.go:181`

2. **`workspacetype`** (`pkg/reconciler/tenancy/workspacetype/`)
   - **Purpose**: Manages workspace type definitions
   - **Reconciles**: WorkspaceType resources
   - **Actions**:
     - Validates type definitions
     - Manages default and optional API lists
     - Updates status with current state
   - **Key File**: `workspacetype_controller.go`

3. **`logicalcluster`** (`pkg/reconciler/core/logicalcluster/`)
   - **Purpose**: Core logical cluster lifecycle management
   - **Reconciles**: LogicalCluster resources
   - **Actions**:
     - Initializes logical cluster in etcd
     - Manages cluster status and conditions
     - Handles cluster-level finalizers
   - **Key File**: `logicalcluster_controller.go`

4. **`logicalcluster-deletion`** (`pkg/reconciler/core/logicaclusterdeletion/`)
   - **Purpose**: Cleanup and deletion of logical clusters
   - **Reconciles**: LogicalCluster resources with deletion timestamp
   - **Actions**:
     - Removes all resources from logical cluster
     - Ensures clean deletion without orphans
     - Removes finalizers when complete
   - **Key File**: `logicalcluster_deletion_controller.go`

5. **`bootstrap`** (`pkg/reconciler/tenancy/bootstrap/`)
   - **Purpose**: Initializes bootstrap workspaces and system resources
   - **Reconciles**: System bootstrap on startup
   - **Actions**:
     - Creates system workspaces
     - Loads default RBAC policies
     - Initializes built-in WorkspaceTypes
   - **Key File**: `bootstrap_controller.go`

6. **`initialization`** (`pkg/reconciler/tenancy/initialization/`)
   - **Purpose**: Initializes new workspaces with APIs from their WorkspaceType
   - **Reconciles**: Workspace resources in Initializing phase
   - **Actions**:
     - Creates APIBindings for default APIs
     - Waits for APIBindings to become ready
     - Transitions workspace to Ready phase
   - **Key File**: `initialization_controller.go`

7. **Replication Controllers**
   - **`replicateclusterrole`**: Replicates ClusterRoles across workspaces
   - **`replicateclusterrolebinding`**: Replicates ClusterRoleBindings
   - **`replicatelogicalcluster`**: Replicates system resources
   - **Purpose**: Ensure system resources are available in all workspaces
   - **Key Files**: `pkg/reconciler/tenancy/replication/`

#### **API BINDING & EXPORT CONTROLLERS (8)**

8. **`apibinding`** (`pkg/reconciler/apis/apibinding/`)
   - **Purpose**: Materializes APIs from APIExports into consumer workspace
   - **Reconciles**: APIBinding resources
   - **Actions**:
     - Watches APIBindings and referenced APIExports
     - Syncs API schemas (CRDs) into workspace
     - Materializes CRDs in consumer workspace
     - Manages permission claims (accept/reject)
     - Updates binding status with bound resources
   - **Key File**: `apibinding_controller.go:294`
   - **Critical Path**: This is how APIs become available in workspaces

9. **`apibindingdeletion`** (`pkg/reconciler/apis/apibindingdeletion/`)
   - **Purpose**: Cleanup when APIBinding is deleted
   - **Reconciles**: APIBinding resources with deletion timestamp
   - **Actions**:
     - Removes materialized CRDs from workspace
     - Cleans up permission claim labels
     - Removes finalizers
   - **Key File**: `apibindingdeletion_controller.go`

10. **`apiexport`** (`pkg/reconciler/apis/apiexport/`)
    - **Purpose**: Manages APIExport lifecycle and validation
    - **Reconciles**: APIExport resources
    - **Actions**:
      - Validates exported API schemas
      - Sets up identity hash (immutable)
      - Updates status with available schemas
      - Manages permission claims
    - **Key File**: `apiexport_controller.go:187`

11. **`apiexportendpointslice`** (`pkg/reconciler/apis/apiexportendpointslice/`)
    - **Purpose**: Tracks where APIs are accessible (which shards/URLs)
    - **Reconciles**: APIExportEndpointSlice resources
    - **Actions**:
      - Tracks partition and shard information
      - Updates endpoint URLs
      - Manages endpoint lifecycle
    - **Key File**: `apiexportendpointslice_controller.go`

12. **`identitycache`** (`pkg/reconciler/apis/identitycache/`)
    - **Purpose**: Caches APIExport identities for fast lookup
    - **Reconciles**: APIExport resources
    - **Actions**:
      - Builds cache of APIExport identity → export mapping
      - Enables quick resolution of APIBinding references
    - **Key File**: `identitycache_controller.go`

13. **`permissionclaimlabel`** (`pkg/reconciler/apis/permissionclaimlabel/`)
    - **Purpose**: Labels resources with permission claims
    - **Reconciles**: Resources that have permission claims
    - **Actions**:
      - Adds labels for accepted permission claims
      - Enables authorization based on claims
      - Updates labels when claims change
    - **Key File**: `permissionclaimlabel_controller.go`

14. **`extraannotationsync`** (`pkg/reconciler/apis/extraannotationsync/`)
    - **Purpose**: Syncs annotations between APIExports and APIBindings
    - **Reconciles**: APIBinding resources
    - **Actions**:
      - Copies annotations from APIExport to binding
      - Keeps metadata in sync
    - **Key File**: `extraannotationsync_controller.go`

15. **Replication Controllers** (for APIs)
    - Replicate ClusterRoles/Bindings needed for API access
    - Ensure proper RBAC for API consumers

#### **WORKLOAD CONTROLLERS (8)**

16. **`placement`** (`pkg/reconciler/workload/placement/`)
    - **Purpose**: Places workloads onto SyncTargets based on location requirements
    - **Reconciles**: Resources in namespaces with Placement
    - **Actions**:
      - Evaluates location selectors
      - Selects compatible SyncTarget
      - Labels resource with `state.workload.kcp.io/<target>=Sync`
      - Handles rescheduling on eviction
    - **Key File**: `placement_controller.go:215`
    - **Critical Path**: This assigns workloads to physical clusters

17. **`placement`** (scheduling) (`pkg/reconciler/scheduling/placement/`)
    - **Purpose**: Determines which Location satisfies placement requirements
    - **Reconciles**: Placement resources
    - **Actions**:
      - Evaluates location selectors
      - Finds compatible locations
      - Updates Placement status with selected location
    - **Key File**: `placement_controller.go`

18. **`synctarget`** (`pkg/reconciler/workload/synctarget/`)
    - **Purpose**: Manages SyncTarget lifecycle and status
    - **Reconciles**: SyncTarget resources
    - **Actions**:
      - Tracks syncer heartbeats
      - Updates capacity/allocatable status
      - Manages virtual workspace URLs
      - Handles unschedulable/eviction states
    - **Key File**: `synctarget_controller.go:189`

19. **`resource`** (`pkg/reconciler/workload/resource/`)
    - **Purpose**: Syncs resource specs to SyncTargets
    - **Reconciles**: Resources with placement labels
    - **Actions**:
      - Coordinates with syncer on spec updates
      - Manages resource state transitions
      - Handles deletion and finalizers
    - **Key File**: `resource_controller.go`

20. **`heartbeat`** (`pkg/reconciler/workload/heartbeat/`)
    - **Purpose**: Monitors heartbeat from syncers
    - **Reconciles**: SyncTarget resources
    - **Actions**:
      - Checks last heartbeat timestamp
      - Marks targets as unavailable if heartbeat lost
      - Triggers eviction if needed
    - **Key File**: `heartbeat_controller.go`

21. **`namespace`** (`pkg/reconciler/workload/namespace/`)
    - **Purpose**: Manages namespace creation on SyncTargets
    - **Reconciles**: Namespace resources
    - **Actions**:
      - Creates namespaces on physical clusters via syncer
      - Syncs namespace labels and annotations
      - Handles namespace deletion
    - **Key File**: `namespace_controller.go`

22. **`apiexport`** (workload) (`pkg/reconciler/workload/apiexport/`)
    - **Purpose**: Creates APIExport for workload APIs
    - **Reconciles**: System APIExports for workload APIs
    - **Actions**:
      - Exports workload-related APIs
      - Makes scheduling APIs available
    - **Key File**: `apiexport_controller.go`

23. **`apiexportcreate`** (`pkg/reconciler/workload/apiexportcreate/`)
    - **Purpose**: Creates new APIExports for discovered APIs
    - **Reconciles**: Watches for new API schemas
    - **Actions**:
      - Creates APIExport when new API is registered
      - Links schemas to exports
    - **Key File**: `apiexportcreate_controller.go`

#### **CORE INFRASTRUCTURE CONTROLLERS (8)**

24. **`logicalcluster`** (core) (`pkg/reconciler/core/logicalcluster/`)
    - **Purpose**: Core logical cluster resource management
    - Already described above in Tenancy section

25. **`logicalcluster-deletion`** (core)
    - **Purpose**: Core logical cluster cleanup
    - Already described above in Tenancy section

26. **`shard`** (`pkg/reconciler/core/shard/`)
    - **Purpose**: Manages Shard resources and distribution
    - **Reconciles**: Shard resources
    - **Actions**:
      - Tracks shard availability
      - Updates shard URLs
      - Manages shard capacity
    - **Key File**: `shard_controller.go`

27. **`garbagecollector`** (`pkg/reconciler/core/garbagecollector/`)
    - **Purpose**: Garbage collection across logical clusters
    - **Reconciles**: Resources with owner references
    - **Actions**:
      - Deletes resources when owners are deleted
      - Respects finalizers and deletion order
      - Handles cross-cluster references
    - **Key File**: `garbagecollector_controller.go:134`

28. **`kubequota`** (`pkg/reconciler/quota/`)
    - **Purpose**: Implements resource quota management
    - **Reconciles**: ResourceQuota resources
    - **Actions**:
      - Tracks resource usage per workspace
      - Enforces quota limits
      - Updates quota status
    - **Key File**: `kubequota_controller.go`

29. **`clusterroleaggregation`** (Kubernetes)
    - **Purpose**: Aggregates ClusterRoles based on labels
    - **Reconciles**: ClusterRole resources with aggregation rules
    - **Actions**:
      - Combines multiple roles into aggregate role
      - Updates aggregate role when component roles change
    - **From**: Standard Kubernetes controller

30-31. **Replication Controllers** (core)
    - Replicate core system resources
    - Ensure consistency across shards

#### **CACHE CONTROLLERS (4)**

32. **`labellogicalcluster`** (`pkg/reconciler/cache/labellogicalcluster/`)
    - **Purpose**: Adds labels to logical cluster references
    - **Reconciles**: Resources in cache
    - **Actions**:
      - Labels resources with their logical cluster
      - Enables efficient cache queries
    - **Key File**: `labellogicalcluster_controller.go`

33. **`labelclusterroles`** (`pkg/reconciler/cache/labelclusterroles/`)
    - **Purpose**: Labels ClusterRoles for cache efficiency
    - **Reconciles**: ClusterRole resources
    - **Actions**:
      - Adds cache-relevant labels
      - Optimizes RBAC queries
    - **Key File**: `labelclusterroles_controller.go`

34. **`labelclusterrolebindings`** (`pkg/reconciler/cache/labelclusterrolebindings/`)
    - **Purpose**: Labels ClusterRoleBindings for cache
    - **Reconciles**: ClusterRoleBinding resources
    - **Actions**:
      - Adds cache-relevant labels
      - Optimizes RBAC queries
    - **Key File**: `labelclusterrolebindings_controller.go`

35. **`replication`** (`pkg/reconciler/cache/replication/`)
    - **Purpose**: Replicates cached resources across shards
    - **Reconciles**: Cached resources
    - **Actions**:
      - Syncs cache entries
      - Handles cache invalidation
    - **Key File**: `replication_controller.go`

#### **TOPOLOGY & COORDINATION CONTROLLERS (2)**

36. **`partitionset`** (`pkg/reconciler/topology/partitionset/`)
    - **Purpose**: Manages resource partitioning for distributed placement
    - **Reconciles**: PartitionSet resources
    - **Actions**:
      - Calculates partition dimensions
      - Assigns resources to partitions
      - Balances distribution
    - **Key File**: `partitionset_controller.go`

37. **`deployment`** (coordination) (`pkg/reconciler/coordination/deployment/`)
    - **Purpose**: Coordinates deployment across shards
    - **Reconciles**: Deployment resources
    - **Actions**:
      - Ensures deployment across required shards
      - Coordinates rollout strategies
    - **Key File**: `deployment_controller.go`

#### **OTHER CONTROLLERS**

38. **`crdcleanup`** (`pkg/reconciler/crdcleanup/`)
    - **Purpose**: Removes orphaned CRDs
    - **Reconciles**: CRD resources
    - **Actions**:
      - Detects CRDs no longer referenced by APIBindings
      - Removes unused CRDs from workspace
      - Cleans up on APIBinding deletion
    - **Key File**: `crdcleanup_controller.go`

39. **`location`** (scheduling) (`pkg/reconciler/scheduling/location/`)
    - **Purpose**: Manages Location definitions
    - **Reconciles**: Location resources
    - **Actions**:
      - Updates location status
      - Tracks available SyncTargets
      - Validates location selectors
    - **Key File**: `location_controller.go`

### Controller Initialization Order

Controllers are registered via post-start hooks in dependency order:

1. **Core Infrastructure**: Logical clusters, shards, garbage collection
2. **Tenancy**: Bootstrap, workspace types, workspaces
3. **API Management**: APIExports, APIBindings
4. **Workload**: SyncTargets, placement, resource sync
5. **Cache & Optimization**: Cache controllers, replication

---

## Request Flow & Routing

### Request Entry Points

#### 1. **Direct Server Handler** (`pkg/server/handler.go`)

Entry point for all API requests to KCP server.

```
[Client Request]
      ↓
[WithAuditAnnotation] ← Initialize audit context
      ↓
[WithUserAgent] ← Extract user agent
      ↓
[WithAuthenticationInfo] ← Parse authentication
      ↓
[Go-Restful Router] ← Route to API handler
      ↓
[API Handler] ← Process request
```

**Key Filters**:
- `WithAuditAnnotation`: Adds audit annotations for tracking
- `WithUserAgent`: Extracts and validates user agent
- `WithAuthenticationInfo`: Populates authentication context
- `WithRequestInfo`: Parses request metadata (verb, resource, namespace)

**Reference**: `pkg/server/handler.go`

#### 2. **Shard Proxy Handler** (`pkg/proxy/handler.go`)

Routes requests to appropriate shard based on cluster path.

```
[Client Request] → GET /clusters/root:org:ws/apis/apps/v1/deployments
      ↓
[Parse Cluster Path] ← Extract "root:org:ws"
      ↓
[Index Lookup] ← Find shard for cluster
      ↓
[Create Reverse Proxy] ← Proxy to shard URL
      ↓
[Forward Request] ← https://shard-1.kcp.io/clusters/root:org:ws/...
```

**URL Pattern**: `/clusters/{clusterPath}/...`
- **Cluster Path**: Colon-separated workspace path (e.g., `root:org:workspace`)
- **Rest of Path**: Standard Kubernetes API path

**Key Functions**:
- `ParseClusterURL()`: Extracts cluster path from URL
- `LookupShard()`: Finds shard hosting the cluster
- `NewReverseProxy()`: Creates proxy with TLS client cert

**Reference**: `pkg/proxy/handler.go:89`

#### 3. **Reverse Proxy** (`pkg/proxy/proxy.go`)

HTTP reverse proxy with authentication header propagation.

```
[Incoming Request]
      ↓
[Extract User Info from Context]
      ↓
[Add Authentication Headers]
  - X-Remote-User: username
  - X-Remote-Group: group1,group2
  - X-Remote-Extra-<key>: value
      ↓
[TLS Client Certificate] ← For shard authentication
      ↓
[Forward to Shard URL]
```

**Authentication Flow**:
1. Extract user info from request context (set by auth filter)
2. Convert to headers for downstream shard
3. Shard validates client certificate
4. Shard processes request with impersonation headers

**Reference**: `pkg/proxy/proxy.go:127`

#### 4. **Virtual Workspaces** (`pkg/virtual/`)

Transform or aggregate resources for different views.

**Examples**:
- **`apiexport`**: Aggregated view of all APIExports
- **`initializingworkspaces`**: View of workspaces being initialized
- **`syncer`**: Syncer-specific view with placement labels

**Pattern**:
```
[Client Request] → /services/initializingworkspaces/...
      ↓
[Virtual Workspace Handler] ← Transform to show only initializing workspaces
      ↓
[Filter Resources] ← status.phase == "Initializing"
      ↓
[Return Filtered View]
```

**Reference**: `pkg/virtual/framework/`

### Authentication & Authorization

#### **Authentication** (`pkg/server/options/authentication.go`)

**Methods**:
1. **Request Header Auth**: From reverse proxy headers
   - `X-Remote-User`: Username
   - `X-Remote-Group`: Groups (comma-separated)
   - `X-Remote-Extra-*`: Extra attributes

2. **Client Certificate**: TLS client cert authentication
3. **Token Authentication**: Bearer tokens
4. **Anonymous Auth**: Optional anonymous access

**Flow**:
```
[Request] → [Authenticator Chain] → [User Info]
                                        ↓
                                   [Authorization]
```

#### **Authorization** (`pkg/authorization/`)

Multi-layered authorization system:

1. **`workspace_content_authorizer`** (`workspace_content_authorizer.go:87`)
   - **Purpose**: Controls access to workspace contents
   - **Checks**: User has access to the workspace itself
   - **Scope**: All resources within workspace

2. **`maximal_permission_policy_authorizer`** (`maximal_permission_policy_authorizer.go:134`)
   - **Purpose**: Enforces permission claims from APIBindings
   - **Checks**: API provider's permission claims are respected
   - **Scope**: Resources claimed by API providers

3. **`requiredgroups_authorizer`** (`requiredgroups_authorizer.go`)
   - **Purpose**: Validates required group membership
   - **Checks**: User belongs to required groups
   - **Scope**: System-level access

4. **`delegated`** (`delegated.go`)
   - **Purpose**: Delegates to upstream authorization
   - **Use Case**: Multi-level delegation in workspace hierarchy

5. **`bootstrap_policy_authorizer`** (`bootstrap_policy_authorizer.go`)
   - **Purpose**: System bootstrap policies
   - **Scope**: Initial system setup and configuration

6. **`global_authorizer`** (`global_authorizer.go`)
   - **Purpose**: Root shard authorization
   - **Scope**: Cross-workspace and system-level operations

**Authorization Chain**:
```
[Request] → [Workspace Content Authorizer]
                      ↓ (if allowed)
            [Maximal Permission Policy]
                      ↓ (if allowed)
            [Required Groups]
                      ↓ (if allowed)
            [Bootstrap Policy / Global]
                      ↓
            [Allow/Deny Decision]
```

### Request Flow Example: Create Deployment

```
1. [Client] → POST /clusters/root:org:ws/apis/apps/v1/namespaces/default/deployments
      ↓
2. [Front Proxy] → Authenticate user, extract cluster path
      ↓
3. [Shard Lookup] → Find shard hosting "root:org:ws"
      ↓
4. [Reverse Proxy] → Proxy to shard-1 with auth headers
      ↓
5. [Shard Server] → Authenticate via client cert + headers
      ↓
6. [Authorization Chain] → Check workspace access, API permissions
      ↓
7. [Admission Control] → Validate deployment spec
      ↓
8. [API Server] → Write to etcd with cluster prefix: /deployments/root:org:ws/default/my-deploy
      ↓
9. [Placement Controller] → Watch new deployment, assign to SyncTarget
      ↓
10. [Label Deployment] → state.workload.kcp.io/target-cluster=Sync
      ↓
11. [Syncer Watches] → Sees label, creates deployment on physical cluster
      ↓
12. [Response to Client] ← Deployment created (201 Created)
```

---

## API Definitions & Custom Resources

### API Group Organization

KCP defines several API groups in `pkg/apis/`:

#### **1. `tenancy.kcp.io` (v1alpha1)**
**Resources**: Workspace, WorkspaceType
**Purpose**: Multi-tenant workspace management

**Workspace** (`pkg/apis/tenancy/v1alpha1/types_workspace.go:42`):
```go
type Workspace struct {
    Spec WorkspaceSpec       // Type, location, quotas
    Status WorkspaceStatus   // Phase, URL, conditions
}

type WorkspaceSpec struct {
    Type      WorkspaceTypeReference  // Reference to WorkspaceType
    Location  LocationReference       // Preferred location
    Cluster   string                  // Target cluster name
}

type WorkspaceStatus struct {
    Phase         WorkspacePhaseType   // Scheduling, Initializing, Ready
    URL           string                // API endpoint URL
    Conditions    []Condition
    Cluster       string                // Actual cluster name
}
```

**WorkspaceType** (`pkg/apis/tenancy/v1alpha1/types_workspacetype.go:38`):
```go
type WorkspaceType struct {
    Spec WorkspaceTypeSpec
}

type WorkspaceTypeSpec struct {
    DefaultAPIBindings  []APIExportReference  // Auto-bound APIs
    AdditionalWorkspaceLabels  map[string]string
    DefaultResourceQuota       *ResourceQuotaSpec
    Initializer               bool               // Requires initialization
    LimitAllowedParents       *WorkspaceTypeSelector
    LimitAllowedChildren      *WorkspaceTypeSelector
    DefaultChildWorkspaceType *WorkspaceTypeReference
}
```

#### **2. `apis.kcp.io` (v1alpha1)**
**Resources**: APIExport, APIBinding, APIResourceSchema, APIConversion, APIExportEndpointSlice
**Purpose**: API service provider and consumer management

**APIExport** (`pkg/apis/apis/v1alpha1/types_apiexport.go:53`):
```go
type APIExport struct {
    Spec APIExportSpec
    Status APIExportStatus
}

type APIExportSpec struct {
    LatestResourceSchemas []string              // Current schemas
    PermissionClaims      []PermissionClaim     // Required permissions
    MaximalPermissionPolicy *MaximalPermissionPolicy
}

type PermissionClaim struct {
    GroupResource GroupResource  // e.g., "deployments.apps"
    IdentityHash  string          // Immutable export identity
    All           bool            // Claim all verbs
    ResourceSelector []ResourceSelector
}

type APIExportStatus struct {
    IdentityHash           string  // Immutable once set
    VirtualWorkspaces      []VirtualWorkspace
    Conditions             []Condition
}
```

**APIBinding** (`pkg/apis/apis/v1alpha1/types_apibinding.go:48`):
```go
type APIBinding struct {
    Spec APIBindingSpec
    Status APIBindingStatus
}

type APIBindingSpec struct {
    Reference ExportBindingReference  // Which APIExport to bind
    PermissionClaims []AcceptablePermissionClaim  // Accepted claims
}

type ExportBindingReference struct {
    Export *ExportReference  // Path + export name
}

type APIBindingStatus struct {
    Phase                WorkspaceBindingPhaseType  // Binding, Ready
    BoundAPIExport       BoundAPIExport
    BoundResources       []BoundAPIResource  // Materialized APIs
}
```

**APIResourceSchema** (`pkg/apis/apis/v1alpha1/types_apiresourceschema.go:35`):
```go
type APIResourceSchema struct {
    Spec APIResourceSchemaSpec
}

type APIResourceSchemaSpec struct {
    Group   string
    Names   apiextensionsv1.CustomResourceDefinitionNames
    Scope   apiextensionsv1.ResourceScope
    Versions []APIResourceVersion
}

type APIResourceVersion struct {
    Name     string
    Served   bool
    Storage  bool
    Schema   apiextensionsv1.CustomResourceValidation  // OpenAPI v3
}
```

#### **3. `core.kcp.io` (v1alpha1)**
**Resources**: LogicalCluster, Shard
**Purpose**: Core infrastructure management

**LogicalCluster** (`pkg/apis/core/v1alpha1/logicalcluster_types.go:37`):
```go
type LogicalCluster struct {
    Spec LogicalClusterSpec
    Status LogicalClusterStatus
}

type LogicalClusterSpec struct {
    Owner               *LogicalClusterOwner
    DirectlyDeletable   bool
}

type LogicalClusterStatus struct {
    Phase         LogicalClusterPhaseType  // Scheduling, Initializing, Ready
    URL           string
    Conditions    []Condition
}
```

**Shard** (`pkg/apis/core/v1alpha1/shard_types.go:41`):
```go
type Shard struct {
    Spec ShardSpec
    Status ShardStatus
}

type ShardSpec struct {
    BaseURL             string  // e.g., https://shard-1.kcp.io
    ExternalURL         string  // Public-facing URL
    VirtualWorkspaceURL string
}

type ShardStatus struct {
    Capacity  corev1.ResourceList
    Conditions []Condition
}
```

#### **4. `workload.kcp.io` (v1alpha1)**
**Resources**: SyncTarget, Placement (deprecated - moved to scheduling.kcp.io)
**Purpose**: Workload synchronization and placement

**SyncTarget** (`pkg/apis/workload/v1alpha1/synctarget_types.go:52`):
```go
type SyncTarget struct {
    Spec SyncTargetSpec
    Status SyncTargetStatus
}

type SyncTargetSpec struct {
    Unschedulable   bool
    EvictAfter      *metav1.Time
    Cells           map[string]Cell
    SupportedAPIExports []string
}

type SyncTargetStatus struct {
    Capacity           corev1.ResourceList
    Allocatable        corev1.ResourceList
    SyncedResources    []ResourceToSync
    VirtualWorkspaces  []VirtualWorkspace
    LastSyncerHeartbeatTime *metav1.Time
    Conditions         []Condition
}

type ResourceToSync struct {
    GroupResource GroupResource
    Versions      []string
    IdentityHash  string  // APIExport identity
    State         ResourceToSyncState  // Accepted, Pending
}
```

#### **5. `scheduling.kcp.io` (v1alpha1)**
**Resources**: Location, Placement
**Purpose**: Workload placement decisions

**Location** (`pkg/apis/scheduling/v1alpha1/location_types.go:38`):
```go
type Location struct {
    Spec LocationSpec
    Status LocationStatus
}

type LocationSpec struct {
    Resource       LocationResource  // SyncTarget or Partition
    InstanceSelector *metav1.LabelSelector
    Description    string
}

type LocationStatus struct {
    AvailableInstances int32
    Conditions         []Condition
}
```

**Placement** (`pkg/apis/scheduling/v1alpha1/placement_types.go:42`):
```go
type Placement struct {
    Spec PlacementSpec
    Status PlacementStatus
}

type PlacementSpec struct {
    LocationSelectors   []metav1.LabelSelector
    NamespaceSelector   *metav1.LabelSelector
    LocationResource    string
    LocationWorkspace   string
}

type PlacementStatus struct {
    SelectedLocation *LocationReference
    Phase            PlacementPhaseType  // Pending, Bound
    Conditions       []Condition
}
```

#### **6. `topology.kcp.io` (v1alpha1)**
**Resources**: Partition, PartitionSet
**Purpose**: Advanced workload distribution

**PartitionSet** (`pkg/apis/topology/v1alpha1/partitionset_types.go:38`):
```go
type PartitionSet struct {
    Spec PartitionSetSpec
    Status PartitionSetStatus
}

type PartitionSetSpec struct {
    Dimensions []Dimension
    Count      *int32
    Selector   *metav1.LabelSelector
}

type Dimension struct {
    Name     string
    LabelKey string
}
```

#### **7. `apiresource.kcp.io` (v1alpha1)**
**Resources**: NegotiatedAPIResource, APIResourceImport
**Purpose**: API negotiation between KCP and sync targets

**NegotiatedAPIResource** (`pkg/apis/apiresource/v1alpha1/types_negotiatedapiresource.go:38`):
```go
type NegotiatedAPIResource struct {
    Spec NegotiatedAPIResourceSpec
}

type NegotiatedAPIResourceSpec struct {
    CommonAPIResourceSpec
    Publish bool
}

type CommonAPIResourceSpec struct {
    GroupVersion GroupVersion
    Scope        string
    CustomResourceDefinitionNames apiextensionsv1.CustomResourceDefinitionNames
    SubResources []APISubResourceSpec
    ColumnDefinitions []apiextensionsv1.CustomResourceColumnDefinition
}
```

#### **8. Kubernetes APIs (standard)**
Managed through APIExports and bindings:
- **Core** (v1): Pods, Services, ConfigMaps, Secrets, Namespaces, etc.
- **Apps** (v1): Deployments, StatefulSets, DaemonSets
- **Batch** (v1): Jobs, CronJobs
- **RBAC** (v1): Roles, RoleBindings, ClusterRoles, ClusterRoleBindings
- **Extensions**: CustomResourceDefinitions

### CRD Management

**CRD Lifecycle**:
1. **Definition**: CRDs defined in APIResourceSchemas
2. **Export**: Added to APIExport's LatestResourceSchemas
3. **Binding**: Consumer creates APIBinding to export
4. **Materialization**: APIBinding controller creates CRD in consumer workspace
5. **Usage**: Consumer can now create custom resources
6. **Cleanup**: CRD removed when APIBinding deleted

**CRD Storage**:
- Multiple versions of same CRD can exist across logical clusters
- Each logical cluster has independent CRD definitions
- Schema conversion handled by `APIConversion` resources

**Reference**: `pkg/reconciler/apis/apibinding/apibinding_controller.go:294`

---

## Admission Control

Located in `pkg/admission/`, KCP implements 30+ admission plugins.

### Admission Plugin Categories

#### **1. Validation Plugins**

Validate resource specifications before persistence.

- **`apibinding`** (`apibinding/apibinding_admission.go`): Validates APIBinding specs
  - Checks APIExport exists and is accessible
  - Validates permission claims are acceptable
  - Ensures no circular bindings

- **`apiexport`** (`apiexport/apiexport_admission.go`): Manages APIExport lifecycle
  - Validates exported schemas exist
  - Ensures identity hash is immutable
  - Validates permission claims are well-formed

- **`workspace`** (`workspace/workspace_admission.go`): Validates workspace creation
  - Checks WorkspaceType exists
  - Validates workspace name is unique
  - Ensures parent workspace allows child creation

- **`workspacetype`** (`workspacetype/workspacetype_admission.go`): Validates workspace type definitions
  - Checks default APIs are valid
  - Validates type hierarchy

- **`apiresourceschema`** (`apiresourceschema/apiresourceschema_admission.go`): Validates API schemas
  - Ensures OpenAPI v3 schema is valid
  - Checks version consistency

- **`crdnooverlappinggvr`** (`crdnooverlappinggvr/crdnooverlappinggvr_admission.go`): Prevents GVR conflicts
  - Ensures no two CRDs claim same GroupVersionResource
  - Critical for API uniqueness

#### **2. Mutation Plugins**

Modify resources before persistence.

- **`logicalcluster`** (`logicalcluster/logicalcluster_admission.go`): Manages finalizers
  - Adds finalizer for cleanup on deletion
  - Sets owner references

- **`permissionclaims`** (`permissionclaims/permissionclaims_admission.go`): Manages permission claims
  - Adds identity hash to claims
  - Labels resources with accepted claims

- **`reservednames`** (`reservednames/reservednames_admission.go`): Protects system resources
  - Prevents creation of resources with reserved names
  - Examples: `kcp-`, `system:`, `kube-`

- **`reservedmetadata`** (`reservedmetadata/reservedmetadata_admission.go`): Protects metadata
  - Prevents modification of system labels/annotations
  - Examples: `kcp.io/`, `internal.kcp.io/`

- **`reservedcrdannotations`** (`reservedcrdannotations/reservedcrdannotations_admission.go`): Protects CRD annotations
  - Prevents tampering with CRD system annotations

#### **3. Lifecycle Plugins**

Manage resource lifecycle and dependencies.

- **`finalizer`** (`finalizer/finalizer_admission.go`): General finalizer management
  - Adds finalizers for cleanup coordination

- **`logicalclusterfinalizer`** (`logicalclusterfinalizer/logicalclusterfinalizer_admission.go`): Logical cluster cleanup
  - Ensures all resources deleted before cluster removal

#### **4. Webhook Plugins**

External admission control via webhooks.

- **`mutatingwebhook`** (`mutatingwebhook/mutatingwebhook_admission.go`): External mutation
  - Calls external services to mutate resources

- **`validatingwebhook`** (`validatingwebhook/validatingwebhook_admission.go`): External validation
  - Calls external services to validate resources

#### **5. Quota & Resource Management**

- **`resourcequota`** (`resourcequota/resourcequota_admission.go`): Enforces quotas
  - Checks resource usage against workspace quotas
  - Rejects requests exceeding limits

### Admission Chain

Admission plugins run in order:

```
[API Request]
      ↓
[Mutation Phase]
  → reservedmetadata
  → logicalcluster (add finalizers)
  → permissionclaims (add labels)
  → mutatingwebhook
      ↓
[Validation Phase]
  → workspace
  → apibinding
  → apiexport
  → apiresourceschema
  → crdnooverlappinggvr
  → reservednames
  → resourcequota
  → validatingwebhook
      ↓
[Persistence to etcd]
```

**Plugin Registration**: `pkg/admission/plugins.go`

---

## Testing Structure

### Unit Tests

**Location**: Throughout `pkg/` directories
**Pattern**: `*_test.go` files alongside source code
**Count**: 118 test files

**Test Categories**:

1. **Controller Tests** (`*_controller_test.go`)
   - Test controller reconciliation logic
   - Mock informers and clients
   - Example: `pkg/reconciler/tenancy/workspace/workspace_controller_test.go`

2. **Reconciler Tests** (`*_reconcile_test.go`)
   - Test specific reconcile functions
   - Table-driven tests for different scenarios
   - Example: `pkg/reconciler/apis/apibinding/apibinding_reconcile_test.go`

3. **Admission Tests** (`*_admission_test.go`)
   - Test admission plugin logic
   - Validate/mutate scenarios
   - Example: `pkg/admission/workspace/workspace_admission_test.go`

4. **Helper/Utility Tests**
   - Test utility functions and helpers
   - Example: `pkg/indexers/indexers_test.go`

**Test Utilities**:
- `pkg/reconciler/committer/committer_test_helpers.go`: Test helpers for controllers
- Mock clients and informers generated by code-generator

### E2E Tests

**Location**: `test/e2e/`
**Count**: 50+ test files
**Framework**: Custom test framework in `test/e2e/framework/`

**Test Framework Components** (`test/e2e/framework/`):

1. **`kcp.go`**: KCP server setup and management
   - `StartKCPServer()`: Start test KCP instance
   - `StopKCPServer()`: Clean shutdown
   - Manages test data directory and etcd

2. **`workspaces.go`**: Workspace creation and management
   - `CreateWorkspace()`: Create test workspace
   - `DeleteWorkspace()`: Clean up workspace
   - `WaitForWorkspaceReady()`: Wait for workspace to initialize

3. **`bind.go`**: APIBinding helpers
   - `CreateAPIBinding()`: Bind to APIExport
   - `WaitForAPIBindingReady()`: Wait for binding to materialize

4. **`syncer.go`**: Syncer integration testing
   - `StartSyncer()`: Start test syncer
   - `CreateSyncTarget()`: Register physical cluster
   - Mock physical cluster for testing

5. **`kubectl.go`**: kubectl command execution
   - `Kubectl()`: Execute kubectl commands
   - Parse and validate output

6. **`users.go`**: User/authentication management
   - `CreateUser()`: Create test users
   - `ImpersonateUser()`: Test with different users

7. **`config.go`**: Configuration and fixture management
   - Load test fixtures
   - Manage test configurations

**Test Categories**:

#### **Workspace Management Tests**
- `workspacetype/controller_test.go`: WorkspaceType controller tests
- `workspace/workspace_test.go`: Workspace lifecycle tests
- `initialization/initialization_test.go`: Workspace initialization tests

#### **API Management Tests**
- `apibinding/apibinding_protected_test.go`: APIBinding protection tests
- `apibinding/maximalpermissionpolicy_authorizer_test.go`: Permission claim tests
- `apiexport/apiexport_test.go`: APIExport lifecycle tests

#### **Workload Tests**
- `placement/placement_test.go`: Placement controller tests
- `synctarget/synctarget_test.go`: SyncTarget management tests
- `syncer/syncer_test.go`: Syncer integration tests

#### **Infrastructure Tests**
- `garbagecollector/garbagecollector_test.go`: GC across clusters
- `cache/cache_server_test.go`: Cache server tests
- `shard/shard_test.go`: Shard management tests

#### **Feature Tests**
- `homeworkspaces/home_workspaces_test.go`: Home workspace feature
- `watchcache/watchcache_enabled_test.go`: Watch cache optimization
- `quota/quota_test.go`: Resource quota enforcement

**Running Tests**:
```bash
# Run all unit tests
make test

# Run E2E tests
make test-e2e

# Run specific E2E test
go test -v ./test/e2e/apibinding -run TestAPIBindingProtected
```

**Test Patterns**:

1. **Table-Driven Tests**: Most tests use table-driven approach
   ```go
   tests := []struct {
       name     string
       input    *Workspace
       expected WorkspacePhase
   }{
       {"case1", workspace1, PhaseReady},
       {"case2", workspace2, PhaseInitializing},
   }
   ```

2. **Eventually Assertions**: Wait for async operations
   ```go
   require.Eventually(t, func() bool {
       ws, _ := client.Get(ctx, name, metav1.GetOptions{})
       return ws.Status.Phase == PhaseReady
   }, 30*time.Second, 100*time.Millisecond)
   ```

3. **Cleanup**: Use `t.Cleanup()` for resource cleanup
   ```go
   t.Cleanup(func() {
       client.Delete(ctx, name, metav1.DeleteOptions{})
   })
   ```

---

## Key Directories

| Directory | Purpose | Key Files |
|-----------|---------|-----------|
| **`/cmd`** | Binary entry points (16 binaries) | `kcp/kcp.go`, `syncer/cmd/syncer.go` |
| **`/pkg/server`** | Core server implementation, routing, middleware | `server.go:127`, `handler.go:89` |
| **`/pkg/reconciler`** | All 44 controller implementations | Organized by domain (tenancy, workload, apis, etc.) |
| **`/pkg/apis`** | CRD definitions for KCP resources | `tenancy/`, `workload/`, `scheduling/`, `apis/` |
| **`/pkg/authorization`** | Authorization plugins and decorators | `workspace_content_authorizer.go:87` |
| **`/pkg/admission`** | 30+ admission control plugins | `workspace/`, `apibinding/`, `apiexport/` |
| **`/pkg/client`** | Generated client code (cluster-aware) | `clientset/`, `informers/`, `listers/` |
| **`/pkg/informer`** | Shared informer factories | `generic/`, `dynamic/` |
| **`/pkg/proxy`** | Reverse proxy, shard routing | `handler.go:89`, `proxy.go:127` |
| **`/pkg/syncer`** | Workload synchronization engine | `syncer.go`, `spec/`, `status/`, `upsync/` |
| **`/pkg/tunneler`** | Network tunneling for pod access | `tunneler.go` |
| **`/pkg/virtual`** | Virtual workspace implementations | `apiexport/`, `initializingworkspaces/`, `syncer/` |
| **`/pkg/metadata`** | Metadata-only clients (efficient watching) | `client.go` |
| **`/pkg/indexers`** | Custom indexers for resource lookups | `byworkspace.go`, `byshard.go` |
| **`/pkg/logging`** | Structured logging utilities | `logger.go` |
| **`/test/e2e`** | End-to-end test suites | `framework/`, `apibinding/`, `workspace/` |
| **`/test/e2e/framework`** | Shared test framework | `kcp.go`, `workspaces.go`, `syncer.go` |
| **`/config`** | Bootstrap configs, CRDs, RBAC | `crds/`, `bootstrap/`, `rbac/` |
| **`/docs`** | Architecture documentation | Design docs and ADRs |
| **`/hack`** | Build scripts, code generation | `update-codegen.sh`, `verify-*.sh` |
| **`/contrib`** | Community contributions, examples | Examples and tooling |

---

## Flow Examples

### Example 1: Workspace Creation Flow

```
1. [User] → kubectl kcp workspace create my-app --type kubernetes

2. [API Server] → Receives Workspace resource
      ↓
3. [Admission] → workspace admission plugin validates type exists
      ↓
4. [Persistence] → Write to etcd: /workspaces/root:org/my-app
      ↓
5. [Workspace Controller] → Reconcile (pkg/reconciler/tenancy/workspace/workspace_controller.go:181)
      ↓
   5a. Create LogicalCluster resource
   5b. Wait for LogicalCluster to be ready
   5c. Set workspace status.phase = Initializing
      ↓
6. [Initialization Controller] → Reconcile (pkg/reconciler/tenancy/initialization/)
      ↓
   6a. Read WorkspaceType "kubernetes"
   6b. Create APIBindings for default APIs (core, apps, batch, etc.)
   6c. Wait for all APIBindings to become Ready
   6d. Set workspace status.phase = Ready
      ↓
7. [APIBinding Controller] → For each APIBinding (pkg/reconciler/apis/apibinding/)
      ↓
   7a. Fetch APIExport from provider workspace
   7b. Materialize CRDs into workspace
   7c. Set up RBAC for API access
   7d. Update APIBinding status = Ready
      ↓
8. [Workspace Ready] → User can now create resources
      ↓
9. [User] → kubectl create deployment nginx --image=nginx
```

**Key Controllers Involved**:
- workspace_controller.go:181
- initialization_controller.go
- apibinding_controller.go:294

### Example 2: Workload Deployment Flow

```
1. [User] → kubectl create -f deployment.yaml (in workspace with SyncTarget)

2. [API Server] → Receives Deployment resource
      ↓
3. [Admission] → Validate deployment spec
      ↓
4. [Persistence] → Write to etcd: /deployments/root:org:ws/default/nginx
      ↓
5. [Placement Controller] → Reconcile (pkg/reconciler/workload/placement/placement_controller.go:215)
      ↓
   5a. Check namespace has Placement resource
   5b. Evaluate location selectors
   5c. Find compatible SyncTarget (check API support, capacity, labels)
   5d. Label Deployment: state.workload.kcp.io/target-cluster=Sync
      ↓
6. [Syncer (on physical cluster)] → Watch for label
      ↓
   6a. Sees state.workload.kcp.io/target-cluster=Sync
   6b. Transform resource (remove KCP labels, adjust namespace)
   6c. Create Deployment on physical cluster
      ↓
7. [Kubernetes (physical cluster)] → Create Pods
      ↓
   7a. Schedule pods
   7b. Start containers
   7c. Update Deployment status
      ↓
8. [Syncer Status Controller] → Watch physical Deployment
      ↓
   8a. Read Deployment.status from physical cluster
   8b. Sync status back to KCP
   8c. Update Deployment status in KCP workspace
      ↓
9. [User] → kubectl get deployment nginx (sees status from physical cluster)
```

**Key Components**:
- placement_controller.go:215
- syncer/spec/spec_controller.go (downstream sync)
- syncer/status/status_controller.go (upstream sync)
- Physical cluster Kubernetes components

**State Transitions**:
```
[Deployment Created] → phase=""
         ↓ (placement controller)
[Pending] → state.workload.kcp.io/target=""
         ↓ (placement decision)
[Sync] → state.workload.kcp.io/target="Sync"
         ↓ (syncer creates on cluster)
[Active] → status shows pods running
         ↓ (user deletes)
[Deleted] → state.workload.kcp.io/target="Delete"
         ↓ (syncer removes from cluster)
[Finalizer Removed] → resource removed from etcd
```

### Example 3: API Provider Integration Flow

```
1. [Provider] → Create APIExport in provider workspace

   apiVersion: apis.kcp.io/v1alpha1
   kind: APIExport
   metadata:
     name: my-api
   spec:
     latestResourceSchemas:
     - v1.widgets
     permissionClaims:
     - group: ""
       resource: "configmaps"
       all: true

2. [APIExport Controller] → Reconcile (pkg/reconciler/apis/apiexport/apiexport_controller.go:187)
      ↓
   2a. Validate schemas exist
   2b. Generate identity hash (immutable)
   2c. Set up virtual workspace URLs
   2d. Update status.identityHash
      ↓
3. [Consumer] → Create APIBinding in consumer workspace

   apiVersion: apis.kcp.io/v1alpha1
   kind: APIBinding
   metadata:
     name: provider-api
   spec:
     reference:
       export:
         path: root:provider-workspace
         name: my-api

4. [APIBinding Controller] → Reconcile (pkg/reconciler/apis/apibinding/apibinding_controller.go:294)
      ↓
   4a. Resolve APIExport reference (path + name)
   4b. Fetch APIExport from provider workspace
   4c. Check permission claims
   4d. Create CRD for "widgets" in consumer workspace
   4e. Set up RBAC for widgets API
   4f. Label widgets with permission claim info
   4g. Update APIBinding status.phase = Ready
      ↓
5. [Consumer] → Create widget in consumer workspace

   apiVersion: provider.io/v1
   kind: Widget
   metadata:
     name: my-widget
   spec:
     color: blue

6. [API Server] → Validates against CRD materialized by APIBinding

7. [Permission Claim Authorizer] → Check if provider has access
      ↓
   7a. Provider claimed "configmaps" access
   7b. Consumer accepted claim
   7c. Allow provider to read/write ConfigMaps in consumer workspace
      ↓
8. [Provider Controller] → Watch for widgets in consumer workspaces
      ↓
   8a. Sees new widget
   8b. Create ConfigMap with widget config (uses permission claim)
   8c. Perform business logic
   8d. Update widget status
      ↓
9. [Consumer] → kubectl get widget my-widget (sees status from provider)
```

**Key Security Points**:
- Provider must explicitly declare permission claims
- Consumer must explicitly accept permission claims
- maximal_permission_policy_authorizer enforces claims
- Identity hash prevents APIExport identity spoofing

**Reference**:
- apibinding_controller.go:294
- apiexport_controller.go:187
- maximal_permission_policy_authorizer.go:134

---

## Key Design Patterns

### 1. Logical Cluster Prefix Pattern

All resources in etcd are prefixed with their logical cluster name.

**Pattern**: `/<resource>/<cluster>/<namespace>/<name>`

**Example**:
```
/deployments/root:org:team/default/nginx
/configmaps/root:org:team/app/config
/workspaces/root:org/team
```

**Implementation**: `pkg/apis/core/v1alpha1/logicalcluster.go`

**Benefits**:
- Complete isolation between logical clusters
- Single etcd instance for all clusters
- Efficient storage and lookup
- No cross-cluster access without explicit permission

### 2. State Machine via Labels

Resource sync state managed through labels for async coordination.

**Pattern**:
```yaml
labels:
  state.workload.kcp.io/<sync-target-name>: "Sync"  # or Upsync, Pending
  deletion.internal.workload.kcp.io/<sync-target-name>: "1234567890"
  finalizers.workload.kcp.io/<sync-target-name>: ""
```

**States**:
- **Pending**: Waiting for placement decision
- **Sync**: Syncing from KCP → cluster (normal mode)
- **Upsync**: Syncing from cluster → KCP (cluster-originated resources)
- **Delete**: Resource being deleted

**Benefits**:
- Multiple controllers can coordinate without direct communication
- Idempotent operations
- Clear state transitions
- Easy debugging (labels visible in kubectl)

**Reference**: `pkg/syncer/resourcesync/resourcesync.go`

### 3. Work Queue Pattern

All controllers use rate-limited work queues for reconciliation.

**Pattern**:
```go
type Controller struct {
    queue workqueue.RateLimitingInterface
    informer cache.SharedIndexInformer
}

func (c *Controller) Run(ctx context.Context) {
    go c.informer.Run(ctx.Done())
    go wait.Until(c.worker, time.Second, ctx.Done())
}

func (c *Controller) worker() {
    for c.processNextWorkItem() {
    }
}

func (c *Controller) processNextWorkItem() bool {
    key, quit := c.queue.Get()
    if quit {
        return false
    }
    defer c.queue.Done(key)

    err := c.reconcile(key)
    if err != nil {
        c.queue.AddRateLimited(key)  // Retry with backoff
    } else {
        c.queue.Forget(key)
    }
    return true
}
```

**Benefits**:
- Automatic retry with exponential backoff
- Rate limiting prevents overwhelming API server
- Deduplication (multiple events → single reconcile)
- Graceful error handling

**Reference**: All controllers in `pkg/reconciler/`

### 4. Shared Informer Factories

Efficient watching with shared caches across controllers.

**Pattern**:
```go
// Single informer shared by all controllers
factory := informers.NewSharedInformerFactory(client, 10*time.Minute)
deploymentInformer := factory.Apps().V1().Deployments()

// Multiple controllers use same informer
placementController := placement.NewController(deploymentInformer, ...)
resourceController := resource.NewController(deploymentInformer, ...)

// Start informer once
factory.Start(ctx.Done())
```

**Benefits**:
- Single watch per resource type (not per controller)
- Reduced API server load
- Lower memory usage
- Consistent view across controllers

**Reference**: `pkg/informer/generic/generic.go`

### 5. Virtual Clusters

Transform data for different consumption patterns without duplicating storage.

**Pattern**:
```go
type VirtualWorkspace interface {
    // Transform list requests
    List(ctx context.Context, options *metav1.ListOptions) (runtime.Object, error)

    // Transform get requests
    Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error)
}
```

**Examples**:
- **initializingworkspaces**: Show only workspaces in Initializing phase
- **syncer**: Show only resources with specific placement label
- **apiexport**: Aggregate view of all APIExports

**Benefits**:
- Same storage, multiple views
- No data duplication
- Efficient filtering and transformation
- Custom access patterns per use case

**Reference**: `pkg/virtual/framework/`

### 6. Cluster-Aware Clients

Clients can operate on multiple clusters concurrently.

**Pattern**:
```go
import "github.com/kcp-dev/logicalcluster/v3"

// Create cluster-aware client
cluster := logicalcluster.Name("root:org:team")
client := client.Cluster(cluster.Path())

// Operations scoped to specific cluster
deployment, err := client.AppsV1().Deployments("default").Get(ctx, "nginx", metav1.GetOptions{})
```

**Benefits**:
- Single client can access multiple logical clusters
- Type-safe cluster scoping
- No need to create separate clients per cluster
- Efficient connection pooling

**Reference**: `pkg/client/clientset/versioned/cluster/`

### 7. Reverse Proxy for Sharding

Transparent routing to appropriate shard without client knowledge.

**Pattern**:
```
[Client Request] → /clusters/root:org:ws/apis/apps/v1/deployments
      ↓
[Front Proxy] → Parse cluster path "root:org:ws"
      ↓
[Index Lookup] → Find shard hosting cluster
      ↓
[Reverse Proxy] → https://shard-1.kcp.io/clusters/root:org:ws/...
```

**Benefits**:
- Clients unaware of sharding
- Dynamic shard assignment
- Fault isolation per shard
- Horizontal scalability

**Reference**: `pkg/proxy/handler.go:89`

### 8. Permission Claims

Explicit permission model for API provider access to consumer resources.

**Pattern**:
```yaml
# Provider declares needed permissions
apiVersion: apis.kcp.io/v1alpha1
kind: APIExport
metadata:
  name: my-api
spec:
  permissionClaims:
  - group: ""
    resource: "configmaps"
    all: true  # Read/write access

# Consumer explicitly accepts
apiVersion: apis.kcp.io/v1alpha1
kind: APIBinding
metadata:
  name: provider-api
spec:
  permissionClaims:
  - group: ""
    resource: "configmaps"
    identityHash: "abc123"
    state: Accepted
```

**Authorization Flow**:
```
[Provider Controller Request]
      ↓
[maximal_permission_policy_authorizer]
      ↓
   Check: Permission claimed? → No → Deny
      ↓
   Check: Claim accepted? → No → Deny
      ↓
   Check: Resource matches claim? → Yes → Allow
```

**Benefits**:
- Explicit, auditable permissions
- Consumer control over provider access
- Fine-grained access control
- No implicit trust relationships

**Reference**: `pkg/authorization/maximal_permission_policy_authorizer.go:134`

### 9. Informer Index Pattern

Custom indexes for efficient resource lookups.

**Pattern**:
```go
// Add custom index
indexer.AddIndexers(cache.Indexers{
    "byWorkspace": func(obj interface{}) ([]string, error) {
        deployment := obj.(*appsv1.Deployment)
        cluster := logicalcluster.From(deployment)
        return []string{cluster.String()}, nil
    },
})

// Lookup by index
deployments, err := indexer.ByIndex("byWorkspace", "root:org:team")
```

**Common Indexes**:
- `byWorkspace`: Resources by logical cluster
- `byShard`: Resources by shard
- `byAPIExport`: APIBindings by referenced export
- `bySyncTarget`: Resources by placement target

**Benefits**:
- O(1) lookup instead of O(n) scan
- Efficient queries across large datasets
- Reduced memory allocation

**Reference**: `pkg/indexers/`

### 10. Finalizer Coordination

Manage cleanup dependencies across resources and controllers.

**Pattern**:
```go
// Controller adds finalizer
if !hasFinalizer(obj, MyFinalizer) {
    obj.Finalizers = append(obj.Finalizers, MyFinalizer)
    client.Update(ctx, obj)
}

// On deletion
if !obj.DeletionTimestamp.IsZero() {
    // Perform cleanup
    err := cleanup(obj)
    if err != nil {
        return err  // Retry
    }

    // Remove finalizer
    obj.Finalizers = removeString(obj.Finalizers, MyFinalizer)
    client.Update(ctx, obj)
}
```

**Finalizers**:
- `workload.kcp.io/<sync-target>`: Ensure syncer cleanup
- `apis.kcp.io/apibinding-finalizer`: Clean up materialized CRDs
- `tenancy.kcp.io/logicalcluster`: Ensure all resources deleted

**Benefits**:
- Guaranteed cleanup order
- Prevent orphaned resources
- Coordination between multiple controllers
- Graceful degradation

**Reference**: Throughout `pkg/reconciler/` controllers

---

## Technologies & Dependencies

### Core Technologies

- **Kubernetes (fork)**: Base API server, client libraries, controller patterns
  - Forked to add logical cluster support
  - Version: Kubernetes 1.24+

- **etcd**: Primary storage backend
  - Logical cluster prefixing for multi-tenancy
  - Version: etcd 3.5+

- **Go**: Implementation language
  - Version: Go 1.19+

- **gRPC**: Component communication
  - Used for internal service communication

### Key Libraries

- **`logicalcluster/v3`**: Cluster-aware path handling
  - `github.com/kcp-dev/logicalcluster/v3`
  - Core abstraction for logical cluster names

- **`client-go`**: Kubernetes client library (modified)
  - Extended with cluster-awareness

- **`apiextensions-apiserver`**: CRD support
  - Manages custom resource definitions

- **`code-generator`**: Client code generation
  - Generates clientsets, informers, listers from CRDs

- **`controller-runtime`**: Controller utilities (partial)
  - Some utilities used, but KCP uses custom controller framework

- **`klog/v2`**: Structured logging
  - Standard Kubernetes logging library

- **`cobra`**: CLI framework
  - Used for all CLI commands (`kcp`, `syncer`, `kubectl-kcp`)

- **`go-restful`**: API routing
  - HTTP API routing framework

### Build & Development

- **`make`**: Build system
  - Targets: `build`, `test`, `test-e2e`, `verify`, `update-codegen`

- **`code-generator`**: Generates clients, informers, listers
  - Script: `hack/update-codegen.sh`

- **`openapi-gen`**: Generates OpenAPI specs
  - For API documentation

- **`golangci-lint`**: Linting
  - Code quality checks

### Testing

- **`testing`**: Standard Go testing
- **`testify`**: Assertion library
  - `require` and `assert` packages
- **`gomock`**: Mocking framework (selective use)
- Custom test framework in `test/e2e/framework`

---

## Summary

KCP is a sophisticated multi-tenant Kubernetes control plane that provides:

1. **Workspaces**: Isolated environments sharing infrastructure
2. **API Management**: Provider/consumer model for API services
3. **Workload Distribution**: Transparent placement across physical clusters
4. **Strong Multi-Tenancy**: Complete isolation via logical clusters
5. **Horizontal Scalability**: Sharding support for massive scale

**Key Innovations**:
- Logical cluster storage model (single etcd, many clusters)
- Label-based state machine for async coordination
- Permission claims for fine-grained API provider access
- Virtual workspaces for efficient data transformation
- Transparent workload scheduling to physical clusters

**Architecture Highlights**:
- 44 controllers managing all aspects of the system
- Multi-layered authorization and admission control
- Bidirectional syncer for physical cluster integration
- Shard-aware reverse proxy for request routing
- Comprehensive testing at unit and E2E levels

This architecture enables KCP to scale to tens of thousands of workspaces while maintaining Kubernetes API compatibility and providing strong isolation guarantees.

---

**Document Version**: 1.0
**Last Updated**: 2025-10-29
**KCP Version**: Based on latest main branch analysis
