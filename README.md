# RzRouter

A purpose-built TCP router for the Roomzin platform. It routes client requests from SDKs and HTTP proxies to the correct Roomzin shards based on the request's segment (city or property group), without clients needing to know where data is physically located.

## What It Does

RzRouter operates in two layers:

- **Edge Router**: Sits at the platform boundary, routes requests from clients to the appropriate zone router based on the segment
- **Zone Router**: Routes requests from edge routers to the correct shard bridge within a zone

This two-tier design enables global deployments with shards distributed across zones and regions, providing high availability and resilience.

## How It Fits in Roomzin

```
Client SDK ──┐
             │
HTTP Proxy ──┼──► Edge Router ──► Zone Router ──► Bridge ──► Shard
             │        │               │              │
Other SDKs ──┘        │               │              │
                      ▼               ▼              ▼
                    RzID ◄────────────┴──────────────┘
                    (Service Registry)
```

### Request Flow

1. **Client request arrives** via Roomzin SDKs or HTTP Proxy
2. **Edge Router** reads the segment (e.g., `london`, `nyc`) from the TCP frame header
3. **Edge Router** forwards to the **Zone Router** responsible for that segment's zone
4. **Zone Router** forwards to the **Bridge** connected to the correct shard
5. **Bridge** sends the request to the Roomzin shard

Clients only need to know the segment. The routing layer handles everything else.

## Why It Exists

Roomzin operates many shards across different zones worldwide. Clients should not need to know:

- Which zone a segment belongs to
- Which shard owns that segment's data
- Where shards are physically located
- How shards are distributed across zones

RzRouter abstracts all of this. The only client requirement is sending the segment in the request header.

This also means operations teams do not need to manage service registries, load balancer configurations, or complex routing rules. Components register themselves with RzID, and routers automatically discover the topology.

## Infrastructure Independence

The routing architecture decouples identities from infrastructure:

- **RzID** maintains the logical topology (zones, shards, segments, routers, bridges)
- **RzPoint** resolves logical IDs to actual hostnames - this is a service implemented by the company based on their own infrastructure
- **Routers and bridges** only need to know logical IDs

Infrastructure changes (new instances, IP changes, scaling) do not require router reconfiguration. The resolver service translates IDs to the current infrastructure state.

## Components

### RzID (Service Registry)
Central source of truth for the routing topology. Stores which segments belong to which zones, which shards exist in each zone, and which routers and bridges are active.

### RzPoint (Resolver)
Translates component IDs (router IDs, bridge IDs) to actual hostnames. Decouples routing logic from infrastructure details.

### Edge Router
- Accepts client TCP connections
- Reads segment from the request frame
- Routes to the appropriate zone router
- Runs in **edge** mode

### Zone Router
- Accepts connections from edge routers
- Routes to the correct bridge for the shard
- Runs in **zone** mode

### Bridge
- Connects directly to a Roomzin shard
- Handles the final hop from the routing layer to the database tier

## Running a Router

### Prerequisites

- **RzID** service must be running and reachable
- **RzPoint** resolver must be running and reachable
- For zone mode: `zone_id` and `router_id` must be registered with RzID

### Configuration

```yaml
# rzrouter.yml
mode: edge                    # edge or zone
listen_host: "0.0.0.0"
tcp_port: 9000
rzid_addr: "rzid.internal:8080"
rzpoint_addr: "rzpoint.internal:8081"
zone_id: "eu"                 # Required for zone mode
router_id: "router-1"         # Required for zone mode
```

### Starting

```bash
./rzrouter --config rzrouter.yml
```


## Deployment

### Edge Routers
- Deployed behind the platform's load balancer
- Clients connect to a single address; load balancer distributes to the nearest edge router
- Requires no client-side routing configuration

### Zone Routers
- Deployed within each zone
- Register themselves with RzID
- Edge routers discover them automatically

### Registration
All components register with RzID:

- **Routers**: Register with their `router_id` and `zone_id`
- **Bridges**: Register with their `bridge_id`, `shard_id`, and `zone_id`
- **Shards**: Register their segment ownership

Routers discover the topology by polling RzID for updates.

## Network Topology

```
                   ┌─────────────────────────────────────────────┐
                   │              Global Load Balancer          │
                   └─────────────────┬───────────────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
                    ▼                ▼                ▼
              ┌──────────┐    ┌──────────┐    ┌──────────┐
              │ Edge     │    │ Edge     │    │ Edge     │
              │ Router   │    │ Router   │    │ Router   │
              └────┬─────┘    └────┬─────┘    └────┬─────┘
                   │               │               │
         ┌─────────┼───────────────┼───────────────┼─────────┐
         │         │               │               │         │
         ▼         ▼               ▼               ▼         ▼
    ┌────────┐ ┌────────┐    ┌────────┐    ┌────────┐ ┌────────┐
    │ Zone   │ │ Zone   │    │ Zone   │    │ Zone   │ │ Zone   │
    │ Router │ │ Router │    │ Router │    │ Router │ │ Router │
    └───┬────┘ └───┬────┘    └───┬────┘    └───┬────┘ └───┬────┘
        │          │              │              │          │
        ▼          ▼              ▼              ▼          ▼
    ┌────────┐ ┌────────┐    ┌────────┐    ┌────────┐ ┌────────┐
    │ Bridge │ │ Bridge │    │ Bridge │    │ Bridge │ │ Bridge │
    └───┬────┘ └───┬────┘    └───┬────┘    └───┬────┘ └───┬────┘
        │          │              │              │          │
        └──────────┴──────────────┴──────────────┴──────────┘
                                     │
                                     ▼
                              ┌─────────────┐
                              │   Shards    │
                              └─────────────┘
```

## Monitoring

Routers expose:

- **Logs**: Structured JSON logs via `RUST_LOG` environment variable
- **Metrics**: Prometheus metrics available at `/metrics` endpoint (if enabled)
- **Health**: `/health` endpoint for readiness/liveness probes

## Build

```bash
# Standard build
cargo build --release

# With metrics support
cargo build --features metrics --release

# Static binary (musl)
cargo build --target x86_64-unknown-linux-musl --release
```

## Environment

The router is designed for:

- **Linux**: Primary platform (containers, VMs)
- **High concurrency**: Thousands of concurrent TCP connections
- **Low latency**: Sub-millisecond forwarding
- **High throughput**: 10,000+ requests per second per instance

## Support

For issues with routing, start by checking:

1. **RzID connectivity**: `curl http://rzid:8080/health`
2. **RzPoint connectivity**: `curl http://rzpoint:8081/health`  
3. **Component registration**: Verify the router is registered with RzID
4. **Segment ownership**: Confirm the segment exists in RzID
5. **Logs**: Check router logs for routing failures
```