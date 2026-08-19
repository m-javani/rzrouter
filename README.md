# RzRouter

A purpose-built TCP router for the [Roomzin platform](https://m-javani.github.io/roomzin-doc/). It routes client requests from SDKs and HTTP proxies to the correct Roomzin shards based on the request's segment (city or property group), without clients needing to know where data is physically located.

## What It Does

RzRouter operates in two layers:

- **Edge Router**: Sits at the platform boundary, routes requests from clients to the appropriate zone router based on the segment
- **Zone Router**: Routes requests from edge routers to the correct shard bridge within a zone

This two-tier design enables global deployments with shards distributed across zones and regions, providing high availability and resilience.

## How It Fits in Roomzin

```
Client SDK ───┐
              │
HTTP Proxy ───┼───► Edge Router ───► Zone Router ───► Bridge ───► Shard
              │        │               │              │         │
Other SDKs ───┘        │               │              │         │
                       ▼               ▼              ▼         ▼
                    RzID ◄─────────────┴──────────────┴─────────┘
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


### Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | `edge` | Router mode: `edge` or `zone` |
| `--listen-host` | `0.0.0.0` | TCP listen host |
| `--tcp-port` | `9000` | TCP port to listen on |
| `--api-listening-addr` | `0.0.0.0` | HTTP API listen host |
| `--api-port` | `9100` | HTTP API port |
| `--rzid-addr` | (required) | RzID service address (e.g., `localhost:8080`) |
| `--rzpoint-addr` | (required) | RzPoint resolver address (e.g., `localhost:8081`) |
| `--zone-id` | (required for zone) | Zone ID |
| `--router-id` | (required for zone) | Router ID |
| `--max-connections` | `10000` | Maximum concurrent TCP connections |
| `--conn-per-hop` | `4` | Number of connections per hop |
| `--hop-tcp-port` | `9000` | Hop TCP port |
| `--refresh-interval-secs` | `10` | RzID refresh interval in seconds |
| `--heartbeat-interval-secs` | `30` | Heartbeat interval in seconds |
| `--request-timeout-secs` | `30` | Request timeout in seconds |
| `--worker-threads` | `4` | Number of worker threads |
| `--app-keepalive-secs` | `15` | Application-level keepalive interval |
| `--frame-timeout-secs` | `20` | Max time for a complete frame |
| `--idle-timeout-secs` | `90` | Absolute idle timeout |
| `--max-buffer-size` | `262144` | Max receive buffer per connection (bytes) |
| `--max-frame-size` | `16384` | Max single frame size (bytes) |

### Example Usage

**Edge Router:**
```bash
rzrouter \
  --mode edge \
  --listen-host 0.0.0.0 \
  --tcp-port 9000 \
  --api-port 9100 \
  --rzid-addr rzid.internal:8080 \
  --rzpoint-addr rzpoint.internal:8081 
```

**Zone Router:**
```bash
rzrouter \
  --mode zone \
  --zone-id zone-us-east \
  --router-id router-zone-us-east-01 \
  --listen-host 0.0.0.0 \
  --tcp-port 9000 \
  --api-port 9100 \
  --rzid-addr rzid.internal:8080 \
  --rzpoint-addr rzpoint.internal:8081 
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
                   ┌─────────────────────────────────────────┐
                   │         Global Load Balancer           │
                   └─────────────────┬───────────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
                    ▼                ▼                ▼
              ┌───────────┐    ┌───────────┐    ┌───────────┐
              │ Edge      │    │ Edge      │    │ Edge      │
              │ Router    │    │ Router    │    │ Router    │
              └─────┬─────┘    └─────┬─────┘    └─────┬─────┘
                    │               │               │
         ┌──────────┼───────────────┼───────────────┼──────────┐
         │          │               │               │          │
         ▼          ▼               ▼               ▼          ▼
    ┌─────────┐ ┌─────────┐    ┌─────────┐    ┌─────────┐ ┌─────────┐
    │ Zone    │ │ Zone    │    │ Zone    │    │ Zone    │ │ Zone    │
    │ Router  │ │ Router  │    │ Router  │    │ Router  │ │ Router  │
    └────┬────┘ └────┬────┘    └────┬────┘    └────┬────┘ └────┬────┘
         │           │              │              │           │
         ▼           ▼              ▼              ▼           ▼
    ┌─────────┐ ┌─────────┐    ┌─────────┐    ┌─────────┐ ┌─────────┐
    │ Bridge  │ │ Bridge  │    │ Bridge  │    │ Bridge  │ │ Bridge  │
    └────┬────┘ └────┬────┘    └────┬────┘    └────┬────┘ └────┬────┘
         │           │              │              │           │
         └───────────┴──────────────┼──────────────┴───────────┘
                                    │
                                    ▼
                             ┌───────────────┐
                             │   Shards      │
                             └───────────────┘
```

## Monitoring

### Health Check

```
GET /health
Response: 200 OK
```

### Metrics

Routers expose Prometheus metrics at `/metrics` endpoint (port configured via `--api-port`).

| Metric | Type | Description |
|--------|------|-------------|
| `router_connections_opened_total` | Counter | Total connections opened |
| `router_connections_closed_total` | Counter | Total connections closed |
| `router_frames_received_total` | Counter | Total frames received |
| `router_frames_forwarded_total` | Counter | Total frames forwarded |
| `router_bytes_received_total` | Counter | Total bytes received |
| `router_bytes_sent_total` | Counter | Total bytes sent |
| `router_client_errors_total` | Counter | Client-side errors |
| `router_unknown_segment_total` | Counter | Unknown segment errors |
| `router_network_errors_total` | Counter | Network-related errors |
| `router_timeouts_total` | Counter | Timeout events |
| `router_internal_errors_total` | Counter | Internal server errors |
| `router_keepalives_sent_total` | Counter | Keepalive messages sent |
| `router_keepalives_received_total` | Counter | Keepalive messages received |
| `router_resyncs_total` | Counter | Resynchronization events |

---

## Contributing

Contributions are welcome!

Please open an issue before proposing large changes. All contributions are subject to the BUSL-1.1 License terms.

---

## License

This project is licensed under the [BUSL-1.1 License](LICENSE).

**Note:** RzProxy is designed to communicate with Roomzin Server, which requires a valid Roomzin license.

---

## Support

- **Community Q&A**: [GitHub Discussions](https://github.com/m-javani/roomzin-doc/discussions)
- **Issues**: [GitHub Issues](https://github.com/m-javani/rzrouter/issues)

---

## Related Repositories

- [Roomzin](https://m-javani.github.io/roomzin-doc/) - Roomzin Documents
- [RzRouter](https://github.com/m-javani/rzrouter) - Routing fabric
- [RzID](https://github.com/m-javani/rzid) - Roomzin Service Registry
- [RzProxy](https://github.com/m-javani/rzproxy) - HTTP/JSON proxy
- [Roomzin Quickstart](https://github.com/m-javani/roomzin-quickstart) — Local Docker cluster
- [Roomzin Bench](https://github.com/m-javani/roomzin-bench) — Benchmarking tool