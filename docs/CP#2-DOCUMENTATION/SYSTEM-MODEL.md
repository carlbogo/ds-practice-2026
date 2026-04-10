# System Model

## Architectural Model

The system uses a **hybrid architectural model**. The client-facing part follows a client-server pattern: the browser sends HTTP POST requests to a centralized Flask orchestrator, which then coordinates three backend gRPC services — transaction verification (port 50052), fraud detection (port 50051), and suggestions (port 50053). Each service handles a specific domain and communicates only through the orchestrator, never directly with each other. This part of the system is strictly client-server.

The order execution layer, however, follows a **peer-to-peer** pattern. Three order executor replicas (all on port 50055) communicate directly with each other via gRPC to run the Bully leader election algorithm. They exchange Election, Coordinator, and Heartbeat messages without any central authority. The elected leader pulls orders from a shared order queue service (port 50054). Replicas discover each other through Docker DNS round-robin resolution of the service name.

## Communication Model

All inter-process communication uses **message passing** — there is no shared memory between services. The orchestrator makes synchronous gRPC calls to backend services during order validation and receives responses containing vector clock data for causal ordering. Some of these calls run in parallel using a thread pool (e.g., CheckItems and CheckUserAndBillingData execute concurrently), while others run sequentially when there are causal dependencies (e.g., CheckCardFraud depends on CheckPaymentData's result).

After validation, the orchestrator enqueues approved orders to the order queue via a gRPC call and returns a response to the client immediately, without waiting for order execution. The leader executor polls the queue every 2 seconds — this decouples order approval from execution. Executor-to-executor communication for leader election is also gRPC-based and bidirectional.

## Timing Model

The system is **partially asynchronous**. Under normal operation, all gRPC calls complete within known time bounds: the orchestrator uses a 3-second timeout for all RPC calls, executors use a 2-second election timeout, and heartbeats are sent every 3 seconds. The system relies on these timeouts to detect failures — if a heartbeat to the leader times out, followers assume the leader has crashed and start a new election.

## Failure Model

The system assumes **crash-recovery** node failures — services do not exhibit Byzantine behavior, but they may crash and restart at any time, losing their in-memory state. Docker handles restarting crashed containers. When an executor restarts, it re-discovers peers via DNS and rejoins the election protocol automatically. The Bully algorithm ensures a new leader is elected within seconds if the current leader goes down, so order processing resumes without manual intervention. The order queue stores data in memory (`collections.deque`), so persistence would need to be added for production use.

For links, the system assumes **reliable links**. All communication runs over gRPC (HTTP/2 over TCP), where TCP handles retransmission and de-duplication at the transport layer. The gRPC timeouts (2–3 seconds) serve to detect unresponsive nodes rather than link failures.
