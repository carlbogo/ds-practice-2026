# System Model: Distributed Order Processing System

## Architecture Type

**Microservices Architecture with Message-Oriented Middleware**

The system follows a distributed microservices pattern where independent services communicate via gRPC (Remote Procedure Call) and coordinate through a centralized message queue. The architecture separates concerns into distinct services, each responsible for a specific domain function.

## System Components

### 1. **Orchestrator Service** (Port 5001)
- **Technology**: Flask (HTTP REST API)
- **Role**: Central coordinator and entry point
- **Responsibilities**:
  - Receives incoming checkout requests
  - Coordinates workflow across validation services
  - Manages vector clocks for causal ordering
  - Enqueues approved orders to the Order Queue
- **Communication**: HTTP (client-facing), gRPC (service-to-service)

### 2. **Fraud Detection Service** (Port 50051)
- **Technology**: gRPC Python service
- **Role**: Security validation
- **Responsibilities**:
  - Analyzes transaction patterns for fraudulent behavior
  - Returns approval/rejection decisions
  - Maintains its own vector clock
- **Communication**: gRPC (bidirectional with Orchestrator)

### 3. **Transaction Verification Service** (Port 50050)
- **Technology**: gRPC Python service
- **Role**: Payment validation
- **Responsibilities**:
  - Verifies credit card details
  - Validates billing information
  - Checks transaction feasibility
- **Communication**: gRPC (bidirectional with Orchestrator)

### 4. **Suggestions Service** (Port 50053)
- **Technology**: gRPC Python service with LLM integration
- **Role**: Recommendation engine
- **Responsibilities**:
  - Generates personalized book recommendations using LLM
  - Enhances user experience with AI-powered suggestions
- **Communication**: gRPC (with Orchestrator), external LLM API

### 5. **Order Queue Service** (Port 50054)
- **Technology**: gRPC Python service with in-memory queue
- **Role**: Message broker for order processing
- **Responsibilities**:
  - Buffers approved orders in FIFO queue
  - Provides Enqueue/Dequeue operations
  - Thread-safe access via locks
  - Maintains vector clock for queue operations
- **Communication**: gRPC (with Orchestrator and Executors)
- **Data Structure**: Python `collections.deque` (double-ended queue)

### 6. **Order Executor Service** (Port 50055, 3 replicas)
- **Technology**: gRPC Python service with Bully leader election
- **Role**: Order execution with fault tolerance
- **Responsibilities**:
  - Processes orders from the queue
  - Implements distributed leader election (Bully algorithm)
  - Executes orders (currently logs, extensible for real fulfillment)
  - Monitors leader health via heartbeats
- **Communication**:
  - gRPC with Order Queue (dequeue operations)
  - gRPC peer-to-peer (election, coordinator, heartbeat messages)
- **Replication**: 3 replicas for high availability

## Communication Patterns

### Synchronous Communication (gRPC)
- **Request-Response**: All inter-service communication uses gRPC
- **Timeout Handling**: 2-3 second timeouts with exponential backoff
- **Service Discovery**: Docker DNS for replica discovery
- **Protocol**: Protocol Buffers for message serialization

### Asynchronous Communication (Queue-based)
- **Order Enqueueing**: Fire-and-forget pattern after order approval
- **Order Dequeueing**: Pull-based model by leader executor
- **Decoupling**: Orchestrator doesn't wait for order execution

## Distributed Coordination

### Vector Clocks
**Purpose**: Track causal ordering of events across distributed services

**Format**: `[Orchestrator, FraudDetection, OrderQueue, OrderExecutor]`

**Rules**:
1. Each process increments its own counter on internal events
2. On message send: include sender's current vector clock
3. On message receive: merge received VC with local VC (element-wise max), then increment own counter

**Example Flow**:
```
E1: Orchestrator receives order     → [1,0,0,0]
E2: Fraud Detection processes       → [1,1,0,0]
E3: Orchestrator receives response  → [2,1,0,0]
E4: Order Queue enqueues            → [2,1,1,0]
E5: Order Queue dequeues            → [2,1,2,1]
E6: Order Executor executes         → [2,1,2,2]
```

### Leader Election (Bully Algorithm)

**Why Bully Algorithm?**
- Simple to implement compared to Paxos/Raft
- Deterministic: highest-ID process always wins
- No external coordination service needed
- Works well with Docker's DNS-based service discovery

**Algorithm Phases**:

1. **Initialization**:
   - Each executor derives unique ID from IP address (last octet)
   - Discovers peers via DNS lookup of service name
   - Starts heartbeat monitoring

2. **Election Trigger**:
   - Heartbeat timeout (leader unresponsive)
   - Executor startup (no known leader)
   - Manual trigger

3. **Election Process**:
   - Executor sends `Election` RPC to all higher-ID peers
   - If **no response**: declares itself leader, sends `Coordinator` to all
   - If **response received**: waits for `Coordinator` message
   - If timeout without coordinator: retry election

4. **Normal Operation**:
   - Followers send periodic `Heartbeat` RPCs to leader
   - Leader responds with liveness confirmation
   - Only leader dequeues orders from queue

**Timing Parameters**:
- Election Timeout: 2.0 seconds
- Heartbeat Interval: 3.0 seconds
- Dequeue Interval: 2.0 seconds
- Peer Discovery Interval: 5.0 seconds

## Failure Model

**Type**: **Crash-Stop Failures**

The system assumes:
- ✅ Processes may crash and stop responding
- ✅ Messages may be lost due to network issues
- ✅ Processes may recover after crashes
- ❌ No Byzantine failures (malicious behavior)
- ❌ No arbitrary corruption of messages
- ❌ No split-brain scenarios (network partitions are detected)

### Failure Scenarios & Recovery

#### 1. **Leader Executor Crash**
- **Detection**: Followers detect missing heartbeats
- **Recovery**: Automatic re-election via Bully algorithm
- **Impact**: Temporary pause in order processing (~2-4 seconds)
- **Data Loss**: None (orders remain in queue)

#### 2. **Follower Executor Crash**
- **Detection**: Leader notices unresponsive peer
- **Recovery**: Follower removed from peer list until it restarts
- **Impact**: None (leader continues processing)

#### 3. **Order Queue Service Crash**
- **Detection**: RPC timeouts from Orchestrator and Executors
- **Recovery**: Manual restart required
- **Impact**: High - orders cannot be enqueued
- **Data Loss**: All queued orders (in-memory queue)
- **Mitigation**: Could add persistent storage or replication

#### 4. **Orchestrator Crash**
- **Detection**: HTTP request failures from clients
- **Recovery**: Docker restart or manual intervention
- **Impact**: High - no new orders can be submitted
- **Data Loss**: In-flight requests lost

#### 5. **Network Partition**
- **Detection**: RPC timeouts trigger election
- **Recovery**: Bully algorithm elects new leader in majority partition
- **Impact**: Potential duplicate elections if partition heals
- **Mitigation**: Heartbeat intervals tuned to avoid false positives

#### 6. **Message Loss**
- **Detection**: gRPC timeout errors
- **Recovery**: Caller handles timeout (retry or fail-fast)
- **Impact**: Order may fail or be retried
- **Mitigation**: Idempotency keys could be added

## Consistency Model

**Eventual Consistency** with **Causal Ordering**

- **Vector Clocks**: Ensure causally related events are ordered correctly
- **FIFO Queue**: Orders processed in arrival order (per-queue consistency)
- **Leader-Based Processing**: Single leader ensures no duplicate order execution
- **No Strong Consistency**: Services may have different views at any moment

## Scalability Characteristics

### Horizontal Scaling
- **Order Executors**: Can increase replica count via Docker Compose
  ```yaml
  deploy:
    replicas: N  # Any number
  ```
- **Peer Discovery**: DNS automatically includes new replicas
- **Election Overhead**: O(N²) messages for N executors during election

### Bottlenecks
1. **Order Queue**: Single instance, in-memory (could become bottleneck)
2. **Orchestrator**: Single instance (could add load balancer + replicas)
3. **Leader Executor**: Only one processes orders (could shard queue)

### Performance Characteristics
- **Order Approval Latency**: ~100-500ms (depends on LLM API)
- **Order Execution Latency**: ~2 seconds (dequeue interval)
- **Election Latency**: ~4-6 seconds (worst case)
- **Throughput**: Limited by single leader (~0.5 orders/second)

## Security Considerations

### Current State
- **No Authentication**: Services trust each other
- **No Encryption**: gRPC uses insecure channels
- **No Input Validation**: Assumes well-formed requests
- **PII in Logs**: Credit card details logged (security risk)

### Production Recommendations
1. Enable gRPC TLS for encrypted communication
2. Implement mutual TLS (mTLS) for service authentication
3. Add API gateway with rate limiting
4. Sanitize sensitive data in logs
5. Use secrets management (e.g., Vault) for credentials

## Deployment Model

**Technology**: Docker Compose (single-host deployment)

**Network**: Shared bridge network (`ds-practice-2026_default`)
- All services can resolve each other via service names
- Docker DNS provides round-robin for replicated services

**Volumes**: Hot-reload enabled for development
```yaml
volumes:
  - ./order_executor/src:/app/order_executor/src
```

**Health Checks**: Not implemented (could add gRPC health checking)

## Observability

### Logging
- **Format**: Structured logging with correlation IDs
- **Level**: INFO for normal operations, ERROR for failures
- **Example**: `===LOG=== event=order_executing order_id=abc123 executor_id=102`

### Monitoring
- **No Metrics**: No Prometheus/Grafana integration
- **No Tracing**: No distributed tracing (e.g., Jaeger)
- **Recommendation**: Add OpenTelemetry for production

## Testing Strategy

### Current State
- No automated tests
- Manual testing via REST API calls

### Recommended Tests
1. **Unit Tests**: Individual service logic
2. **Integration Tests**: Service-to-service interactions
3. **Chaos Engineering**: Kill leader, verify re-election
4. **Load Tests**: Measure throughput under load
5. **Network Partition Tests**: Verify split-brain handling

## Future Enhancements

1. **Persistent Queue**: Replace in-memory queue with Redis/RabbitMQ
2. **Multiple Leaders**: Shard queue for parallel processing
3. **Idempotency**: Add order IDs to prevent duplicate execution
4. **Circuit Breakers**: Handle cascading failures gracefully
5. **API Gateway**: Add rate limiting, authentication
6. **Database Integration**: Store order history for auditing
7. **Event Sourcing**: Capture state changes as immutable events
8. **CQRS**: Separate read/write models for scalability

---

**Document Version**: 1.0
**Last Updated**: 2026-03-30
**System Commit**: c046b7ddc0db744c4ddb1533412eb75118b4a5db
