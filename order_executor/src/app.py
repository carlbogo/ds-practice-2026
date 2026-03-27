import sys
import os
import socket
import logging
import threading
import time
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

order_executor_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/order_executor')
)
sys.path.insert(0, order_executor_grpc_path)

order_queue_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/order_queue')
)
sys.path.insert(0, order_queue_grpc_path)

import order_executor_pb2 as order_executor
import order_executor_pb2_grpc as order_executor_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc
import grpc

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("order_executor")

# Configuration from environment
EXECUTOR_PORT = int(os.getenv("EXECUTOR_PORT", "50055"))
ORDER_QUEUE_ADDR = os.getenv("ORDER_QUEUE_ADDR", "order_queue:50054")
SERVICE_NAME = os.getenv("SERVICE_NAME", "order_executor")

ELECTION_TIMEOUT = 2.0
HEARTBEAT_INTERVAL = 3.0
DEQUEUE_INTERVAL = 2.0
DISCOVERY_INTERVAL = 5.0


def _get_my_ip():
    """Get this container's IP address on the Docker network."""
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


def _ip_to_id(ip):
    """Derive a unique integer ID from an IP address (uses last octet)."""
    return int(ip.strip().split(".")[-1])


def _discover_peers(service_name, port):
    """Resolve the Docker service DNS name to find all replica IPs."""
    try:
        results = socket.getaddrinfo(service_name, port, socket.AF_INET, socket.SOCK_STREAM)
        ips = sorted(set(r[4][0] for r in results))
        return ips
    except socket.gaierror:
        return []


class BullyLeaderElection:
    """Implements the Bully algorithm for leader election among executor replicas.

    Peers are discovered dynamically via DNS, so this works for any number of replicas.
    """

    def __init__(self, my_ip, port, service_name):
        self.my_ip = my_ip
        self.executor_id = _ip_to_id(my_ip)
        self.port = port
        self.service_name = service_name
        self.leader_id = None
        self.peers = {}  # {id: "ip:port"}
        self.lock = threading.Lock()
        self.election_in_progress = False

    def refresh_peers(self):
        """Re-discover peers via DNS and update the peer map."""
        ips = _discover_peers(self.service_name, self.port)
        new_peers = {}
        for ip in ips:
            pid = _ip_to_id(ip)
            if pid != self.executor_id:
                new_peers[pid] = f"{ip}:{self.port}"
        with self.lock:
            self.peers = new_peers

    def start_election(self):
        """Initiate an election using the Bully algorithm."""
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            peers_snapshot = dict(self.peers)

        logger.info(
            "executor_id=%d event=election_started peers=%s",
            self.executor_id,
            list(peers_snapshot.keys()),
        )

        # Send Election messages to all processes with higher IDs
        higher_peers = {pid: addr for pid, addr in peers_snapshot.items() if pid > self.executor_id}
        got_response = False

        for pid, addr in higher_peers.items():
            try:
                channel = grpc.insecure_channel(addr)
                stub = order_executor_grpc.OrderExecutorServiceStub(channel)
                response = stub.Election(
                    order_executor.ElectionRequest(sender_id=self.executor_id),
                    timeout=ELECTION_TIMEOUT,
                )
                if response.alive:
                    got_response = True
                    logger.info(
                        "executor_id=%d event=election_response_received from=%d",
                        self.executor_id,
                        response.responder_id,
                    )
                channel.close()
            except grpc.RpcError:
                logger.info(
                    "executor_id=%d event=peer_unreachable peer_id=%d",
                    self.executor_id,
                    pid,
                )

        if not got_response:
            # No higher-ID process responded — we are the leader
            self._declare_victory()
        else:
            # A higher-ID process responded — wait for coordinator message
            logger.info(
                "executor_id=%d event=waiting_for_coordinator",
                self.executor_id,
            )
            time.sleep(ELECTION_TIMEOUT * 2)
            with self.lock:
                if self.leader_id is None or not self._is_leader_alive_unlocked():
                    self.election_in_progress = False
                    self.start_election()
                    return

        with self.lock:
            self.election_in_progress = False

    def _declare_victory(self):
        """Announce to all peers that this process is the new leader."""
        with self.lock:
            self.leader_id = self.executor_id
            peers_snapshot = dict(self.peers)

        logger.info(
            "executor_id=%d event=declared_leader",
            self.executor_id,
        )

        for pid, addr in peers_snapshot.items():
            try:
                channel = grpc.insecure_channel(addr)
                stub = order_executor_grpc.OrderExecutorServiceStub(channel)
                stub.Coordinator(
                    order_executor.CoordinatorRequest(leader_id=self.executor_id),
                    timeout=ELECTION_TIMEOUT,
                )
                channel.close()
            except grpc.RpcError:
                logger.info(
                    "executor_id=%d event=coordinator_announce_failed peer_id=%d",
                    self.executor_id,
                    pid,
                )

    def _is_leader_alive_unlocked(self):
        """Check if the current leader is reachable (caller must NOT hold self.lock)."""
        if self.leader_id is None:
            return False
        if self.leader_id == self.executor_id:
            return True

        addr = self.peers.get(self.leader_id)
        if not addr:
            return False

        try:
            channel = grpc.insecure_channel(addr)
            stub = order_executor_grpc.OrderExecutorServiceStub(channel)
            response = stub.Heartbeat(
                order_executor.HeartbeatRequest(sender_id=self.executor_id),
                timeout=ELECTION_TIMEOUT,
            )
            channel.close()
            return response.alive
        except grpc.RpcError:
            return False

    def set_leader(self, leader_id):
        with self.lock:
            self.leader_id = leader_id
            logger.info(
                "executor_id=%d event=leader_updated leader_id=%d",
                self.executor_id,
                leader_id,
            )

    def get_leader(self):
        with self.lock:
            return self.leader_id

    def is_leader(self):
        with self.lock:
            return self.leader_id == self.executor_id


class OrderExecutorServiceImpl(order_executor_grpc.OrderExecutorServiceServicer):
    """gRPC service that handles Election, Coordinator, and Heartbeat RPCs."""

    def __init__(self, election):
        self.election = election

    def Election(self, request, context):
        logger.info(
            "executor_id=%d event=election_request_received from=%d",
            self.election.executor_id,
            request.sender_id,
        )
        # Respond alive, then start our own election
        threading.Thread(
            target=self.election.start_election, daemon=True,
        ).start()

        return order_executor.ElectionResponse(
            alive=True,
            responder_id=self.election.executor_id,
        )

    def Coordinator(self, request, context):
        logger.info(
            "executor_id=%d event=coordinator_received leader_id=%d",
            self.election.executor_id,
            request.leader_id,
        )
        self.election.set_leader(request.leader_id)
        return order_executor.CoordinatorResponse(acknowledged=True)

    def Heartbeat(self, request, context):
        return order_executor.HeartbeatResponse(
            alive=True,
            leader_id=self.election.get_leader() or self.election.executor_id,
        )


def peer_discovery_loop(election):
    """Periodically re-discover peers via DNS to handle scaling."""
    while True:
        election.refresh_peers()
        time.sleep(DISCOVERY_INTERVAL)


def heartbeat_monitor(election):
    """Monitor the leader and trigger elections if it goes down."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        leader = election.get_leader()

        if leader is None:
            logger.info(
                "executor_id=%d event=no_leader_detected triggering_election",
                election.executor_id,
            )
            election.start_election()
        elif leader != election.executor_id:
            if not election._is_leader_alive_unlocked():
                logger.info(
                    "executor_id=%d event=leader_down leader_id=%d triggering_election",
                    election.executor_id,
                    leader,
                )
                with election.lock:
                    election.leader_id = None
                election.start_election()


def order_processing_loop(election):
    """Leader dequeues orders from the queue and executes them."""
    while True:
        time.sleep(DEQUEUE_INTERVAL)

        if not election.is_leader():
            continue

        try:
            channel = grpc.insecure_channel(ORDER_QUEUE_ADDR)
            stub = order_queue_grpc.OrderQueueServiceStub(channel)
            response = stub.Dequeue(
                order_queue.DequeueRequest(),
                timeout=3.0,
            )
            channel.close()

            if response.ok:
                items_str = ", ".join(
                    f"{item.name} x{item.quantity}" for item in response.items
                )
                logger.info(
                    "executor_id=%d event=order_executing order_id=%s "
                    "purchaser=%s items=[%s]",
                    election.executor_id,
                    response.order_id,
                    response.purchaser_name,
                    items_str,
                )
                logger.info(
                    "executor_id=%d event=order_executed order_id=%s "
                    "status=completed",
                    election.executor_id,
                    response.order_id,
                )
        except grpc.RpcError as e:
            logger.warning(
                "executor_id=%d event=dequeue_failed error=%s",
                election.executor_id,
                str(e),
            )


def serve():
    my_ip = _get_my_ip()
    my_id = _ip_to_id(my_ip)

    logger.info("executor_id=%d ip=%s starting up", my_id, my_ip)

    election = BullyLeaderElection(my_ip, EXECUTOR_PORT, SERVICE_NAME)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_executor_grpc.add_OrderExecutorServiceServicer_to_server(
        OrderExecutorServiceImpl(election), server,
    )

    port = str(EXECUTOR_PORT)
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(
        "executor_id=%d Server started. Listening on port %s.",
        my_id, port,
    )

    # Start peer discovery loop
    threading.Thread(target=peer_discovery_loop, args=(election,), daemon=True).start()

    # Wait for peers to register in DNS, then trigger initial election
    time.sleep(5)
    election.refresh_peers()
    threading.Thread(target=election.start_election, daemon=True).start()

    # Start heartbeat monitor and order processing
    threading.Thread(target=heartbeat_monitor, args=(election,), daemon=True).start()
    threading.Thread(target=order_processing_loop, args=(election,), daemon=True).start()

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
