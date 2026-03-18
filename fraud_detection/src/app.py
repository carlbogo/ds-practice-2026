import sys
import os
import hashlib
import logging
import time
import threading

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/fraud_detection')
)
sys.path.insert(0, fraud_detection_grpc_path)

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import grpc
from concurrent import futures

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("fraud_detection")

# Card BIN prefixes associated with high-fraud prepaid/gift card ranges
_SUSPICIOUS_BIN_PREFIXES = {"400000", "411111", "520000", "530000", "601100"}

# Disposable / temporary email domains
_DISPOSABLE_EMAIL_DOMAINS = {
    "tempmail.com",
    "throwaway.email",
    "guerrillamail.com",
    "mailinator.com",
    "fakeinbox.com",
    "yopmail.com",
}


def _check_card_blocklist(card_number):
    digits = "".join(ch for ch in str(card_number or "") if ch.isdigit())
    if len(digits) >= 6 and digits[:6] in _SUSPICIOUS_BIN_PREFIXES:
        return "Credit card BIN is in a high-risk range"
    return None


def _check_disposable_email(email):
    email = (email or "").strip().lower()
    if "@" in email:
        domain = email.rsplit("@", 1)[1]
        if domain in _DISPOSABLE_EMAIL_DOMAINS:
            return "Purchaser email uses a disposable email provider"
    return None


def _check_risk_score(order_id, card_number, email):
    payload = f"{order_id}:{card_number}:{email}"
    digest = hashlib.sha256(payload.encode()).hexdigest()
    score = int(digest[-4:], 16)

    if score >= 64000:
        return f"Transaction risk score is elevated ({score})"
    return None


class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceServicer):

    def __init__(self):
        # Vector clock layout: [transaction_verification, fraud_detection, suggestions]
        self.svc_idx = int(os.getenv("FRAUD_SERVICE_INDEX", "1"))
        self.total_svcs = int(os.getenv("VECTOR_CLOCK_SIZE", "3"))

        # order_id -> cached data + shared vector clock
        self.orders = {}
        self.lock = threading.Lock()

    def _normalize_vc(self, incoming_vc):
        vc = list(incoming_vc or [])

        if len(vc) < self.total_svcs:
            vc.extend([0] * (self.total_svcs - len(vc)))
        elif len(vc) > self.total_svcs:
            vc = vc[:self.total_svcs]

        return vc

    def _merge_and_increment(self, local_vc, incoming_vc):
        incoming_vc = self._normalize_vc(incoming_vc)

        for i in range(self.total_svcs):
            local_vc[i] = max(local_vc[i], incoming_vc[i])

        local_vc[self.svc_idx] += 1

    def _get_order_or_not_found(self, order_id, context):
        entry = self.orders.get(order_id)

        if entry is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Order '{order_id}' not initialized")

        return entry

    def InitOrder(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = {
                    "order_id": request.order_id,
                    "purchaser_email": request.purchaser_email,
                    "credit_card_number": request.credit_card_number,
                    "vc": [0] * self.total_svcs,
                }

                entry["vc"][self.svc_idx] += 1
                self.orders[request.order_id] = entry

                vc_snapshot = entry["vc"][:]

            return fraud_detection.InitOrderResponse(
                ok=True,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=init_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during fraud order initialization")

            return fraud_detection.InitOrderResponse(
                ok=False,
                vc=[0] * self.total_svcs,
            )

    def CheckCardFraud(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return fraud_detection.FraudEventResponse(
                        is_fraud=False,
                        reasons=["Order not initialized"],
                        vc=[0] * self.total_svcs,
                    )

                self._merge_and_increment(entry["vc"], request.vc)
                vc_snapshot = entry["vc"][:]

                card_number = entry["credit_card_number"]

            reason = _check_card_blocklist(card_number)
            reasons = [reason] if reason else []
            is_fraud = len(reasons) > 0

            return fraud_detection.FraudEventResponse(
                is_fraud=is_fraud,
                reasons=reasons,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=check_card_fraud_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during card fraud check")

            return fraud_detection.FraudEventResponse(
                is_fraud=False,
                reasons=["Internal card fraud error"],
                vc=[0] * self.total_svcs,
            )

    def CheckEmailFraud(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return fraud_detection.FraudEventResponse(
                        is_fraud=False,
                        reasons=["Order not initialized"],
                        vc=[0] * self.total_svcs,
                    )

                self._merge_and_increment(entry["vc"], request.vc)
                vc_snapshot = entry["vc"][:]

                purchaser_email = entry["purchaser_email"]

            reason = _check_disposable_email(purchaser_email)
            reasons = [reason] if reason else []
            is_fraud = len(reasons) > 0

            return fraud_detection.FraudEventResponse(
                is_fraud=is_fraud,
                reasons=reasons,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=check_email_fraud_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during email fraud check")

            return fraud_detection.FraudEventResponse(
                is_fraud=False,
                reasons=["Internal email fraud error"],
                vc=[0] * self.total_svcs,
            )

    def CheckRiskScoreFraud(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return fraud_detection.FraudEventResponse(
                        is_fraud=False,
                        reasons=["Order not initialized"],
                        vc=[0] * self.total_svcs,
                    )

                self._merge_and_increment(entry["vc"], request.vc)
                vc_snapshot = entry["vc"][:]

                cached_order_id = entry["order_id"]
                purchaser_email = entry["purchaser_email"]
                card_number = entry["credit_card_number"]

            reason = _check_risk_score(
                cached_order_id,
                card_number,
                purchaser_email,
            )

            reasons = [reason] if reason else []
            is_fraud = len(reasons) > 0

            return fraud_detection.FraudEventResponse(
                is_fraud=is_fraud,
                reasons=reasons,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=check_risk_score_fraud_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during risk score fraud check")

            return fraud_detection.FraudEventResponse(
                is_fraud=False,
                reasons=["Internal risk score fraud error"],
                vc=[0] * self.total_svcs,
            )

    def FinalizeOrder(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                if request.order_id in self.orders:
                    del self.orders[request.order_id]

            return fraud_detection.FinalizeOrderResponse(ok=True)

        except Exception:
            logger.exception("event=finalize_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during order finalization")

            return fraud_detection.FinalizeOrderResponse(ok=False)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    fraud_detection_grpc.add_FraudDetectionServiceServicer_to_server(
        FraudDetectionService(),
        server,
    )

    port = "50051"
    server.add_insecure_port("[::]:" + port)

    server.start()
    logger.info("Server started. Listening on port %s.", port)

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
