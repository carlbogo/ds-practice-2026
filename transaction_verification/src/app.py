import sys
import os
import re
import threading
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# gRPC stubs for this service
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/transaction_verification')
)
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

# gRPC stubs for fraud_detection (peer-to-peer calls)
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
logger = logging.getLogger("transaction_verification")

PIPELINE_TIMEOUT = float(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30.0"))
FRAUD_DETECTION_ADDR = os.getenv("FRAUD_DETECTION_ADDR", "fraud_detection:50051")

# Lazy-init fraud_detection stub
_fraud_channel = None
_fraud_stub = None


def _get_fraud_stub():
    global _fraud_channel, _fraud_stub
    if _fraud_stub is None:
        _fraud_channel = grpc.insecure_channel(FRAUD_DETECTION_ADDR)
        _fraud_stub = fraud_detection_grpc.FraudDetectionServiceStub(_fraud_channel)
    return _fraud_stub


def _is_non_empty(value: str) -> bool:
    return bool(value and value.strip())


def _is_valid_email(email: str) -> bool:
    email = (email or "").strip()
    return re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) is not None


def _is_luhn_valid(card_number: str) -> bool:
    digits_only = "".join(ch for ch in card_number if ch.isdigit())
    if not 13 <= len(digits_only) <= 19:
        return False
    if len(set(digits_only)) == 1:
        return False

    checksum = 0
    should_double = False
    for digit in reversed(digits_only):
        value = int(digit)
        if should_double:
            value *= 2
            if value > 9:
                value -= 9
        checksum += value
        should_double = not should_double
    return checksum % 10 == 0


def _is_valid_expiration(expiration: str) -> bool:
    exp = (expiration or "").strip()
    match = re.fullmatch(r"(0[1-9]|1[0-2])\/(\d{2}|\d{4})", exp)
    if not match:
        return False

    month = int(match.group(1))
    year_raw = match.group(2)
    year = int(year_raw) + 2000 if len(year_raw) == 2 else int(year_raw)

    now = datetime.utcnow()
    if year < now.year:
        return False
    if year == now.year and month < now.month:
        return False
    return True


def _is_valid_cvv(cvv: str) -> bool:
    cvv = (cvv or "").strip()
    return re.fullmatch(r"\d{3,4}", cvv) is not None


def _is_valid_billing_zip(zip_code: str) -> bool:
    zip_code = (zip_code or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9 -]{3,10}", zip_code):
        return False
    return any(ch.isdigit() for ch in zip_code)


def _validate_items(items) -> list[str]:
    errors = []
    if len(items) == 0:
        errors.append("At least one item is required")
        return errors

    for idx, item in enumerate(items):
        if not _is_non_empty(item.name):
            errors.append(f"Item at index {idx} must have a name")
        if item.quantity <= 0:
            errors.append(f"Item '{item.name or idx}' must have quantity > 0")
    return errors


class _InternalContext:
    """Dummy gRPC context for internal (non-RPC) calls to step handlers."""
    def set_code(self, code):
        pass

    def set_details(self, details):
        pass


class TransactionVerificationService(
    transaction_verification_grpc.TransactionVerificationServiceServicer
):
    def __init__(self):
        # Vector clock layout: [transaction_verification, fraud_detection, suggestions]
        self.svc_idx = 0
        self.total_svcs = int(os.getenv("VECTOR_CLOCK_SIZE", "3"))
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
                    "purchaser_name": request.purchaser_name,
                    "purchaser_email": request.purchaser_email,
                    "credit_card_number": request.credit_card_number,
                    "credit_card_expiration": request.credit_card_expiration,
                    "credit_card_cvv": request.credit_card_cvv,
                    "billing_street": request.billing_street,
                    "billing_city": request.billing_city,
                    "billing_state": request.billing_state,
                    "billing_zip": request.billing_zip,
                    "billing_country": request.billing_country,
                    "items": [
                        {"name": item.name, "quantity": item.quantity}
                        for item in request.items
                    ],
                    "vc": [0] * self.total_svcs,
                }

                entry["vc"][self.svc_idx] += 1
                self.orders[request.order_id] = entry

                vc_snapshot = entry["vc"][:]

            return transaction_verification.InitOrderResponse(
                ok=True,
                vc=vc_snapshot,
            )
        except Exception:
            logger.exception("event=init_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(
                "Internal error during transaction verification order initialization"
            )

            return transaction_verification.InitOrderResponse(
                ok=False,
                vc=[0] * self.total_svcs,
            )

    def CheckItems(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return transaction_verification.TransactionEventResponse(
                        is_valid=False,
                        reasons=["Order not initialized"],
                        vc=[0] * self.total_svcs,
                    )

                # VC fix: work on a COPY so parallel steps stay concurrent
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc
                items = [dict(item) for item in entry["items"]]

            item_messages = [
                transaction_verification.Item(
                    name=item["name"],
                    quantity=item["quantity"],
                )
                for item in items
            ]

            reasons = _validate_items(item_messages)
            is_valid = len(reasons) == 0

            logger.info(
                "event=step_completed step=A outcome=%s vc=%s cid=%s",
                "success" if is_valid else "reject",
                vc_snapshot,
                order_id,
            )

            return transaction_verification.TransactionEventResponse(
                is_valid=is_valid,
                reasons=reasons,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=check_items_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during item validation")

            return transaction_verification.TransactionEventResponse(
                is_valid=False,
                reasons=["Internal item validation error"],
                vc=[0] * self.total_svcs,
            )

    def CheckUserAndBillingData(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return transaction_verification.TransactionEventResponse(
                        is_valid=False,
                        reasons=["Order not initialized"],
                        vc=[0] * self.total_svcs,
                    )

                # VC fix: work on a COPY
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc

                cached_order_id = entry["order_id"]
                purchaser_name = entry["purchaser_name"]
                purchaser_email = entry["purchaser_email"]
                billing_street = entry["billing_street"]
                billing_city = entry["billing_city"]
                billing_state = entry["billing_state"]
                billing_zip = entry["billing_zip"]
                billing_country = entry["billing_country"]

            reasons = []

            if not _is_non_empty(cached_order_id):
                reasons.append("Missing transaction ID")
            if not _is_non_empty(purchaser_name):
                reasons.append("Missing purchaser name")
            if not _is_non_empty(purchaser_email):
                reasons.append("Missing purchaser email")
            elif not _is_valid_email(purchaser_email):
                reasons.append("Purchaser email format is invalid")

            if not _is_non_empty(billing_street):
                reasons.append("Missing billing street")
            if not _is_non_empty(billing_city):
                reasons.append("Missing billing city")
            if not _is_non_empty(billing_state):
                reasons.append("Missing billing state")
            if not _is_non_empty(billing_zip):
                reasons.append("Missing billing zip")
            elif not _is_valid_billing_zip(billing_zip):
                reasons.append("Billing zip format is invalid")
            if not _is_non_empty(billing_country):
                reasons.append("Missing billing country")

            is_valid = len(reasons) == 0

            logger.info(
                "event=step_completed step=B outcome=%s vc=%s cid=%s",
                "success" if is_valid else "reject",
                vc_snapshot,
                order_id,
            )

            return transaction_verification.TransactionEventResponse(
                is_valid=is_valid,
                reasons=reasons,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=check_user_billing_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during user and billing validation")

            return transaction_verification.TransactionEventResponse(
                is_valid=False,
                reasons=["Internal user and billing validation error"],
                vc=[0] * self.total_svcs,
            )

    def CheckPaymentData(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return transaction_verification.TransactionEventResponse(
                        is_valid=False,
                        reasons=["Order not initialized"],
                        vc=[0] * self.total_svcs,
                    )

                # VC fix: work on a COPY
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc

                credit_card_number = entry["credit_card_number"]
                credit_card_expiration = entry["credit_card_expiration"]
                credit_card_cvv = entry["credit_card_cvv"]

            reasons = []

            if not _is_non_empty(credit_card_number):
                reasons.append("Missing credit card number")
            elif not _is_luhn_valid(credit_card_number):
                reasons.append("Credit card number is invalid")

            if not _is_non_empty(credit_card_expiration):
                reasons.append("Missing credit card expiration")
            elif not _is_valid_expiration(credit_card_expiration):
                reasons.append("Credit card expiration is invalid or expired")

            if not _is_non_empty(credit_card_cvv):
                reasons.append("Missing credit card CVV")
            elif not _is_valid_cvv(credit_card_cvv):
                reasons.append("Credit card CVV is invalid")

            is_valid = len(reasons) == 0

            logger.info(
                "event=step_completed step=C outcome=%s vc=%s cid=%s",
                "success" if is_valid else "reject",
                vc_snapshot,
                order_id,
            )

            return transaction_verification.TransactionEventResponse(
                is_valid=is_valid,
                reasons=reasons,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=check_payment_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during payment validation")

            return transaction_verification.TransactionEventResponse(
                is_valid=False,
                reasons=["Internal payment validation error"],
                vc=[0] * self.total_svcs,
            )

    def FinalizeOrder(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                if request.order_id in self.orders:
                    del self.orders[request.order_id]

            return transaction_verification.FinalizeOrderResponse(ok=True)

        except Exception:
            logger.exception("event=finalize_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during order finalization")

            return transaction_verification.FinalizeOrderResponse(ok=False)

    # ------------------------------------------------------------------
    # StartWorkflow: peer-to-peer entry point called by the orchestrator
    # ------------------------------------------------------------------

    def StartWorkflow(self, request, context):
        order_id = request.order_id or "unknown"
        base_vc = list(request.vc)
        purchased_books = list(request.purchased_books)
        suggestion_mode = request.suggestion_mode or "author"

        logger.info(
            "event=workflow_started base_vc=%s cid=%s",
            base_vc, order_id,
        )

        dummy_ctx = _InternalContext()

        # --- Run A and B in parallel ---
        def run_a():
            req = transaction_verification.TransactionEventRequest(
                order_id=order_id, vc=base_vc,
            )
            return self.CheckItems(req, dummy_ctx)

        def run_b():
            req = transaction_verification.TransactionEventRequest(
                order_id=order_id, vc=base_vc,
            )
            return self.CheckUserAndBillingData(req, dummy_ctx)

        with ThreadPoolExecutor(max_workers=2) as pool:
            future_a = pool.submit(run_a)
            future_b = pool.submit(run_b)
            a_resp = future_a.result()
            b_resp = future_b.result()

        a_ok, a_reasons, a_vc = a_resp.is_valid, list(a_resp.reasons), list(a_resp.vc)
        b_ok, b_reasons, b_vc = b_resp.is_valid, list(b_resp.reasons), list(b_resp.vc)

        # --- Run C (depends on A) ---
        if a_ok:
            c_req = transaction_verification.TransactionEventRequest(
                order_id=order_id, vc=a_vc,
            )
            c_resp = self.CheckPaymentData(c_req, dummy_ctx)
            c_ok, c_reasons, c_vc = c_resp.is_valid, list(c_resp.reasons), list(c_resp.vc)
        else:
            # A failed → C is skipped, forward A's failure
            c_ok, c_reasons, c_vc = False, list(a_reasons), list(a_vc)

        logger.info(
            "event=tx_steps_done A=%s B=%s C=%s a_vc=%s b_vc=%s c_vc=%s cid=%s",
            a_ok, b_ok, c_ok, a_vc, b_vc, c_vc, order_id,
        )

        # --- Deliver B and C results to fraud_detection in parallel ---
        # Both calls block until the full pipeline (D→E→F→G) completes.
        fraud_stub = _get_fraud_stub()

        def deliver_b():
            req = fraud_detection.DeliverStepRequest(
                order_id=order_id,
                step="D",
                ok=b_ok,
                reasons=b_reasons,
                vc=b_vc,
                purchased_books=purchased_books,
                suggestion_mode=suggestion_mode,
            )
            return fraud_stub.DeliverStepResult(req, timeout=PIPELINE_TIMEOUT)

        def deliver_c():
            req = fraud_detection.DeliverStepRequest(
                order_id=order_id,
                step="E",
                ok=c_ok,
                reasons=c_reasons,
                vc=c_vc,
                purchased_books=purchased_books,
                suggestion_mode=suggestion_mode,
            )
            return fraud_stub.DeliverStepResult(req, timeout=PIPELINE_TIMEOUT)

        with ThreadPoolExecutor(max_workers=2) as pool:
            future_d = pool.submit(deliver_b)
            future_c = pool.submit(deliver_c)
            # Both return the same PipelineResponse once pipeline completes
            pipeline_resp = future_d.result()
            # Ignore second result (same data)
            try:
                future_c.result()
            except Exception:
                pass

        # Combine all reasons: tx verification failures + pipeline failures
        all_reasons = []
        if not a_ok:
            all_reasons.extend(a_reasons)
        if not b_ok:
            all_reasons.extend(b_reasons)
        if not c_ok and a_ok:
            # Only add C's own reasons if A succeeded (otherwise C's reasons are A's)
            all_reasons.extend(c_reasons)
        all_reasons.extend(list(pipeline_resp.reasons))

        # Deduplicate while preserving order
        seen = set()
        unique_reasons = []
        for r in all_reasons:
            if r not in seen:
                seen.add(r)
                unique_reasons.append(r)

        pipeline_ok = pipeline_resp.ok and a_ok and b_ok and c_ok
        final_vc = list(pipeline_resp.vc) if pipeline_resp.vc else c_vc

        logger.info(
            "event=workflow_done ok=%s reasons=%s final_vc=%s cid=%s",
            pipeline_ok, unique_reasons, final_vc, order_id,
        )

        return transaction_verification.WorkflowResponse(
            ok=pipeline_ok,
            reasons=unique_reasons,
            suggested_books=list(pipeline_resp.suggested_books),
            vc=final_vc,
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(
        TransactionVerificationService(), server
    )
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port %s.", port)
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
