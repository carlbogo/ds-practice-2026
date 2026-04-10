import sys
import os
import hashlib
import logging
import threading

# gRPC stubs for this service
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/fraud_detection')
)
sys.path.insert(0, fraud_detection_grpc_path)

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

# gRPC stubs for suggestions (peer-to-peer calls)
suggestions_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/suggestions')
)
sys.path.insert(0, suggestions_grpc_path)

import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

import grpc
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("fraud_detection")

PIPELINE_TIMEOUT = float(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30.0"))
SUGGESTIONS_ADDR = os.getenv("SUGGESTIONS_ADDR", "suggestions:50053")

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

# Lazy-init suggestions stub
_suggestions_channel = None
_suggestions_stub = None


def _get_suggestions_stub():
    global _suggestions_channel, _suggestions_stub
    if _suggestions_stub is None:
        _suggestions_channel = grpc.insecure_channel(SUGGESTIONS_ADDR)
        _suggestions_stub = suggestions_grpc.SuggestionsServiceStub(_suggestions_channel)
    return _suggestions_stub


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


def _merge_vc(*vectors):
    """Merge multiple vector clocks by taking element-wise max."""
    size = 3
    merged = [0] * size
    for vector in vectors:
        current = list(vector or [])
        for i in range(min(len(current), size)):
            merged[i] = max(merged[i], current[i])
    return merged


class _PipelineState:
    """Per-order gating state for the D+E -> F -> G pipeline."""

    def __init__(self):
        self.d_result = None  # {ok, reasons, vc}
        self.e_result = None
        self.d_done = threading.Event()
        self.e_done = threading.Event()
        self.pipeline_complete = threading.Event()
        self.final_response = None
        self.completion_lock = threading.Lock()
        self.purchased_books = []
        self.suggestion_mode = "author"


class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceServicer):

    def __init__(self):
        # Vector clock layout: [transaction_verification, fraud_detection, suggestions]
        self.svc_idx = int(os.getenv("FRAUD_SERVICE_INDEX", "1"))
        self.total_svcs = int(os.getenv("VECTOR_CLOCK_SIZE", "3"))

        # order_id -> cached data + vector clock
        self.orders = {}
        self.lock = threading.Lock()

        # order_id -> _PipelineState (for DeliverStepResult gating)
        self.pipeline_states = {}
        self.pipeline_lock = threading.Lock()

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

                # VC fix: work on a COPY
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc

                card_number = entry["credit_card_number"]

            reason = _check_card_blocklist(card_number)
            reasons = [reason] if reason else []
            is_fraud = len(reasons) > 0

            logger.info(
                "event=step_completed step=E outcome=%s vc=%s cid=%s",
                "fraud" if is_fraud else "success",
                vc_snapshot,
                order_id,
            )

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

                # VC fix: work on a COPY
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc

                purchaser_email = entry["purchaser_email"]

            reason = _check_disposable_email(purchaser_email)
            reasons = [reason] if reason else []
            is_fraud = len(reasons) > 0

            logger.info(
                "event=step_completed step=D outcome=%s vc=%s cid=%s",
                "fraud" if is_fraud else "success",
                vc_snapshot,
                order_id,
            )

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

                # VC fix: work on a COPY
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc

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

            logger.info(
                "event=step_completed step=F outcome=%s vc=%s cid=%s",
                "fraud" if is_fraud else "success",
                vc_snapshot,
                order_id,
            )

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

            with self.pipeline_lock:
                self.pipeline_states.pop(request.order_id, None)

            return fraud_detection.FinalizeOrderResponse(ok=True)

        except Exception:
            logger.exception("event=finalize_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during order finalization")

            return fraud_detection.FinalizeOrderResponse(ok=False)

    # ------------------------------------------------------------------
    # DeliverStepResult: receives B or C result from transaction_verification
    # Runs the corresponding fraud step (D or E), gates on both, then F→G
    # ------------------------------------------------------------------

    def _get_or_create_pipeline_state(self, order_id, purchased_books, suggestion_mode):
        with self.pipeline_lock:
            if order_id not in self.pipeline_states:
                ps = _PipelineState()
                ps.purchased_books = list(purchased_books or [])
                ps.suggestion_mode = suggestion_mode or "author"
                self.pipeline_states[order_id] = ps
            else:
                ps = self.pipeline_states[order_id]
                # Update passthrough data if provided
                if purchased_books:
                    ps.purchased_books = list(purchased_books)
                if suggestion_mode:
                    ps.suggestion_mode = suggestion_mode
            return ps

    def _complete_pipeline(self, order_id, ps):
        """Run F → G after both D and E are done. Called exactly once per order."""
        all_reasons = []
        d_ok = ps.d_result["ok"]
        e_ok = ps.e_result["ok"]

        if not d_ok:
            all_reasons.extend(ps.d_result["reasons"])
        if not e_ok:
            all_reasons.extend(ps.e_result["reasons"])

        # F requires both D and E to have succeeded
        if d_ok and e_ok:
            # Merge D and E VCs for F's input
            f_input_vc = _merge_vc(ps.d_result["vc"], ps.e_result["vc"])

            dummy_ctx = type('Ctx', (), {
                'set_code': lambda s, c: None,
                'set_details': lambda s, d: None,
            })()

            f_req = fraud_detection.FraudEventRequest(
                order_id=order_id, vc=f_input_vc,
            )
            f_resp = self.CheckRiskScoreFraud(f_req, dummy_ctx)
            f_ok = not f_resp.is_fraud
            f_reasons = list(f_resp.reasons)
            f_vc = list(f_resp.vc)

            if not f_ok:
                all_reasons.extend(f_reasons)
        else:
            f_ok = False
            # Use best available VC
            f_vc = _merge_vc(
                ps.d_result.get("vc", []),
                ps.e_result.get("vc", []),
            )

        # G requires F to have succeeded (and no earlier failures)
        if f_ok and not all_reasons:
            sugg_stub = _get_suggestions_stub()
            g_req = suggestions.SuggestionsRequest(
                order_id=order_id,
                purchased_books=ps.purchased_books,
                mode=ps.suggestion_mode,
                vc=f_vc,
            )
            g_resp = sugg_stub.GetSuggestions(g_req, timeout=PIPELINE_TIMEOUT)
            suggested_books = list(g_resp.suggested_books)
            final_vc = list(g_resp.vc)

            logger.info(
                "event=step_completed step=G outcome=success vc=%s cid=%s",
                final_vc, order_id,
            )
        else:
            suggested_books = []
            final_vc = f_vc

        pipeline_ok = len(all_reasons) == 0

        logger.info(
            "event=pipeline_done ok=%s reasons=%s final_vc=%s cid=%s",
            pipeline_ok, all_reasons, final_vc, order_id,
        )

        return fraud_detection.PipelineResponse(
            ok=pipeline_ok,
            reasons=all_reasons,
            suggested_books=suggested_books,
            vc=final_vc,
        )

    def DeliverStepResult(self, request, context):
        order_id = request.order_id or "unknown"
        step = request.step  # "D" or "E"

        ps = self._get_or_create_pipeline_state(
            order_id, request.purchased_books, request.suggestion_mode,
        )

        dummy_ctx = type('Ctx', (), {
            'set_code': lambda s, c: None,
            'set_details': lambda s, d: None,
        })()

        if step == "D":
            # B's result arrived → run D (email fraud) if B was OK
            if request.ok:
                d_req = fraud_detection.FraudEventRequest(
                    order_id=order_id, vc=list(request.vc),
                )
                d_resp = self.CheckEmailFraud(d_req, dummy_ctx)
                ps.d_result = {
                    "ok": not d_resp.is_fraud,
                    "reasons": list(d_resp.reasons),
                    "vc": list(d_resp.vc),
                }
            else:
                # B failed → skip D, forward failure
                ps.d_result = {
                    "ok": False,
                    "reasons": list(request.reasons),
                    "vc": list(request.vc),
                }
                logger.info(
                    "event=step_skipped step=D reason=B_failed cid=%s", order_id,
                )
            ps.d_done.set()

        elif step == "E":
            # C's result arrived → E depends on BOTH C and D,
            # so wait for D to finish first
            ps.d_done.wait()

            if request.ok:
                # Merge C's VC with D's output VC (E has seen both)
                e_input_vc = _merge_vc(list(request.vc), ps.d_result["vc"])
                e_req = fraud_detection.FraudEventRequest(
                    order_id=order_id, vc=e_input_vc,
                )
                e_resp = self.CheckCardFraud(e_req, dummy_ctx)
                ps.e_result = {
                    "ok": not e_resp.is_fraud,
                    "reasons": list(e_resp.reasons),
                    "vc": list(e_resp.vc),
                }
            else:
                # C failed → skip E, forward failure
                ps.e_result = {
                    "ok": False,
                    "reasons": list(request.reasons),
                    "vc": list(request.vc),
                }
                logger.info(
                    "event=step_skipped step=E reason=C_failed cid=%s", order_id,
                )
            ps.e_done.set()

        # Wait for BOTH D and E to be done
        ps.d_done.wait()
        ps.e_done.wait()

        # Only one thread should run F→G (the one that arrives second)
        with ps.completion_lock:
            if ps.final_response is None:
                ps.final_response = self._complete_pipeline(order_id, ps)
                ps.pipeline_complete.set()

        ps.pipeline_complete.wait()
        return ps.final_response


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
