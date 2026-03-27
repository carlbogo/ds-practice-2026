import sys
import os
import uuid
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc
from flask import Flask, request
from flask_cors import CORS

# gRPC imports
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/transaction_verification")
)
sys.path.insert(0, transaction_verification_grpc_path)

fraud_detection_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/fraud_detection")
)
sys.path.insert(0, fraud_detection_grpc_path)

suggestions_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/suggestions")
)
sys.path.insert(0, suggestions_grpc_path)

order_queue_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/order_queue")
)
sys.path.insert(0, order_queue_grpc_path)

import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("orchestrator")

RPC_TIMEOUT_SECONDS = float(os.getenv("GRPC_TIMEOUT_SECONDS", "3.0"))
TRANSACTION_VERIFICATION_ADDR = os.getenv(
    "TRANSACTION_VERIFICATION_ADDR",
    "transaction_verification:50052",
)
FRAUD_DETECTION_ADDR = os.getenv("FRAUD_DETECTION_ADDR", "fraud_detection:50051")
SUGGESTIONS_ADDR = os.getenv("SUGGESTIONS_ADDR", "suggestions:50053")
ORDER_QUEUE_ADDR = os.getenv("ORDER_QUEUE_ADDR", "order_queue:50054")

transaction_verification_channel = grpc.insecure_channel(
    TRANSACTION_VERIFICATION_ADDR
)
transaction_verification_stub = (
    transaction_verification_grpc.TransactionVerificationServiceStub(
        transaction_verification_channel
    )
)

fraud_detection_channel = grpc.insecure_channel(FRAUD_DETECTION_ADDR)
fraud_detection_stub = fraud_detection_grpc.FraudDetectionServiceStub(
    fraud_detection_channel
)

suggestions_channel = grpc.insecure_channel(SUGGESTIONS_ADDR)
suggestions_stub = suggestions_grpc.SuggestionsServiceStub(suggestions_channel)

order_queue_channel = grpc.insecure_channel(ORDER_QUEUE_ADDR)
order_queue_stub = order_queue_grpc.OrderQueueServiceStub(order_queue_channel)


def _merge_vc(*vectors):
    merged = [0, 0, 0]

    for vector in vectors:
        current = list(vector or [])
        for i in range(min(len(current), len(merged))):
            merged[i] = max(merged[i], current[i])

    return merged


def _workflow_result(ok, reasons=None, suggested_books=None, status_code=None):
    result = {
        "ok": ok,
        "reasons": list(reasons or []),
        "suggestedBooks": list(suggested_books or []),
    }
    if status_code is not None:
        result["status_code"] = status_code
    return result


def _reject_result(reasons):
    return _workflow_result(
        ok=True,
        reasons=list(reasons or []),
        suggested_books=[],
    )


def _validation_outcome(response):
    return response.is_valid, list(response.reasons), list(response.vc)


def _fraud_outcome(response):
    ok = not response.is_fraud
    return ok, list(response.reasons), list(response.vc)


def _build_transaction_init_request(checkout_data):
    user = checkout_data.get("user", {})
    credit_card = checkout_data.get("creditCard", {})
    billing_address = checkout_data.get("billingAddress", {})

    items = [
        transaction_verification.Item(
            name=str(item.get("name", "")),
            quantity=int(item.get("quantity", 0)),
        )
        for item in checkout_data.get("items", [])
    ]

    return transaction_verification.InitOrderRequest(
        order_id=checkout_data.get("orderId", ""),
        purchaser_name=user.get("name", ""),
        purchaser_email=user.get("contact", ""),
        credit_card_number=credit_card.get("number", ""),
        credit_card_expiration=credit_card.get("expirationDate", ""),
        credit_card_cvv=credit_card.get("cvv", ""),
        billing_street=billing_address.get("street", ""),
        billing_city=billing_address.get("city", ""),
        billing_state=billing_address.get("state", ""),
        billing_zip=billing_address.get("zip", ""),
        billing_country=billing_address.get("country", ""),
        items=items,
    )


def _build_fraud_init_request(checkout_data):
    user = checkout_data.get("user", {})
    credit_card = checkout_data.get("creditCard", {})

    return fraud_detection.InitOrderRequest(
        order_id=checkout_data.get("orderId", ""),
        purchaser_email=user.get("contact", ""),
        credit_card_number=credit_card.get("number", ""),
    )


def _build_suggestions_request(checkout_data, vc):
    purchased_books = [
        str(item.get("name", ""))
        for item in checkout_data.get("items", [])
        if str(item.get("name", "")).strip()
    ]

    return suggestions.SuggestionsRequest(
        order_id=checkout_data.get("orderId", ""),
        purchased_books=purchased_books,
        mode=str(checkout_data.get("suggestionMode", "author")),
        vc=vc,
    )


def _init_transaction_order(checkout_data):
    return transaction_verification_stub.InitOrder(
        _build_transaction_init_request(checkout_data),
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _init_fraud_order(checkout_data):
    return fraud_detection_stub.InitOrder(
        _build_fraud_init_request(checkout_data),
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _init_suggestions_order(checkout_data):
    request_message = suggestions.InitOrderRequest(
        order_id=checkout_data.get("orderId", ""),
    )
    return suggestions_stub.InitOrder(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _check_items(order_id, vc):
    request_message = transaction_verification.TransactionEventRequest(
        order_id=order_id,
        vc=vc,
    )
    return transaction_verification_stub.CheckItems(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _check_user_and_billing_data(order_id, vc):
    request_message = transaction_verification.TransactionEventRequest(
        order_id=order_id,
        vc=vc,
    )
    return transaction_verification_stub.CheckUserAndBillingData(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _check_payment_data(order_id, vc):
    request_message = transaction_verification.TransactionEventRequest(
        order_id=order_id,
        vc=vc,
    )
    return transaction_verification_stub.CheckPaymentData(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _check_email_fraud(order_id, vc):
    request_message = fraud_detection.FraudEventRequest(
        order_id=order_id,
        vc=vc,
    )
    return fraud_detection_stub.CheckEmailFraud(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _check_card_fraud(order_id, vc):
    request_message = fraud_detection.FraudEventRequest(
        order_id=order_id,
        vc=vc,
    )
    return fraud_detection_stub.CheckCardFraud(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _check_risk_score_fraud(order_id, vc):
    request_message = fraud_detection.FraudEventRequest(
        order_id=order_id,
        vc=vc,
    )
    return fraud_detection_stub.CheckRiskScoreFraud(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _get_suggestions(request_message):
    return suggestions_stub.GetSuggestions(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _finalize_transaction_order(order_id, vc):
    request_message = transaction_verification.FinalizeOrderRequest(
        order_id=order_id,
        vc=vc,
    )
    return transaction_verification_stub.FinalizeOrder(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _finalize_fraud_order(order_id, vc):
    request_message = fraud_detection.FinalizeOrderRequest(
        order_id=order_id,
        vc=vc,
    )
    return fraud_detection_stub.FinalizeOrder(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


def _finalize_suggestions_order(order_id, vc):
    request_message = suggestions.FinalizeOrderRequest(
        order_id=order_id,
        vc=vc,
    )
    return suggestions_stub.FinalizeOrder(
        request_message,
        timeout=RPC_TIMEOUT_SECONDS,
    )


class WorkflowRejected(Exception):
    def __init__(self, reasons):
        super().__init__(", ".join(reasons or []))
        self.reasons = list(reasons or [])


class WorkflowState:
    def __init__(self):
        self.lock = threading.Lock()
        self.done_event = threading.Event()

        self.infrastructure_failed = False
        self.infrastructure_reasons = []
        self.status_code = 503

        self.business_reasons = []
        self.business_reason_set = set()

        self.suggested_books = []

        self.vc_by_step = {}
        self.pending_steps = set()
        self.successful_steps = set()
        self.failed_steps = set()

        self.initialized_services = set()

        self.transaction_init_vc = [0, 0, 0]
        self.fraud_init_vc = [0, 0, 0]
        self.suggestions_init_vc = [0, 0, 0]
        self.base_vc = [0, 0, 0]

    def mark_submitted(self, step_name):
        with self.lock:
            if self.infrastructure_failed:
                return False
            if step_name in self.pending_steps:
                return False
            if step_name in self.successful_steps:
                return False
            if step_name in self.failed_steps:
                return False
            self.pending_steps.add(step_name)
            return True

    def mark_success(self, step_name, vc):
        with self.lock:
            self.vc_by_step[step_name] = list(vc or [])
            self.successful_steps.add(step_name)

    def mark_business_failure(self, step_name, reasons):
        with self.lock:
            self.failed_steps.add(step_name)
            for reason in list(reasons or []):
                if reason not in self.business_reason_set:
                    self.business_reason_set.add(reason)
                    self.business_reasons.append(reason)

    def mark_infrastructure_failure(self, reasons, status_code=503):
        with self.lock:
            if self.infrastructure_failed:
                return
            self.infrastructure_failed = True
            self.infrastructure_reasons = list(reasons or [])
            self.status_code = status_code
            self.done_event.set()

    def finish_step(self, step_name):
        with self.lock:
            self.pending_steps.discard(step_name)

            if self.infrastructure_failed:
                self.done_event.set()
                return

            if "G" in self.successful_steps:
                self.done_event.set()
                return

            if not self.pending_steps and self.business_reasons:
                self.done_event.set()

    def get_vc(self, step_name):
        with self.lock:
            return list(self.vc_by_step.get(step_name, []))

    def can_submit_f(self):
        with self.lock:
            if self.infrastructure_failed:
                return False
            if "F" in self.pending_steps or "F" in self.successful_steps or "F" in self.failed_steps:
                return False
            return "D" in self.successful_steps and "E" in self.successful_steps

    def build_finalize_vc(self):
        with self.lock:
            return _merge_vc(
                self.transaction_init_vc,
                self.fraud_init_vc,
                self.suggestions_init_vc,
                self.vc_by_step.get("A"),
                self.vc_by_step.get("B"),
                self.vc_by_step.get("C"),
                self.vc_by_step.get("D"),
                self.vc_by_step.get("E"),
                self.vc_by_step.get("F"),
                self.vc_by_step.get("G"),
            )


def _run_validation_step(step_fn, order_id, input_vc):
    response = step_fn(order_id, input_vc)
    ok, reasons, output_vc = _validation_outcome(response)
    if not ok:
        raise WorkflowRejected(reasons)
    return output_vc


def _run_fraud_step(step_fn, order_id, input_vc):
    response = step_fn(order_id, input_vc)
    ok, reasons, output_vc = _fraud_outcome(response)
    if not ok:
        raise WorkflowRejected(reasons)
    return output_vc


def run_checkout_workflow(checkout_data, correlation_id):
    state = WorkflowState()

    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            logger.info("event=init_submit services=transaction_verification,fraud_detection,suggestions cid=%s", correlation_id)

            future_tx_init = executor.submit(_init_transaction_order, checkout_data)
            future_fraud_init = executor.submit(_init_fraud_order, checkout_data)
            future_sugg_init = executor.submit(_init_suggestions_order, checkout_data)

            tx_init = future_tx_init.result()
            fraud_init = future_fraud_init.result()
            sugg_init = future_sugg_init.result()

            if not tx_init.ok:
                logger.error(
                    "service=transaction_verification event=init_failed cid=%s",
                    correlation_id,
                )
                return _workflow_result(
                    ok=False,
                    reasons=["Transaction verification init failed"],
                    status_code=503,
                )
            state.initialized_services.add("transaction_verification")
            state.transaction_init_vc = list(tx_init.vc)

            if not fraud_init.ok:
                logger.error(
                    "service=fraud_detection event=init_failed cid=%s",
                    correlation_id,
                )
                return _workflow_result(
                    ok=False,
                    reasons=["Fraud service init failed"],
                    status_code=503,
                )
            state.initialized_services.add("fraud_detection")
            state.fraud_init_vc = list(fraud_init.vc)

            if not sugg_init.ok:
                logger.error(
                    "service=suggestions event=init_failed cid=%s",
                    correlation_id,
                )
                return _workflow_result(
                    ok=False,
                    reasons=["Suggestions init failed"],
                    status_code=503,
                )
            state.initialized_services.add("suggestions")
            state.suggestions_init_vc = list(sugg_init.vc)

            state.base_vc = _merge_vc(
                state.transaction_init_vc,
                state.fraud_init_vc,
                state.suggestions_init_vc,
            )

            logger.info(
                "event=init_completed tx_vc=%s fraud_vc=%s suggestions_vc=%s base_vc=%s cid=%s",
                state.transaction_init_vc,
                state.fraud_init_vc,
                state.suggestions_init_vc,
                state.base_vc,
                correlation_id,
            )

            def submit_a():
                if not state.mark_submitted("A"):
                    return

                input_vc = list(state.base_vc)
                logger.info(
                    "service=transaction_verification event=step_submitted step=A input_vc=%s cid=%s",
                    input_vc,
                    correlation_id,
                )

                future = executor.submit(
                    _run_validation_step,
                    _check_items,
                    correlation_id,
                    input_vc,
                )

                def on_done(done_future):
                    try:
                        output_vc = done_future.result()
                        state.mark_success("A", output_vc)
                        logger.info(
                            "service=transaction_verification event=step_completed step=A outcome=success output_vc=%s next=C cid=%s",
                            output_vc,
                            correlation_id,
                        )
                        submit_c()
                    except WorkflowRejected as error:
                        state.mark_business_failure("A", error.reasons)
                        logger.warning(
                            "service=transaction_verification event=step_completed step=A outcome=reject reasons=%s cid=%s",
                            error.reasons,
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=transaction_verification event=step_completed step=A outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=transaction_verification event=step_completed step=A outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("A")

                future.add_done_callback(on_done)

            def submit_b():
                if not state.mark_submitted("B"):
                    return

                input_vc = list(state.base_vc)
                logger.info(
                    "service=transaction_verification event=step_submitted step=B input_vc=%s cid=%s",
                    input_vc,
                    correlation_id,
                )

                future = executor.submit(
                    _run_validation_step,
                    _check_user_and_billing_data,
                    correlation_id,
                    input_vc,
                )

                def on_done(done_future):
                    try:
                        output_vc = done_future.result()
                        state.mark_success("B", output_vc)
                        logger.info(
                            "service=transaction_verification event=step_completed step=B outcome=success output_vc=%s next=D cid=%s",
                            output_vc,
                            correlation_id,
                        )
                        submit_d()
                    except WorkflowRejected as error:
                        state.mark_business_failure("B", error.reasons)
                        logger.warning(
                            "service=transaction_verification event=step_completed step=B outcome=reject reasons=%s cid=%s",
                            error.reasons,
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=transaction_verification event=step_completed step=B outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=transaction_verification event=step_completed step=B outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("B")

                future.add_done_callback(on_done)

            def submit_c():
                if not state.mark_submitted("C"):
                    return

                input_vc = state.get_vc("A")
                logger.info(
                    "service=transaction_verification event=step_submitted step=C input_vc=%s cid=%s",
                    input_vc,
                    correlation_id,
                )

                future = executor.submit(
                    _run_validation_step,
                    _check_payment_data,
                    correlation_id,
                    input_vc,
                )

                def on_done(done_future):
                    try:
                        output_vc = done_future.result()
                        state.mark_success("C", output_vc)
                        logger.info(
                            "service=transaction_verification event=step_completed step=C outcome=success output_vc=%s next=E cid=%s",
                            output_vc,
                            correlation_id,
                        )
                        submit_e()
                    except WorkflowRejected as error:
                        state.mark_business_failure("C", error.reasons)
                        logger.warning(
                            "service=transaction_verification event=step_completed step=C outcome=reject reasons=%s cid=%s",
                            error.reasons,
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=transaction_verification event=step_completed step=C outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=transaction_verification event=step_completed step=C outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("C")

                future.add_done_callback(on_done)

            def submit_d():
                if not state.mark_submitted("D"):
                    return

                input_vc = state.get_vc("B")
                logger.info(
                    "service=fraud_detection event=step_submitted step=D input_vc=%s cid=%s",
                    input_vc,
                    correlation_id,
                )

                future = executor.submit(
                    _run_fraud_step,
                    _check_email_fraud,
                    correlation_id,
                    input_vc,
                )

                def on_done(done_future):
                    try:
                        output_vc = done_future.result()
                        state.mark_success("D", output_vc)
                        logger.info(
                            "service=fraud_detection event=step_completed step=D outcome=success output_vc=%s next=F_waiting_for_E cid=%s",
                            output_vc,
                            correlation_id,
                        )
                        maybe_submit_f()
                    except WorkflowRejected as error:
                        state.mark_business_failure("D", error.reasons)
                        logger.warning(
                            "service=fraud_detection event=step_completed step=D outcome=reject reasons=%s cid=%s",
                            error.reasons,
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=fraud_detection event=step_completed step=D outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=fraud_detection event=step_completed step=D outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("D")

                future.add_done_callback(on_done)

            def submit_e():
                if not state.mark_submitted("E"):
                    return

                input_vc = state.get_vc("C")
                logger.info(
                    "service=fraud_detection event=step_submitted step=E input_vc=%s cid=%s",
                    input_vc,
                    correlation_id,
                )

                future = executor.submit(
                    _run_fraud_step,
                    _check_card_fraud,
                    correlation_id,
                    input_vc,
                )

                def on_done(done_future):
                    try:
                        output_vc = done_future.result()
                        state.mark_success("E", output_vc)
                        logger.info(
                            "service=fraud_detection event=step_completed step=E outcome=success output_vc=%s next=F_waiting_for_D cid=%s",
                            output_vc,
                            correlation_id,
                        )
                        maybe_submit_f()
                    except WorkflowRejected as error:
                        state.mark_business_failure("E", error.reasons)
                        logger.warning(
                            "service=fraud_detection event=step_completed step=E outcome=reject reasons=%s cid=%s",
                            error.reasons,
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=fraud_detection event=step_completed step=E outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=fraud_detection event=step_completed step=E outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("E")

                future.add_done_callback(on_done)

            def maybe_submit_f():
                if not state.can_submit_f():
                    return

                if not state.mark_submitted("F"):
                    return

                input_vc = _merge_vc(state.get_vc("D"), state.get_vc("E"))
                logger.info(
                    "service=fraud_detection event=step_submitted step=F input_vc=%s cid=%s",
                    input_vc,
                    correlation_id,
                )

                future = executor.submit(
                    _run_fraud_step,
                    _check_risk_score_fraud,
                    correlation_id,
                    input_vc,
                )

                def on_done(done_future):
                    try:
                        output_vc = done_future.result()
                        state.mark_success("F", output_vc)
                        logger.info(
                            "service=fraud_detection event=step_completed step=F outcome=success output_vc=%s next=G cid=%s",
                            output_vc,
                            correlation_id,
                        )
                        submit_g()
                    except WorkflowRejected as error:
                        state.mark_business_failure("F", error.reasons)
                        logger.warning(
                            "service=fraud_detection event=step_completed step=F outcome=reject reasons=%s cid=%s",
                            error.reasons,
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=fraud_detection event=step_completed step=F outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=fraud_detection event=step_completed step=F outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("F")

                future.add_done_callback(on_done)

            def submit_g():
                if not state.mark_submitted("G"):
                    return

                input_vc = state.get_vc("F")
                request_message = _build_suggestions_request(
                    checkout_data,
                    input_vc,
                )

                logger.info(
                    "service=suggestions event=step_submitted step=G input_vc=%s mode=%s cid=%s",
                    input_vc,
                    checkout_data.get("suggestionMode", "author"),
                    correlation_id,
                )

                future = executor.submit(_get_suggestions, request_message)

                def on_done(done_future):
                    try:
                        response = done_future.result()
                        state.suggested_books = list(response.suggested_books)
                        state.mark_success("G", list(response.vc))
                        logger.info(
                            "service=suggestions event=step_completed step=G outcome=success output_vc=%s suggested_books_count=%s cid=%s",
                            list(response.vc),
                            len(state.suggested_books),
                            correlation_id,
                        )
                    except grpc.RpcError as error:
                        logger.error(
                            "service=suggestions event=step_completed step=G outcome=rpc_error code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["A dependent service is unavailable"],
                            status_code=503,
                        )
                    except Exception:
                        logger.exception(
                            "service=suggestions event=step_completed step=G outcome=unexpected_error cid=%s",
                            correlation_id,
                        )
                        state.mark_infrastructure_failure(
                            ["Unexpected orchestrator error"],
                            status_code=503,
                        )
                    finally:
                        state.finish_step("G")

                future.add_done_callback(on_done)

            submit_a()
            submit_b()
            state.done_event.wait()

        if state.infrastructure_failed:
            logger.error(
                "event=workflow_completed outcome=infrastructure_failure reasons=%s status_code=%s final_vc=%s cid=%s",
                state.infrastructure_reasons,
                state.status_code,
                state.build_finalize_vc(),
                correlation_id,
            )
            return _workflow_result(
                ok=False,
                reasons=state.infrastructure_reasons or ["A dependent service is unavailable"],
                status_code=state.status_code,
            )

        if state.business_reasons:
            logger.warning(
                "event=workflow_completed outcome=reject reasons=%s final_vc=%s cid=%s",
                state.business_reasons,
                state.build_finalize_vc(),
                correlation_id,
            )
            return _reject_result(state.business_reasons)

        logger.info(
            "event=workflow_completed outcome=approved suggested_books=[%s] final_vc=%s cid=%s",
            ", ".join(state.suggested_books),
            state.build_finalize_vc(),
            correlation_id,
        )
        return _workflow_result(
            ok=True,
            reasons=[],
            suggested_books=state.suggested_books,
        )

    except grpc.RpcError as error:
        logger.error(
            "event=checkout_workflow_rpc_failed code=%s details=%s cid=%s",
            error.code(),
            error.details(),
            correlation_id,
        )
        return _workflow_result(
            ok=False,
            reasons=["A dependent service is unavailable"],
            status_code=503,
        )

    finally:
        if state.initialized_services:
            finalize_vc = state.build_finalize_vc()

            logger.info(
                "event=finalize_started services=%s finalize_vc=%s cid=%s",
                sorted(state.initialized_services),
                finalize_vc,
                correlation_id,
            )

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []

                if "transaction" in state.initialized_services:
                    futures.append(
                        executor.submit(
                            _finalize_transaction_order,
                            correlation_id,
                            finalize_vc,
                        )
                    )

                if "fraud" in state.initialized_services:
                    futures.append(
                        executor.submit(
                            _finalize_fraud_order,
                            correlation_id,
                            finalize_vc,
                        )
                    )

                if "suggestions" in state.initialized_services:
                    futures.append(
                        executor.submit(
                            _finalize_suggestions_order,
                            correlation_id,
                            finalize_vc,
                        )
                    )

                for future in futures:
                    try:
                        future.result()
                    except grpc.RpcError as error:
                        logger.error(
                            "event=checkout_finalize_rpc_failed code=%s details=%s cid=%s",
                            error.code(),
                            error.details(),
                            correlation_id,
                        )
                    except Exception:
                        logger.exception(
                            "event=checkout_finalize_unexpected_error cid=%s",
                            correlation_id,
                        )


def _enqueue_order(checkout_data, suggested_books, correlation_id):
    """Enqueue an approved order to the order queue for execution."""
    user = checkout_data.get("user", {})
    credit_card = checkout_data.get("creditCard", {})
    billing_address = checkout_data.get("billingAddress", {})

    items = [
        order_queue.OrderItem(
            name=str(item.get("name", "")),
            quantity=int(item.get("quantity", 0)),
        )
        for item in checkout_data.get("items", [])
    ]

    enqueue_request = order_queue.EnqueueRequest(
        order_id=correlation_id,
        purchaser_name=user.get("name", ""),
        purchaser_email=user.get("contact", ""),
        credit_card_number=credit_card.get("number", ""),
        credit_card_expiration=credit_card.get("expirationDate", ""),
        credit_card_cvv=credit_card.get("cvv", ""),
        billing_street=billing_address.get("street", ""),
        billing_city=billing_address.get("city", ""),
        billing_state=billing_address.get("state", ""),
        billing_zip=billing_address.get("zip", ""),
        billing_country=billing_address.get("country", ""),
        items=items,
        suggested_books=list(suggested_books or []),
    )

    response = order_queue_stub.Enqueue(enqueue_request, timeout=RPC_TIMEOUT_SECONDS)
    return response.ok


@app.route("/checkout", methods=["POST"])
def checkout():
    request_data = request.get_json(silent=True)
    if not isinstance(request_data, dict):
        return {"error": {"message": "Invalid JSON payload"}}, 400

    correlation_id = str(request_data.get("orderId") or uuid.uuid4())
    request_data["orderId"] = correlation_id

    result = run_checkout_workflow(request_data, correlation_id)

    if not result["ok"]:
        return {
            "status": "Order Rejected",
            "reasons": result["reasons"],
            "suggestedBooks": [],
        }, result.get("status_code", 500)

    if result["reasons"]:
        return {
            "status": "Order Rejected",
            "reasons": result["reasons"],
            "suggestedBooks": [],
        }

    # Order approved — enqueue for execution
    try:
        enqueued = _enqueue_order(
            request_data, result["suggestedBooks"], correlation_id,
        )
        if enqueued:
            logger.info("event=order_enqueued cid=%s", correlation_id)
        else:
            logger.error("event=enqueue_failed cid=%s", correlation_id)
    except grpc.RpcError as error:
        logger.error(
            "event=enqueue_rpc_failed code=%s details=%s cid=%s",
            error.code(), error.details(), correlation_id,
        )

    return {
        "status": "Order Approved",
        "reasons": [],
        "suggestedBooks": result["suggestedBooks"],
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0")
