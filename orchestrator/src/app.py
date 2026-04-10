import sys
import os
import uuid
import logging

import grpc
from flask import Flask, request
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor

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
PIPELINE_TIMEOUT = float(os.getenv("PIPELINE_TIMEOUT_SECONDS", "30.0"))

TRANSACTION_VERIFICATION_ADDR = os.getenv(
    "TRANSACTION_VERIFICATION_ADDR",
    "transaction_verification:50052",
)
FRAUD_DETECTION_ADDR = os.getenv("FRAUD_DETECTION_ADDR", "fraud_detection:50051")
SUGGESTIONS_ADDR = os.getenv("SUGGESTIONS_ADDR", "suggestions:50053")
ORDER_QUEUE_ADDR = os.getenv("ORDER_QUEUE_ADDR", "order_queue:50054")

transaction_verification_channel = grpc.insecure_channel(TRANSACTION_VERIFICATION_ADDR)
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


# ------------------------------------------------------------------
# Init helpers (unchanged)
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Finalize helpers (unchanged)
# ------------------------------------------------------------------

def _finalize_transaction_order(order_id, vc):
    request_message = transaction_verification.FinalizeOrderRequest(
        order_id=order_id, vc=vc,
    )
    return transaction_verification_stub.FinalizeOrder(
        request_message, timeout=RPC_TIMEOUT_SECONDS,
    )


def _finalize_fraud_order(order_id, vc):
    request_message = fraud_detection.FinalizeOrderRequest(
        order_id=order_id, vc=vc,
    )
    return fraud_detection_stub.FinalizeOrder(
        request_message, timeout=RPC_TIMEOUT_SECONDS,
    )


def _finalize_suggestions_order(order_id, vc):
    request_message = suggestions.FinalizeOrderRequest(
        order_id=order_id, vc=vc,
    )
    return suggestions_stub.FinalizeOrder(
        request_message, timeout=RPC_TIMEOUT_SECONDS,
    )


# ------------------------------------------------------------------
# Main workflow
# ------------------------------------------------------------------

def run_checkout_workflow(checkout_data, correlation_id):
    initialized_services = set()
    finalize_vc = [0, 0, 0]

    try:
        # --- Phase 1: Init all 3 services in parallel ---
        logger.info(
            "event=init_submit services=transaction_verification,fraud_detection,suggestions cid=%s",
            correlation_id,
        )

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_tx = executor.submit(_init_transaction_order, checkout_data)
            future_fraud = executor.submit(_init_fraud_order, checkout_data)
            future_sugg = executor.submit(_init_suggestions_order, checkout_data)

            tx_init = future_tx.result()
            fraud_init = future_fraud.result()
            sugg_init = future_sugg.result()

        if not tx_init.ok:
            logger.error("service=transaction_verification event=init_failed cid=%s", correlation_id)
            return {"ok": False, "reasons": ["Transaction verification init failed"], "suggestedBooks": [], "status_code": 503}
        initialized_services.add("transaction_verification")

        if not fraud_init.ok:
            logger.error("service=fraud_detection event=init_failed cid=%s", correlation_id)
            return {"ok": False, "reasons": ["Fraud service init failed"], "suggestedBooks": [], "status_code": 503}
        initialized_services.add("fraud_detection")

        if not sugg_init.ok:
            logger.error("service=suggestions event=init_failed cid=%s", correlation_id)
            return {"ok": False, "reasons": ["Suggestions init failed"], "suggestedBooks": [], "status_code": 503}
        initialized_services.add("suggestions")

        base_vc = _merge_vc(
            list(tx_init.vc),
            list(fraud_init.vc),
            list(sugg_init.vc),
        )

        logger.info(
            "event=init_completed tx_vc=%s fraud_vc=%s suggestions_vc=%s base_vc=%s cid=%s",
            list(tx_init.vc), list(fraud_init.vc), list(sugg_init.vc), base_vc,
            correlation_id,
        )

        # --- Phase 2: Start the peer-to-peer workflow (single blocking call) ---
        purchased_books = [
            str(item.get("name", ""))
            for item in checkout_data.get("items", [])
            if str(item.get("name", "")).strip()
        ]
        suggestion_mode = str(checkout_data.get("suggestionMode", "author"))

        workflow_request = transaction_verification.StartWorkflowRequest(
            order_id=correlation_id,
            vc=base_vc,
            purchased_books=purchased_books,
            suggestion_mode=suggestion_mode,
        )

        logger.info("event=workflow_started cid=%s", correlation_id)

        workflow_response = transaction_verification_stub.StartWorkflow(
            workflow_request,
            timeout=PIPELINE_TIMEOUT,
        )

        finalize_vc = list(workflow_response.vc) if workflow_response.vc else base_vc
        reasons = list(workflow_response.reasons)
        suggested_books = list(workflow_response.suggested_books)

        if not workflow_response.ok or reasons:
            logger.warning(
                "event=workflow_completed outcome=reject reasons=%s final_vc=%s cid=%s",
                reasons, finalize_vc, correlation_id,
            )
            return {
                "ok": True,
                "reasons": reasons,
                "suggestedBooks": [],
            }

        logger.info(
            "event=workflow_completed outcome=approved suggested_books=[%s] final_vc=%s cid=%s",
            ", ".join(suggested_books), finalize_vc, correlation_id,
        )
        return {
            "ok": True,
            "reasons": [],
            "suggestedBooks": suggested_books,
        }

    except grpc.RpcError as error:
        logger.error(
            "event=checkout_workflow_rpc_failed code=%s details=%s cid=%s",
            error.code(), error.details(), correlation_id,
        )
        return {
            "ok": False,
            "reasons": ["A dependent service is unavailable"],
            "suggestedBooks": [],
            "status_code": 503,
        }

    finally:
        if initialized_services:
            logger.info(
                "event=finalize_started services=%s finalize_vc=%s cid=%s",
                sorted(initialized_services), finalize_vc, correlation_id,
            )

            # Cleanup - clear the caches in all services
            with ThreadPoolExecutor(max_workers=3) as executor:
                finalize_futures = []

                if "transaction_verification" in initialized_services:
                    finalize_futures.append(
                        executor.submit(_finalize_transaction_order, correlation_id, finalize_vc)
                    )
                if "fraud_detection" in initialized_services:
                    finalize_futures.append(
                        executor.submit(_finalize_fraud_order, correlation_id, finalize_vc)
                    )
                if "suggestions" in initialized_services:
                    finalize_futures.append(
                        executor.submit(_finalize_suggestions_order, correlation_id, finalize_vc)
                    )

                for f in finalize_futures:
                    try:
                        f.result()
                    except grpc.RpcError as error:
                        logger.error(
                            "event=checkout_finalize_rpc_failed code=%s details=%s cid=%s",
                            error.code(), error.details(), correlation_id,
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
