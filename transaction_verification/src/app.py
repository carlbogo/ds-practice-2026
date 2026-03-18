import sys
import os
import re
import threading
from datetime import datetime

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/transaction_verification')
)
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

import grpc
from concurrent import futures
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("transaction_verification")


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
    # Accept MM/YY or MM/YYYY
    match = re.fullmatch(r"(0[1-9]|1[0-2])\/(\d{2}|\d{4})", exp)
    if not match:
        return False

    month = int(match.group(1))
    year_raw = match.group(2)
    year = int(year_raw) + 2000 if len(year_raw) == 2 else int(year_raw)

    now = datetime.utcnow()
    # Card valid through end of expiry month
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
    # Simple international-safe check (3-10, letters/numbers/space/hyphen)
    if not re.fullmatch(r"[A-Za-z0-9 -]{3,10}", zip_code):
        return False
    return any(ch.isdigit() for ch in zip_code)


def _validate_items(items) -> list[str]:
    errors = []
    if len(items) == 0:
        # Reject empty cart
        errors.append("At least one item is required")
        return errors

    for idx, item in enumerate(items):
        if not _is_non_empty(item.name):
            # Reject item with blank name
            errors.append(f"Item at index {idx} must have a name")
        if item.quantity <= 0:
            # Reject item with non-positive quantity
            errors.append(f"Item '{item.name or idx}' must have quantity > 0")
    return errors


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

                self._merge_and_increment(entry["vc"], request.vc)
                vc_snapshot = entry["vc"][:]
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

                self._merge_and_increment(entry["vc"], request.vc)
                vc_snapshot = entry["vc"][:]

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

                self._merge_and_increment(entry["vc"], request.vc)
                vc_snapshot = entry["vc"][:]

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

