import os
import logging
import grpc
from concurrent import futures

from utils.pb.payment import payment_pb2
from utils.pb.payment import payment_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("payment")

PORT = os.getenv("PORT", "50056")


class PaymentService(payment_pb2_grpc.PaymentServiceServicer):

    def __init__(self):
        self.prepared = False

    def Prepare(self, request, context):
        # Dummy validation logic, e.g. check funds
        self.prepared = True
        logger.info(
            "PREPARE order_id=%s card=****%s ready=True",
            request.order_id,
            request.credit_card_number[-4:] if request.credit_card_number else "????",
        )
        return payment_pb2.PaymentPrepareResponse(ready=True)

    def Commit(self, request, context):
        if self.prepared:
            logger.info("COMMIT order_id=%s payment executed", request.order_id)
            self.prepared = False
        return payment_pb2.PaymentCommitResponse(success=True)

    def Abort(self, request, context):
        self.prepared = False
        logger.info("ABORT order_id=%s payment cancelled", request.order_id)
        return payment_pb2.PaymentAbortResponse(aborted=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    logger.info("Server started. Listening on port %s.", PORT)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
