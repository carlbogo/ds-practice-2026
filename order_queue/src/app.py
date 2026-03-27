import sys
import os
import logging
import threading
from collections import deque
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/order_queue')
)
sys.path.insert(0, order_queue_grpc_path)

import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc
import grpc

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("order_queue")


class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):

    def __init__(self):
        self.queue = deque()
        self.lock = threading.Lock()

    def Enqueue(self, request, context):
        with self.lock:
            order_data = {
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
                "suggested_books": list(request.suggested_books),
            }
            self.queue.append(order_data)
            logger.info(
                "event=enqueued order_id=%s queue_size=%d",
                request.order_id,
                len(self.queue),
            )
        return order_queue.EnqueueResponse(ok=True)

    def Dequeue(self, request, context):
        with self.lock:
            if not self.queue:
                return order_queue.DequeueResponse(ok=False)

            order_data = self.queue.popleft()
            logger.info(
                "event=dequeued order_id=%s queue_size=%d",
                order_data["order_id"],
                len(self.queue),
            )

        items = [
            order_queue.OrderItem(name=item["name"], quantity=item["quantity"])
            for item in order_data["items"]
        ]

        return order_queue.DequeueResponse(
            ok=True,
            order_id=order_data["order_id"],
            purchaser_name=order_data["purchaser_name"],
            purchaser_email=order_data["purchaser_email"],
            credit_card_number=order_data["credit_card_number"],
            credit_card_expiration=order_data["credit_card_expiration"],
            credit_card_cvv=order_data["credit_card_cvv"],
            billing_street=order_data["billing_street"],
            billing_city=order_data["billing_city"],
            billing_state=order_data["billing_state"],
            billing_zip=order_data["billing_zip"],
            billing_country=order_data["billing_country"],
            items=items,
            suggested_books=order_data["suggested_books"],
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_queue_grpc.add_OrderQueueServiceServicer_to_server(
        OrderQueueService(), server,
    )
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port %s.", port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
