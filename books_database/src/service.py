from utils.pb.books_database import books_database_pb2 as books_pb2
from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc
import threading
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)
PORT = os.getenv("PORT", "?")
logger = logging.getLogger(f"books_db:{PORT}")

ALL_BOOKS = [
    "1 x 1 = 2: The Terrence Howard Mathematical Revolution",
    "Terryology: Rethinking Physics and Mathematics",
    "Chariots of the Gods",
    "Ancient Aliens and Lost Civilizations",
    "The Flat Earth Doctrine",
    "200 Proofs Earth Is Not a Spinning Ball",
    "The Electric Universe Theory",
    "Hidden Truths About Gravity",
    "The Secret Space Program",
    "Moon Landing Hoax: The Evidence",
    "Crystal Healing and Quantum Energy",
    "Detox Your DNA Naturally",
    "Sacred Geometry and Cosmic Energy",
    "The Vibrational Frequency of the Universe",
]

# Shared logic for all replicas: basic key-value storage and gRPC handlers
class BooksDatabaseService(books_pb2_grpc.BooksDatabaseServicer):
    def __init__(self):
        self.stock = {title: 10 for title in ALL_BOOKS}
        self.lock = threading.Lock()

    def Read(self, request, context):
        with self.lock:
            stock = self.stock.get(request.title, 0)
        logger.info("READ %r stock=%d", request.title, stock)
        return books_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        with self.lock:
            self.stock[request.title] = request.new_stock
        logger.info("WRITE %r new_stock=%d", request.title, request.new_stock)
        return books_pb2.WriteResponse(success=True)

    def DecrementStock(self, request, context):
        with self.lock:
            current = self.stock.get(request.title, 0)
            if current < request.quantity:
                logger.warning(
                    "DECREMENT %r requested=%d available=%d FAILED",
                    request.title, request.quantity, current,
                )
                return books_pb2.WriteResponse(success=False)
            new_stock = current - request.quantity
            self.stock[request.title] = new_stock
        logger.info(
            "DECREMENT %r %d → %d OK",
            request.title, current, new_stock,
        )
        return books_pb2.WriteResponse(success=True)

    def IncrementStock(self, request, context):
        with self.lock:
            old = self.stock.get(request.title, 0)
            new_stock = old + request.quantity
            self.stock[request.title] = new_stock
        logger.info(
            "INCREMENT %r %d → %d OK",
            request.title, old, new_stock,
        )
        return books_pb2.WriteResponse(success=True)

    def ListAvailableBooks(self, request, context):
        with self.lock:
            available = [
                title for title, stock in self.stock.items()
                if stock > 0
            ]
        return books_pb2.AvailableBooksResponse(books=available)


# Primary replica: extends the base class, propagates writes to backups
class PrimaryReplica(BooksDatabaseService):
    def __init__(self, backup_stubs):
        super().__init__()
        self.backups = backup_stubs  # list of gRPC stubs for backup replicas

    def _replicate(self, title, new_stock):
        write_req = books_pb2.WriteRequest(title=title, new_stock=new_stock)
        results = []
        for i, backup in enumerate(self.backups):
            try:
                backup.Write(write_req)
                results.append(f"backup{i+1}=OK")
            except Exception as e:
                results.append(f"backup{i+1}=FAIL({e})")
        logger.info("REPLICATE %r new_stock=%d %s", title, new_stock, " ".join(results))

    def Write(self, request, context):
        with self.lock:
            self.stock[request.title] = request.new_stock
        self._replicate(request.title, request.new_stock)
        return books_pb2.WriteResponse(success=True)

    def DecrementStock(self, request, context):
        with self.lock:
            current = self.stock.get(request.title, 0)
            if current < request.quantity:
                logger.warning(
                    "DECREMENT %r requested=%d available=%d FAILED",
                    request.title, request.quantity, current,
                )
                return books_pb2.WriteResponse(success=False)
            new_stock = current - request.quantity
            self.stock[request.title] = new_stock
        logger.info("DECREMENT %r %d → %d OK", request.title, current, new_stock)
        self._replicate(request.title, new_stock)
        return books_pb2.WriteResponse(success=True)

    def IncrementStock(self, request, context):
        with self.lock:
            new_stock = self.stock.get(request.title, 0) + request.quantity
            self.stock[request.title] = new_stock
        self._replicate(request.title, new_stock)
        return books_pb2.WriteResponse(success=True)