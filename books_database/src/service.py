from utils.pb.books_database import books_database_pb2 as books_pb2
from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc
import threading

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

class BooksDatabaseService(books_pb2_grpc.BooksDatabaseServicer):
    def __init__(self):
        self.stock = {title: 10 for title in ALL_BOOKS}
        self.lock = threading.Lock()

    def Read(self, request, context):
        return books_pb2.ReadResponse(
            stock=self.stock.get(request.title, 0)
        )

    def Write(self, request, context):
        with self.lock:
            self.stock[request.title] = request.new_stock
        return books_pb2.WriteResponse(success=True)

    def DecrementStock(self, request, context):
        with self.lock:
            current = self.stock.get(request.title, 0)

            if current < request.quantity:
                return books_pb2.WriteResponse(success=False)

            new_stock = current - request.quantity
            self.stock[request.title] = new_stock

        return books_pb2.WriteResponse(success=True)

    def ListAvailableBooks(self, request, context):
        with self.lock:
            available = [
                title for title, stock in self.stock.items()
                if stock > 0
            ]
        return books_pb2.AvailableBooksResponse(books=available)


class PrimaryReplica(BooksDatabaseService):
    def __init__(self, backups):
        super().__init__()
        self.backups = backups

    def DecrementStock(self, request, context):
        with self.lock:
            current = self.stock.get(request.title, 0)

            if current < request.quantity:
                return books_pb2.WriteResponse(success=False)

            new_stock = current - request.quantity
            self.stock[request.title] = new_stock

        write_req = books_pb2.WriteRequest(
            title=request.title,
            new_stock=new_stock
        )

        success_count = 0

        for backup in self.backups:
            try:
                backup.Write(write_req, timeout=2.0)
                success_count += 1
            except:
                pass

        if success_count < len(self.backups):
            print("Warning: not fully replicated")

        return books_pb2.WriteResponse(success=True)