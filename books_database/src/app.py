import os
import grpc
from concurrent import futures

from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc
from utils.pb.books_database import books_database_pb2 as books_pb2
from service import PrimaryReplica, BooksDatabaseService

PORT = os.getenv("PORT", "50060")
IS_PRIMARY = os.getenv("IS_PRIMARY", "true").lower() == "true"


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    if IS_PRIMARY:
        # connect to backups
        backup_addresses = os.getenv("BACKUPS", "").split(",")

        stubs = []
        for addr in backup_addresses:
            if addr:
                channel = grpc.insecure_channel(addr)
                stubs.append(books_pb2_grpc.BooksDatabaseStub(channel))

        service = PrimaryReplica(stubs)
    else:
        service = BooksDatabaseService()

    books_pb2_grpc.add_BooksDatabaseServicer_to_server(service, server)

    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    print(f"Books DB running on {PORT}, primary={IS_PRIMARY}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()