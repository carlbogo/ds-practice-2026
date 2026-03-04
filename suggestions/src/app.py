from utils.pb.suggestions import suggestions_pb2 as suggestions
from utils.pb.suggestions import suggestions_pb2_grpc as suggestions_grpc

import grpc
from concurrent import futures
import random


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


class SuggestionsService(suggestions_grpc.SuggestionsServiceServicer):

    def GetSuggestions(self, request, context):
        purchased = list(request.purchased_books)

        available = [b for b in ALL_BOOKS if b not in purchased]

        if len(available) >= 3:
            suggested = random.sample(available, 3)
        else:
            suggested = available

        print(
            f"Suggestions for transaction={request.transaction_id}: {suggested}"
        )

        return suggestions.SuggestionsResponse(
            suggested_books=suggested
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    suggestions_grpc.add_SuggestionsServiceServicer_to_server(
        SuggestionsService(), server
    )

    server.add_insecure_port("[::]:50053")
    server.start()

    print("Suggestions server started on port 50053")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()