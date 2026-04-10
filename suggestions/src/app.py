import sys
import os
import logging
import time
import random
import threading
from datetime import datetime

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/suggestions')
)
sys.path.insert(0, suggestions_grpc_path)

import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
import grpc
from concurrent import futures
from groq import Groq

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("suggestions")

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

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

groq_client = None


def _initialize_groq_client():
    global groq_client

    if groq_client:
        return groq_client

    if not GROQ_API_KEY:
        logger.warning("GROQ_API_KEY not set, LLM disabled.")
        return None

    try:
        groq_client = Groq(api_key=GROQ_API_KEY)
        logger.info("Groq client initialized using model %s", GROQ_MODEL)
        return groq_client
    except Exception as e:
        logger.error("Failed to initialize Groq client: %s", e)
        return None


def _random_suggestions(purchased):
    available = [b for b in ALL_BOOKS if b not in purchased]

    if len(available) >= 3:
        return random.sample(available, 3)

    return available


def _generate_prompt():
    now = datetime.now()
    month = now.month
    month_name = now.strftime("%B")
    year = now.year

    if month in [12, 1, 2]:
        season = "winter"
        theme = "cozy reads for cold weather"
    elif month in [3, 4, 5]:
        season = "spring"
        theme = "fresh and uplifting stories"
    elif month in [6, 7, 8]:
        season = "summer"
        theme = "light beach reads"
    else:
        season = "fall"
        theme = "thought-provoking autumn reads"

    return f"""
It's {month_name} {year} during {season}. Suggest 3 {theme}.

Return ONLY:

BOOK TITLE - AUTHOR - YEAR

Example:
1. The Great Gatsby - F. Scott Fitzgerald - 1925
2. 1984 - George Orwell - 1949
3. To Kill a Mockingbird - Harper Lee - 1960
""".strip()


def _call_groq(prompt):
    client = _initialize_groq_client()
    if not client:
        return None

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a book recommendation assistant."},
                {"role": "user", "content": prompt},
            ],
            model=GROQ_MODEL,
            temperature=0.7,
            max_tokens=200,
        )

        return chat_completion.choices[0].message.content.strip()

    except Exception as e:
        logger.error("Groq call failed: %s", e)
        return None


def _parse_books(text):
    if not text:
        return []

    books = []

    for line in text.split("\n"):
        line = line.strip()

        for prefix in ["1.", "2.", "3.", "-", "*", "•"]:
            if line.startswith(prefix):
                line = line[len(prefix):].strip()

        if "-" in line and len(line) > 10:
            books.append(line)

        if len(books) == 3:
            break

    return books


class SuggestionsService(suggestions_grpc.SuggestionsServiceServicer):

    def __init__(self):
        # Vector clock layout: [transaction_verification, fraud_detection, suggestions]
        self.svc_idx = int(os.getenv("SUGGESTIONS_SERVICE_INDEX", "2"))
        self.total_svcs = int(os.getenv("VECTOR_CLOCK_SIZE", "3"))

        # order_id -> state
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
                    "vc": [0] * self.total_svcs,
                }

                entry["vc"][self.svc_idx] += 1
                self.orders[request.order_id] = entry

                vc_snapshot = entry["vc"][:]

            return suggestions.InitOrderResponse(
                ok=True,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=init_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during suggestions order initialization")

            return suggestions.InitOrderResponse(
                ok=False,
                vc=[0] * self.total_svcs,
            )

    def GetSuggestions(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self._get_order_or_not_found(request.order_id, context)

                if entry is None:
                    return suggestions.SuggestionsResponse(
                        suggested_books=[],
                        vc=[0] * self.total_svcs,
                    )

                # VC fix: work on a COPY so parallel events stay concurrent
                local_vc = entry["vc"][:]
                self._merge_and_increment(local_vc, request.vc)
                vc_snapshot = local_vc

            purchased = list(request.purchased_books)
            mode = request.mode or "author"

            if mode == "ai":
                prompt = _generate_prompt()
                llm_response = _call_groq(prompt)
                suggestions_list = _parse_books(llm_response)
            else:
                suggestions_list = []

            if not suggestions_list:
                logger.info("event=fallback_random_suggestions cid=%s", order_id)
                suggestions_list = _random_suggestions(purchased)

            return suggestions.SuggestionsResponse(
                suggested_books=suggestions_list,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=get_suggestions_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during suggestions generation")

            return suggestions.SuggestionsResponse(
                suggested_books=[],
                vc=[0] * self.total_svcs,
            )

    def FinalizeOrder(self, request, context):
        order_id = request.order_id or "unknown"

        try:
            with self.lock:
                entry = self.orders.get(request.order_id)

                if entry is not None:
                    self._merge_and_increment(entry["vc"], request.vc)
                    vc_snapshot = entry["vc"][:]
                    del self.orders[request.order_id]
                else:
                    vc_snapshot = self._normalize_vc(request.vc)

            return suggestions.FinalizeOrderResponse(
                ok=True,
                vc=vc_snapshot,
            )

        except Exception:
            logger.exception("event=finalize_order_exception cid=%s", order_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Internal error during suggestions order finalization")

            return suggestions.FinalizeOrderResponse(
                ok=False,
                vc=[0] * self.total_svcs,
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    _initialize_groq_client()

    suggestions_grpc.add_SuggestionsServiceServicer_to_server(
        SuggestionsService(),
        server,
    )

    port = "50053"
    server.add_insecure_port("[::]:" + port)

    server.start()
    logger.info("Server started. Listening on port %s.", port)

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
