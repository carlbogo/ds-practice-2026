import sys
import os
import logging
import time
import random
from datetime import datetime
from concurrent import futures

import grpc
from groq import Groq

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/suggestions')
)
sys.path.insert(0, suggestions_grpc_path)

import suggestions_pb2
import suggestions_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="===LOG=== %(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("suggestions")


# -------------------------------------------------------------------
# Static fallback book list
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Groq Configuration
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Random fallback suggestions
# -------------------------------------------------------------------

def _random_suggestions(purchased):
    available = [b for b in ALL_BOOKS if b not in purchased]

    if len(available) >= 3:
        return random.sample(available, 3)

    return available


# -------------------------------------------------------------------
# Prompt generation
# -------------------------------------------------------------------

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
"""


# -------------------------------------------------------------------
# LLM call
# -------------------------------------------------------------------

def _call_groq(prompt):

    client = _initialize_groq_client()
    if not client:
        return None

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system",
                 "content": "You are a book recommendation assistant."},
                {"role": "user", "content": prompt}
            ],
            model=GROQ_MODEL,
            temperature=0.7,
            max_tokens=200,
        )

        return chat_completion.choices[0].message.content.strip()

    except Exception as e:
        logger.error("Groq call failed: %s", e)
        return None


# -------------------------------------------------------------------
# Parse LLM response
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# gRPC Service
# -------------------------------------------------------------------

class SuggestionsService(suggestions_pb2_grpc.SuggestionsServiceServicer):

    def GetSuggestions(self, request, context):

        started = time.perf_counter()

        metadata = dict(context.invocation_metadata())
        cid = metadata.get(
            "x-correlation-id",
            request.transaction_id or "unknown"
        )

        purchased = list(request.purchased_books)

        logger.info(
            "cid=%s event=request_received purchased=%s",
            cid,
            len(purchased),
        )

        try:

            mode = request.mode or "author"

            if mode == "ai":
                prompt = _generate_prompt()
                llm_response = _call_groq(prompt)
                suggestions_list = _parse_books(llm_response)
            else:
                suggestions_list = []

            if not suggestions_list:
                logger.info(
                    "cid=%s event=fallback_random_suggestions",
                    cid,
                )
                suggestions_list = _random_suggestions(purchased)

            latency_ms = (time.perf_counter() - started) * 1000

            logger.info(
                "cid=%s event=suggestions_generated latency_ms=%.2f suggestions=%s",
                cid,
                latency_ms,
                suggestions_list,
            )

            return suggestions_pb2.SuggestionsResponse(
                suggested_books=suggestions_list
            )

        except Exception:

            logger.exception("cid=%s unexpected_error", cid)

            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Suggestion generation failed")

            return suggestions_pb2.SuggestionsResponse(
                suggested_books=[]
            )


# -------------------------------------------------------------------
# Server
# -------------------------------------------------------------------

def serve():

    logger.info("Starting suggestions service")

    _initialize_groq_client()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    suggestions_pb2_grpc.add_SuggestionsServiceServicer_to_server(
        SuggestionsService(),
        server
    )

    port = "50053"

    server.add_insecure_port(f"[::]:{port}")
    server.start()

    logger.info("Server running on port %s", port)

    server.wait_for_termination()


if __name__ == "__main__":
    serve()