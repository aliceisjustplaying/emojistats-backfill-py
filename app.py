import os
import json
import asyncio
import logging
import multiprocessing
from functools import partial
from typing import Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor
import aiohttp
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)

from atmst.cartool import print_all_records


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler("car_service.log"), logging.StreamHandler()],
)

app = FastAPI(title="CAR File Fetcher and Parser")

CAR_FILES_DIR = "car_files"
os.makedirs(CAR_FILES_DIR, exist_ok=True)

MAX_RETRIES = 10
INITIAL_BACKOFF = 1
BACKOFF_FACTOR = 2

executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())


class FetchRequest(BaseModel):
    did: str
    pds: str


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


async def parse_car(car_file_path: str) -> str:
    records = print_all_records(car_file_path, True)
    for k, v in records.items():
        yield json.dumps({k: v}) + "\n"


@retry(
    reraise=True,
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_random_exponential(
        multiplier=INITIAL_BACKOFF, min=INITIAL_BACKOFF, max=60
    ),
    retry=(
        retry_if_exception_type(RetryableError)
        | retry_if_exception_type(asyncio.TimeoutError)
    ),
)
async def fetch_car_with_retry(
    session: aiohttp.ClientSession, url: str, headers: Dict[str, str], did: str
) -> bytes:
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                car_bytes = await response.read()
                if not car_bytes:
                    logging.error("Received empty CAR file.")
                    raise NonRetryableError("Received empty CAR file.")
                return car_bytes
            elif response.status in {429, 503, 504}:
                logging.warning(
                    f"Received HTTP {response.status} for DID {did}. Retrying..."
                )
                raise RetryableError(f"HTTP {response.status} error.")
            else:
                logging.error(f"Failed to fetch CAR file: HTTP {response.status}")
                raise NonRetryableError(f"HTTP {response.status} error.")
    except aiohttp.ClientConnectorError as e:
        logging.error(f"DNS resolution error for DID {did}: {e}")
        raise NonRetryableError("DNS resolution error.")
    except aiohttp.ClientResponseError as e:
        logging.error(f"Client response error for DID {did}: {e}")
        raise RetryableError(str(e))
    except asyncio.TimeoutError:
        logging.warning(f"Timeout while fetching DID {did}. Retrying...")
        raise RetryableError("Timeout error.")
    except aiohttp.ClientError as e:
        logging.error(f"Client error for DID {did}: {e}")
        raise RetryableError(str(e))


@app.post("/fetch")
async def fetch_car_file(request: FetchRequest) -> str:
    did, pds = request.did, request.pds
    url = f"https://{pds}/xrpc/com.atproto.sync.getRepo?did={did}"
    headers = {
        "Accept": "application/vnd.ipld.car",
        "User-Agent": "emojistats-backfiller/0.0.1",
    }

    logging.info(f"Fetching CAR file for DID: {did} from PDS: {pds}")

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            car_bytes = await fetch_car_with_retry(session, url, headers, did)
    except RetryableError as e:
        logging.error(f"Retryable error fetching CAR file for DID {did}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch CAR file after {MAX_RETRIES} attempts.",
        )
    except NonRetryableError as e:
        logging.error(f"Non-retryable error fetching CAR file for DID {did}: {e}")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logging.error(f"Unexpected error while fetching CAR file for DID {did}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

    # Save CAR file
    try:
        filename = f"{did.replace(':', '_')}.car"
        car_file_path = os.path.join(CAR_FILES_DIR, filename)

        if os.path.exists(car_file_path):
            os.remove(car_file_path)
            logging.info(f"Deleted existing CAR file: {car_file_path}")

        with open(car_file_path, "wb") as f:
            f.write(car_bytes)
        logging.info(f"Saved CAR file to {car_file_path}")
    except Exception as e:
        logging.error(f"Error saving CAR file: {e}")
        raise HTTPException(status_code=500, detail="Error saving CAR file.")

    # Parse CAR file
    try:
        loop = asyncio.get_running_loop()
        parsed_data = await loop.run_in_executor(
            executor, partial(parse_car, car_file_path)
        )
        logging.info(f"Parsed CAR file for DID: {did}")
    except Exception as e:
        logging.error(f"Error parsing CAR file: {e}")
        raise HTTPException(status_code=500, detail="Error parsing CAR file.")

    return StreamingResponse(parsed_data)
