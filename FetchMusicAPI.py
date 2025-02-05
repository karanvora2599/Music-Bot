import os
import uuid
import re
import sys
import hashlib
import time
import logging
import asyncio
import aiofiles
import threading
from queue import Queue, Empty
from logging.handlers import TimedRotatingFileHandler
from mutagen import File
from mutagen.mp3 import HeaderNotFoundError
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- FastAPI Imports ---
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import uvicorn

# --------------------------------------------------
# Logging Setup
# --------------------------------------------------
LOG_DIR = os.path.abspath("logs")
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, "FetchMusic.log")

file_handler = TimedRotatingFileHandler(
    filename=log_file_path,
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8"
)
console_handler = logging.StreamHandler()

detailed_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
)
file_handler.setFormatter(detailed_formatter)
console_handler.setFormatter(detailed_formatter)

logger = logging.getLogger("FetchMusic")
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# --------------------------------------------------
# Constants and Global Variables
# --------------------------------------------------
MEMORY_CACHE_LIMIT = 8 * 1024 * 1024 * 1024  # 8 GB cache limit
MAX_CHECKSUM_RETRIES = 3                     # Maximum number of write retries
FLUSH_INTERVAL = 5                           # Fixed flush interval in seconds

# Create a thread-safe queue for metadata items
metadata_queue = Queue()

# Event to signal the flush thread to stop
stop_flush_event = threading.Event()

# Global dictionary to hold job statuses
jobs = {}  # job_id -> { "status": "pending"/"running"/"completed"/"failed", "elapsed_time": float or None, "error": str or None }

# --------------------------------------------------
# --- Your Existing Pipeline Functions ---
# --------------------------------------------------

def fetch_music_metadata(file_path):
    """
    Fetch metadata from a music file and return it as a dictionary.
    Extensive error handling is provided for file reading and tag processing.
    """
    file_uuid = str(uuid.uuid4())
    try:
        audio_file = File(file_path)
    except (HeaderNotFoundError, Exception) as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return {file_uuid: {"error": "File could not be read or is unsupported", "file_path": file_path}}

    metadata = {}
    if audio_file and audio_file.tags:
        exclude_patterns = [
            r'^APIC:',
            r'^----:',
            r'^cover$',
            r'^TXXX:',
            r'^com\.apple\.iTunes:',
            r'^covr',
            r'^PRIV:',
            r'^GEOB:'
        ]
        try:
            for key, value in audio_file.tags.items():
                try:
                    if not any(re.match(pattern, key) for pattern in exclude_patterns):
                        metadata[key] = str(value) if not isinstance(value, list) else ', '.join(map(str, value))
                except Exception as inner_err:
                    logger.warning(f"Error processing tag {key} in {file_path}: {inner_err}")
        except Exception as tag_err:
            logger.error(f"Error iterating over tags in file {file_path}: {tag_err}")

        try:
            metadata["duration_seconds"] = round(audio_file.info.length, 2) if hasattr(audio_file.info, "length") else None
            metadata["bitrate"] = audio_file.info.bitrate if hasattr(audio_file.info, "bitrate") else None
        except Exception as info_err:
            logger.error(f"Error reading audio info from {file_path}: {info_err}")
        metadata["file_path"] = file_path
    else:
        metadata["error"] = "No metadata found or unsupported file format"

    return {file_uuid: metadata}

def calculate_checksum(data):
    """
    Calculate the SHA-256 checksum of the JSON-serialized data.
    Sorting keys ensures consistency.
    """
    sha256 = hashlib.sha256()
    try:
        sha256.update(json.dumps(data, sort_keys=True).encode('utf-8'))
    except Exception as e:
        logger.error(f"Error calculating checksum: {e}")
    return sha256.hexdigest()

async def async_write_metadata_to_json_with_checksum(json_file, cache, attempt=1):
    """
    Asynchronously write the cached metadata to the JSON file with checksum verification.
    Uses a temporary file and retries with exponential backoff if needed.
    """
    try:
        existing_data = {}
        try:
            async with aiofiles.open(json_file, 'r') as f:
                content = await f.read()
                existing_data = json.loads(content)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.info(f"Existing JSON file not found or corrupt: {e}")

        existing_data.update(cache)
        data_to_write = json.dumps(existing_data, indent=4, sort_keys=True)
        checksum_before = calculate_checksum(existing_data)

        temp_file = json_file + ".tmp"
        async with aiofiles.open(temp_file, 'w') as f:
            await f.write(data_to_write)

        async with aiofiles.open(temp_file, 'r') as f:
            try:
                content_read = await f.read()
                data_read_back = json.loads(content_read)
            except Exception as e:
                logger.error(f"Error reading back temporary file {temp_file}: {e}")
                data_read_back = {}

        checksum_after = calculate_checksum(data_read_back)
        if checksum_before != checksum_after:
            logger.error(f"Checksum mismatch: before={checksum_before}, after={checksum_after}")
            if attempt < MAX_CHECKSUM_RETRIES:
                logger.info(f"Retrying write operation (attempt {attempt+1}) after backoff...")
                await asyncio.sleep(2 ** attempt)
                return await async_write_metadata_to_json_with_checksum(json_file, cache, attempt=attempt+1)
            else:
                logger.error("Maximum checksum retries reached. Write operation failed.")
                return False
        else:
            os.replace(temp_file, json_file)
            logger.info("Checksum verified successfully and data written to disk.")
            return True

    except Exception as write_err:
        logger.error(f"Error writing metadata to JSON file asynchronously: {write_err}")
        return False

def process_files_worker(file_paths):
    """
    Process each file in the provided list using fetch_music_metadata.
    Returns a list of metadata dictionaries.
    """
    results = []
    for file_path in file_paths:
        try:
            result = fetch_music_metadata(file_path)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    return results

def flush_cache_worker(json_file, cache_size_limit, flush_interval=FLUSH_INTERVAL):
    """
    Flush thread: every flush_interval seconds, drain the metadata_queue
    and write the batch to disk asynchronously.
    """
    batch = {}
    while not (stop_flush_event.is_set() and metadata_queue.empty()):
        time.sleep(flush_interval)
        while True:
            try:
                metadata_item = metadata_queue.get_nowait()
                batch.update(metadata_item)
            except Empty:
                break
        if batch:
            logger.info(f"Flushing batch to JSON file (batch size: {sys.getsizeof(batch)} bytes)...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(async_write_metadata_to_json_with_checksum(json_file, batch))
            loop.close()
            if success:
                batch.clear()
            else:
                logger.warning("Failed to flush batch. Retaining data for retry.")
    if batch:
        logger.info("Final flush of remaining batch to JSON file...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_write_metadata_to_json_with_checksum(json_file, batch))
        loop.close()

def get_file_paths(input_path):
    """
    Return a list of supported music file paths given an input path (file or directory).
    """
    supported_extensions = (".mp3", ".flac", ".wav", ".m4a", ".ogg")
    file_paths = []
    if os.path.isdir(input_path):
        for root, dirs, files in os.walk(input_path):
            for file_name in files:
                if file_name.lower().endswith(supported_extensions):
                    file_paths.append(os.path.join(root, file_name))
    elif os.path.isfile(input_path):
        if input_path.lower().endswith(supported_extensions):
            file_paths.append(input_path)
        else:
            logger.warning(f"File {input_path} is not a supported music format.")
    else:
        logger.error(f"Input path {input_path} does not exist.")
    return file_paths

def explore_and_fetch_metadata(input_path, json_file, max_workers=8, cache_size_limit=MEMORY_CACHE_LIMIT):
    """
    Main pipeline function.
    1. Collect file paths.
    2. Start the flush thread.
    3. Use ProcessPoolExecutor to process files in parallel.
    4. Enqueue results.
    5. Signal the flush thread to finish.
    """
    file_paths = get_file_paths(input_path)
    logger.info(f"Total music files found: {len(file_paths)}")

    flush_thread = threading.Thread(
        target=flush_cache_worker,
        args=(json_file, cache_size_limit),
        daemon=True
    )
    flush_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            chunked_paths = [file_paths[i::max_workers] for i in range(max_workers)]
            futures = [executor.submit(process_files_worker, chunk) for chunk in chunked_paths]
            for future in as_completed(futures):
                try:
                    results = future.result()
                    logger.info(f"Worker returned {len(results)} items.")
                    for res in results:
                        metadata_queue.put(res)
                except Exception as e:
                    logger.error(f"Error in worker process: {e}")
    except Exception as e:
        logger.error(f"Error using ProcessPoolExecutor: {e}")

    logger.info(f"Total items in metadata_queue before stopping: {metadata_queue.qsize()}")
    stop_flush_event.set()
    flush_thread.join()

# --------------------------------------------------
# --- FastAPI Application and API Endpoints ---
# --------------------------------------------------

app = FastAPI(
    title="Music Metadata Extraction API",
    description="An API that extracts metadata from music files using a high-performance pipeline.",
    version="1.0"
)

# Request model for extraction job
class ExtractionJobRequest(BaseModel):
    input_path: str  # Directory or file path
    json_file: str   # Output JSON file path
    max_workers: int = 8

# Response model for job submission
class ExtractionJobResponse(BaseModel):
    job_id: str
    status: str

# Response model for job status
class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    elapsed_time: float = None
    error: str = None

# In-memory job store (for demo purposes)
jobs_store = {}  # job_id -> {"status": str, "elapsed_time": float or None, "error": str or None}

def run_extraction_job(job_id: str, input_path: str, json_file: str, max_workers: int):
    """
    Wrapper function that runs the extraction pipeline and updates the job store.
    This function runs in a background thread.
    """
    jobs_store[job_id]["status"] = "running"
    start_time = time.time()
    try:
        explore_and_fetch_metadata(input_path, json_file, max_workers=max_workers)
        elapsed = time.time() - start_time
        jobs_store[job_id]["status"] = "completed"
        jobs_store[job_id]["elapsed_time"] = elapsed
        logger.info(f"Job {job_id} completed in {elapsed} seconds.")
    except Exception as e:
        jobs_store[job_id]["status"] = "failed"
        jobs_store[job_id]["error"] = str(e)
        logger.error(f"Job {job_id} failed: {e}")

@app.post("/extract", response_model=ExtractionJobResponse)
async def extract_metadata(job_request: ExtractionJobRequest, background_tasks: BackgroundTasks):
    """
    Start an extraction job given an input path and an output JSON file.
    The job runs in the background and returns a job ID immediately.
    """
    job_id = str(uuid.uuid4())
    # Initialize job status
    jobs_store[job_id] = {"status": "pending", "elapsed_time": None, "error": None}
    # Schedule the extraction job in a background thread
    background_tasks.add_task(run_extraction_job, job_id, job_request.input_path, job_request.json_file, job_request.max_workers)
    return ExtractionJobResponse(job_id=job_id, status="pending")

@app.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Return the status of an extraction job given its job_id.
    """
    if job_id in jobs_store:
        job = jobs_store[job_id]
        return JobStatusResponse(job_id=job_id, status=job["status"], elapsed_time=job["elapsed_time"], error=job["error"])
    else:
        return JobStatusResponse(job_id=job_id, status="not found", elapsed_time=None, error="Job ID not found.")

# --------------------------------------------------
# Main Execution: Run FastAPI with Uvicorn
# --------------------------------------------------
if __name__ == '__main__':
    # For local testing, you can run this script directly.
    uvicorn.run(app, host="0.0.0.0", port=8000)