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
import json
from pydantic import BaseModel

# --- BlackSheep Imports ---
from blacksheep import Application, Request, Response
from blacksheep.server.responses import json as bs_json_response

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

# Global in-memory job store: job_id -> { "status": "pending"/"running"/"completed"/"failed", "elapsed_time": float or None, "error": str or None }
jobs_store = {}

# --------------------------------------------------
# Pydantic Models for Request and Response
# --------------------------------------------------
class ExtractionJobRequest(BaseModel):
    input_path: str  # Directory or file path
    json_file: str   # Output JSON file path
    max_workers: int = 8

class ExtractionJobResponse(BaseModel):
    job_id: str
    status: str

class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    elapsed_time: float = None
    error: str = None

# --------------------------------------------------
# Pipeline Functions with Extensive Logging
# --------------------------------------------------

def fetch_music_metadata(file_path):
    """
    Fetch metadata from a music file and return it as a dictionary.
    Extensive error handling and logging are provided.
    """
    logger.debug(f"Starting metadata extraction for file: {file_path}")
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
                        logger.debug(f"Tag processed for file {file_path}: {key} -> {metadata[key]}")
                except Exception as inner_err:
                    logger.warning(f"Error processing tag {key} in {file_path}: {inner_err}")
        except Exception as tag_err:
            logger.error(f"Error iterating over tags in file {file_path}: {tag_err}")

        try:
            metadata["duration_seconds"] = round(audio_file.info.length, 2) if hasattr(audio_file.info, "length") else None
            metadata["bitrate"] = audio_file.info.bitrate if hasattr(audio_file.info, "bitrate") else None
            logger.debug(f"Audio info for {file_path}: duration={metadata.get('duration_seconds')}, bitrate={metadata.get('bitrate')}")
        except Exception as info_err:
            logger.error(f"Error reading audio info from {file_path}: {info_err}")
        metadata["file_path"] = file_path
    else:
        metadata["error"] = "No metadata found or unsupported file format"
        logger.warning(f"No metadata found or unsupported format for file: {file_path}")

    logger.debug(f"Completed metadata extraction for file: {file_path}")
    return {file_uuid: metadata}

def calculate_checksum(data):
    """
    Calculate the SHA-256 checksum of the JSON-serialized data.
    Sorting keys ensures consistency.
    """
    logger.debug("Calculating checksum for data.")
    sha256 = hashlib.sha256()
    try:
        serialized = json.dumps(data, sort_keys=True)
        sha256.update(serialized.encode('utf-8'))
        logger.debug(f"Checksum calculated: {sha256.hexdigest()}")
    except Exception as e:
        logger.error(f"Error calculating checksum: {e}")
    return sha256.hexdigest()

async def async_write_metadata_to_json_with_checksum(json_file, cache, attempt=1):
    """
    Asynchronously write the cached metadata to the JSON file with checksum verification.
    Uses a temporary file and retries with exponential backoff if needed.
    """
    logger.info(f"Attempting to write metadata to {json_file} (attempt {attempt}). Cache size: {len(cache)} items")
    try:
        existing_data = {}
        try:
            async with aiofiles.open(json_file, 'r') as f:
                content = await f.read()
                existing_data = json.loads(content)
                logger.debug(f"Existing data loaded from {json_file} (size: {len(existing_data)} items)")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.info(f"Existing JSON file not found or corrupt: {e}")

        existing_data.update(cache)
        data_to_write = json.dumps(existing_data, indent=4, sort_keys=True)
        checksum_before = calculate_checksum(existing_data)
        logger.debug(f"Checksum before write: {checksum_before}")

        temp_file = json_file + ".tmp"
        async with aiofiles.open(temp_file, 'w') as f:
            await f.write(data_to_write)
        logger.info(f"Temporary file created: {temp_file}")

        async with aiofiles.open(temp_file, 'r') as f:
            try:
                content_read = await f.read()
                data_read_back = json.loads(content_read)
                logger.debug(f"Read back data from temporary file: {temp_file}")
            except Exception as e:
                logger.error(f"Error reading back temporary file {temp_file}: {e}")
                data_read_back = {}

        checksum_after = calculate_checksum(data_read_back)
        logger.debug(f"Checksum after write: {checksum_after}")

        if checksum_before != checksum_after:
            logger.error(f"Checksum mismatch for {json_file}: before={checksum_before}, after={checksum_after}")
            if attempt < MAX_CHECKSUM_RETRIES:
                backoff_time = 2 ** attempt
                logger.info(f"Retrying write operation in {backoff_time} seconds (attempt {attempt+1})")
                await asyncio.sleep(backoff_time)
                return await async_write_metadata_to_json_with_checksum(json_file, cache, attempt=attempt+1)
            else:
                logger.error("Maximum checksum retries reached. Write operation failed.")
                return False
        else:
            os.replace(temp_file, json_file)
            logger.info(f"Checksum verified successfully. Data written to {json_file}")
            return True

    except Exception as write_err:
        logger.error(f"Error writing metadata to JSON file asynchronously: {write_err}")
        return False

def process_files_worker(file_paths):
    """
    Process each file in the provided list using fetch_music_metadata.
    Returns a list of metadata dictionaries.
    """
    logger.info(f"Worker started for processing {len(file_paths)} file(s).")
    results = []
    for file_path in file_paths:
        try:
            logger.debug(f"Worker processing file: {file_path}")
            result = fetch_music_metadata(file_path)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    logger.info(f"Worker completed processing {len(file_paths)} file(s).")
    return results

def flush_cache_worker(json_file, cache_size_limit, flush_interval=FLUSH_INTERVAL):
    """
    Flush thread: every flush_interval seconds, drain the metadata_queue
    and write the batch to disk asynchronously.
    """
    logger.info("Flush thread started.")
    batch = {}
    while not (stop_flush_event.is_set() and metadata_queue.empty()):
        logger.debug("Flush thread sleeping...")
        time.sleep(flush_interval)
        while True:
            try:
                metadata_item = metadata_queue.get_nowait()
                batch.update(metadata_item)
                logger.debug("Drained one metadata item from the queue.")
            except Empty:
                break
        if batch:
            logger.info(f"Flushing batch to JSON file {json_file}. Batch contains {len(batch)} item(s).")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(async_write_metadata_to_json_with_checksum(json_file, batch))
            loop.close()
            if success:
                logger.info("Batch flushed successfully. Clearing batch.")
                batch.clear()
            else:
                logger.warning("Failed to flush batch. Data will be retained for next attempt.")
        else:
            logger.debug("No items to flush at this interval.")
    if batch:
        logger.info("Performing final flush of remaining batch to JSON file...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_write_metadata_to_json_with_checksum(json_file, batch))
        loop.close()
    logger.info("Flush thread exiting.")

def get_file_paths(input_path):
    """
    Return a list of supported music file paths given an input path (file or directory).
    """
    logger.info(f"Searching for music files in: {input_path}")
    supported_extensions = (".mp3", ".flac", ".wav", ".m4a", ".ogg")
    file_paths = []
    if os.path.isdir(input_path):
        for root, dirs, files in os.walk(input_path):
            for file_name in files:
                if file_name.lower().endswith(supported_extensions):
                    full_path = os.path.join(root, file_name)
                    file_paths.append(full_path)
                    logger.debug(f"Found supported file: {full_path}")
    elif os.path.isfile(input_path):
        if input_path.lower().endswith(supported_extensions):
            file_paths.append(input_path)
            logger.debug(f"Single supported file provided: {input_path}")
        else:
            logger.warning(f"File {input_path} is not a supported music format.")
    else:
        logger.error(f"Input path {input_path} does not exist.")
    logger.info(f"Total supported music files found: {len(file_paths)}")
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
    logger.info(f"Starting metadata extraction pipeline for input path: {input_path}")
    file_paths = get_file_paths(input_path)
    logger.info(f"Total music files found: {len(file_paths)}")

    flush_thread = threading.Thread(
        target=flush_cache_worker,
        args=(json_file, cache_size_limit),
        daemon=True
    )
    flush_thread.start()
    logger.info("Flush thread started successfully.")

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            logger.info(f"Processing files using {max_workers} workers.")
            # Chunk the file paths evenly among the workers
            chunked_paths = [file_paths[i::max_workers] for i in range(max_workers)]
            futures = [executor.submit(process_files_worker, chunk) for chunk in chunked_paths]
            for future in as_completed(futures):
                try:
                    results = future.result()
                    logger.info(f"A worker completed and returned {len(results)} metadata item(s).")
                    for res in results:
                        metadata_queue.put(res)
                        logger.debug("Metadata item added to the queue.")
                except Exception as e:
                    logger.error(f"Error in worker process: {e}")
    except Exception as e:
        logger.error(f"Error using ProcessPoolExecutor: {e}")

    logger.info(f"Total items in metadata_queue before stopping: {metadata_queue.qsize()}")
    stop_flush_event.set()
    flush_thread.join()
    logger.info("Metadata extraction pipeline completed.")

def run_extraction_job(job_id: str, input_path: str, json_file: str, max_workers: int):
    """
    Wrapper function that runs the extraction pipeline and updates the job store.
    This function runs in a background thread.
    """
    logger.info(f"Job {job_id}: Starting extraction job.")
    jobs_store[job_id]["status"] = "running"
    start_time = time.time()
    try:
        explore_and_fetch_metadata(input_path, json_file, max_workers=max_workers)
        elapsed = time.time() - start_time
        jobs_store[job_id]["status"] = "completed"
        jobs_store[job_id]["elapsed_time"] = elapsed
        logger.info(f"Job {job_id} completed successfully in {elapsed:.2f} seconds.")
    except Exception as e:
        jobs_store[job_id]["status"] = "failed"
        jobs_store[job_id]["error"] = str(e)
        logger.error(f"Job {job_id} failed: {e}")

# --------------------------------------------------
# BlackSheep Application and API Endpoints with Extensive Logging
# --------------------------------------------------

app = Application()

@app.route("POST", "/extract")
async def extract_metadata_endpoint(request: Request) -> Response:
    """
    Start an extraction job given an input path and an output JSON file.
    The job runs in the background and returns a job ID immediately.
    """
    logger.info("Received request at /extract endpoint.")
    try:
        data = await request.json()
        logger.debug(f"Request JSON payload: {data}")
        job_request = ExtractionJobRequest(**data)
    except Exception as e:
        logger.error(f"Invalid request payload: {e}")
        return Response(400, content=f"Invalid request: {str(e)}")
    
    job_id = str(uuid.uuid4())
    jobs_store[job_id] = {"status": "pending", "elapsed_time": None, "error": None}
    logger.info(f"Job {job_id}: Registered with status 'pending'. Starting background extraction task.")

    # Start the extraction job in a background thread
    threading.Thread(
        target=run_extraction_job, 
        args=(job_id, job_request.input_path, job_request.json_file, job_request.max_workers),
        daemon=True
    ).start()
    
    response_data = {"job_id": job_id, "status": "pending"}
    logger.info(f"Job {job_id}: Extraction job started in background. Returning response.")
    return bs_json_response(response_data)

@app.route("GET", "/status/{job_id}")
async def get_job_status(request: Request) -> Response:
    """
    Return the status of an extraction job given its job_id.
    """
    job_id = request.path_params["job_id"]
    logger.info(f"Received status request for job_id: {job_id}")
    if job_id in jobs_store:
        job = jobs_store[job_id]
        response_data = {
            "job_id": job_id,
            "status": job["status"],
            "elapsed_time": job["elapsed_time"],
            "error": job["error"]
        }
        logger.debug(f"Job {job_id} status: {response_data}")
        return bs_json_response(response_data)
    else:
        logger.warning(f"Job {job_id} not found in job store.")
        response_data = {
            "job_id": job_id,
            "status": "not found",
            "elapsed_time": None,
            "error": "Job ID not found."
        }
        return bs_json_response(response_data, status_code=404)