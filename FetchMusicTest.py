import os
import uuid
import json
import re
import sys
import hashlib
import time
import logging
import asyncio
import aiofiles
from logging.handlers import TimedRotatingFileHandler
from mutagen import File
from mutagen.mp3 import HeaderNotFoundError
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading

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

# --------------------------------------------------
# Metadata Fetching and Checksum Functions
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
        logger.error(f"Error reading file {file_path}: {str(e)}")
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
    Sorting keys ensures that the checksum is consistent regardless of key order.
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
    Uses a temporary file and, in case of a checksum mismatch or transient error,
    retries with exponential backoff.
    """
    try:
        # Read existing data asynchronously
        existing_data = {}
        try:
            async with aiofiles.open(json_file, 'r') as f:
                content = await f.read()
                existing_data = json.loads(content)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.info(f"Existing JSON file not found or corrupt: {e}")

        # Merge cached data into existing data
        existing_data.update(cache)
        data_to_write = json.dumps(existing_data, indent=4, sort_keys=True)
        checksum_before = calculate_checksum(existing_data)

        # Write to a temporary file asynchronously
        temp_file = json_file + ".tmp"
        async with aiofiles.open(temp_file, 'w') as f:
            await f.write(data_to_write)

        # Read back the temporary file for checksum verification
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
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                return await async_write_metadata_to_json_with_checksum(json_file, cache, attempt=attempt+1)
            else:
                logger.error("Maximum checksum retries reached. Write operation failed.")
                return False
        else:
            # Replace the original file with the temporary file for a fast atomic write
            os.replace(temp_file, json_file)
            logger.info("Checksum verified successfully and data written to disk.")
            return True

    except Exception as write_err:
        logger.error(f"Error writing metadata to JSON file asynchronously: {write_err}")
        return False

# --------------------------------------------------
# Worker Function: Process Files and Enqueue Metadata
# --------------------------------------------------
def process_files_worker(file_paths):
    """
    Process each file in the provided list, fetch metadata, and place it into the thread-safe queue.
    """
    for file_path in file_paths:
        try:
            metadata = fetch_music_metadata(file_path)
            metadata_queue.put(metadata)
            logger.info(f"Processed: {file_path}")
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

# --------------------------------------------------
# Flush Thread: Monitor Queue and Write Batches to Disk
# --------------------------------------------------
def flush_cache_worker(json_file, cache_size_limit, flush_interval=FLUSH_INTERVAL):
    """
    Dedicated flushing thread that continuously monitors the metadata_queue.
    Every flush_interval seconds, it accumulates available items into a batch
    and flushes them to disk using asynchronous I/O.
    """
    batch = {}
    while not (stop_flush_event.is_set() and metadata_queue.empty()):
        # Wait for flush_interval seconds
        time.sleep(flush_interval)

        # Drain available items from the queue into the batch
        while True:
            try:
                metadata_item = metadata_queue.get_nowait()
                batch.update(metadata_item)
            except Empty:
                break

        # If there is data to flush, write it out asynchronously.
        if batch:
            logger.info(f"Flushing batch to JSON file (batch size: {sys.getsizeof(batch)} bytes)...")
            # Run the asynchronous write in a temporary event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(async_write_metadata_to_json_with_checksum(json_file, batch))
            loop.close()
            if success:
                batch.clear()
            else:
                logger.warning("Failed to flush batch. Retaining data for retry.")

    # Final flush of any remaining data in the batch
    if batch:
        logger.info("Final flush of remaining batch to JSON file...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_write_metadata_to_json_with_checksum(json_file, batch))
        loop.close()

# --------------------------------------------------
# Utility: Get Supported File Paths from Input
# --------------------------------------------------
def get_file_paths(input_path):
    """
    Return a list of supported music file paths.
    If input_path is a file, return a list containing that file (if supported).
    If input_path is a directory, recursively search for supported files.
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

# --------------------------------------------------
# Main Function: Explore Input and Process Files
# --------------------------------------------------
def explore_and_fetch_metadata(input_path, json_file, max_workers=8, cache_size_limit=MEMORY_CACHE_LIMIT):
    """
    Given an input path (a folder or a single file), collect supported file paths and then use multithreading
    to fetch metadata and enqueue it. A dedicated flushing thread will periodically flush the metadata to disk.
    """
    file_paths = get_file_paths(input_path)
    logger.info(f"Total music files found: {len(file_paths)}")

    # Start the dedicated flush thread
    flush_thread = threading.Thread(
        target=flush_cache_worker,
        args=(json_file, cache_size_limit),
        daemon=True
    )
    flush_thread.start()

    # Use ThreadPoolExecutor for processing files concurrently.
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Split file paths evenly among workers.
            futures = [
                executor.submit(process_files_worker, file_paths[i::max_workers])
                for i in range(max_workers)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in worker thread execution: {e}")
    except Exception as e:
        logger.error(f"Error using ThreadPoolExecutor: {e}")

    # Signal the flush thread to stop after all worker threads are done.
    stop_flush_event.set()
    flush_thread.join()

# --------------------------------------------------
# Main Execution
# --------------------------------------------------
if __name__ == '__main__':
    # Replace with your actual folder or file path.
    start = time.time()
    input_path = r"D:\Music"
    json_file = "music_metadata_with_checksum2.json"
    explore_and_fetch_metadata(input_path, json_file, max_workers=16)
    end = time.time()
    print(f"Total time taken: {end - start} seconds")