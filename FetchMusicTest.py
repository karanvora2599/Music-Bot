import os
import uuid
import json
import re
from mutagen import File

def fetch_music_metadata(file_path):
    """Fetch metadata from a music file and return it as a dictionary."""
    # Generate a unique identifier for the file
    file_uuid = str(uuid.uuid4())

    # Load the audio file and read metadata
    audio_file = File(file_path)
    metadata = {}

    if audio_file and audio_file.tags:
        # Define patterns to exclude
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

        for key, value in audio_file.tags.items():
            # Check if the key matches any exclusion pattern
            if not any(re.match(pattern, key) for pattern in exclude_patterns):
                metadata[key] = str(value) if not isinstance(value, list) else ', '.join(map(str, value))

        # Add audio properties
        metadata["duration_seconds"] = round(audio_file.info.length, 2) if hasattr(audio_file.info, "length") else None
        metadata["bitrate"] = audio_file.info.bitrate if hasattr(audio_file.info, "bitrate") else None
        metadata["file_path"] = file_path
    else:
        metadata["error"] = "No metadata found or unsupported file format"

    # Return the metadata dictionary with UUID
    return {file_uuid: metadata}

def explore_and_fetch_metadata(parent_folder, json_file):
    """Recursively explore a folder, fetch metadata from music files, and append to a JSON file."""
    # Open the JSON file and load existing data if available
    try:
        with open(json_file, 'r') as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = {}

    # Recursively walk through directories and process each file
    for root, dirs, files in os.walk(parent_folder):
        for file_name in files:
            # Only process music files (e.g., MP3, FLAC, WAV, etc.)
            if file_name.lower().endswith((".mp3", ".flac", ".wav", ".m4a", ".ogg")):
                file_path = os.path.join(root, file_name)
                metadata = fetch_music_metadata(file_path)

                # Append metadata to the existing data
                existing_data.update(metadata)

                # Write updated data to the JSON file
                with open(json_file, 'w') as f:
                    json.dump(existing_data, f, indent=4)

                print(f"Processed: {file_path}")

# Example usage
parent_folder = r"D:\Music"  # Replace with your folder path
json_file = "music_metadata.json"              # Output JSON file name

explore_and_fetch_metadata(parent_folder, json_file)