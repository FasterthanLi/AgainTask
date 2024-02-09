import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
import os
import threading
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(filename='tile_downloader.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Configuration
url_template = "https://tile.openstreetmap.org/{zoom}/{x}/{y}.png"
zoom_levels = (0, 3)  # Adjust as needed
rate_limit = 100  # Adjust according to your needs
mbtiles_file = r"C:\Users\1\Desktop\SQL\tiles.mbtiles"  # MBTiles file path
user_agent = 'OpenStreetMapTileDownloader/1.0'  # Replace with your app's user agent
max_threads = 3  # Adjust the number of threads

# Set up a requests session with retries
session = requests.Session()
session.headers.update({'User-Agent': user_agent})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))

# Initialize MBTiles database
def initialize_mbtiles(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS tiles (
                       zoom_level INTEGER,
                       tile_column INTEGER,
                       tile_row INTEGER,
                       tile_data BLOB,
                       PRIMARY KEY (zoom_level, tile_column, tile_row))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS metadata (
                       name TEXT,
                       value TEXT,
                       UNIQUE(name))''')
        metadata = [
            ('name', 'OpenStreetMapTiles'),
            ('type', 'overlay'),
            ('version', '1.1'),
            ('description', 'OpenStreetMap tile downloader'),
            ('format', 'png')
        ]
        for name, value in metadata:
            cur.execute("INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)", (name, value))
        conn.commit()
        conn.close()
        logging.info("Initialized the MBTiles database successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize the MBTiles database: {e}")

# Check if a tile exists in the MBTiles database
def tile_exists(db_path, zoom, x, y):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                    (zoom, x, 2**zoom - 1 - y))
        exists = cur.fetchone()[0] > 0
        conn.close()
        return exists
    except Exception as e:
        logging.error(f"Failed to check tile existence: zoom {zoom}, x {x}, y {y}, error: {e}")
        return False  # Assume tile does not exist in case of error

# Save a tile to the MBTiles database
def save_tile_to_mbtiles(db_path, zoom, x, y, tile_data):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                    (zoom, x, 2**zoom - 1 - y, sqlite3.Binary(tile_data)))  # Note the flipping of the y-axis
        conn.commit()
        conn.close()
        logging.info(f"Tile saved: zoom {zoom}, x {x}, y {y}")
    except Exception as e:
        logging.error(f"Failed to save tile: zoom {zoom}, x {x}, y {y}, error: {e}")

# Modify the download_tile function to include thread ID in logging
def download_tile(zoom, x, y, session):
    thread_id = threading.get_ident()  # Get the current thread's identifier
    tile_url = url_template.format(zoom=zoom, x=x, y=y)
    logging.info(f"Thread {thread_id}: Starting download {tile_url}")
    response = session.get(tile_url, stream=True)
    if response.status_code == 200:
        save_tile_to_mbtiles(mbtiles_file, zoom, x, y, response.content)
        logging.info(f"Thread {thread_id}: Downloaded {tile_url}")
        return f"Downloaded {tile_url}"
    else:
        logging.warning(f"Thread {thread_id}: Tile download failed: {tile_url}, status code: {response.status_code}")
        return f"Failed {tile_url}"

# Semaphore to enforce rate limit
semaphore = threading.Semaphore(rate_limit * max_threads)

# Function to download a single tile with rate limiting
def download_tile_rate_limited(zoom, x, y, session):
    if not tile_exists(mbtiles_file, zoom, x, y):  # Check if tile already exists
        with semaphore:  # Acquire a semaphore slot
            return download_tile(zoom, x, y, session)
    else:
        logging.info(f"Tile already exists: zoom {zoom}, x {x}, y {y}, skipping download.")
        return f"Skipped {zoom}/{x}/{y}"

def download_tiles_to_mbtiles(url_template, zoom_levels, rate_limit, mbtiles_file, max_threads):
    # Initialize the MBTiles file
    initialize_mbtiles(mbtiles_file)

    # Calculate the number of total tiles
    total_tiles = sum([4 ** z for z in range(zoom_levels[0], zoom_levels[1] + 1)])
    
    # Create a progress bar
    pbar = tqdm(total=total_tiles, desc='Downloading tiles')
    
    # Initialize a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Create a list to store futures
        futures = []

        # Submit tasks to the executor
        for zoom in range(zoom_levels[0], zoom_levels[1] + 1):
            for x in range(2 ** zoom):
                for y in range(2 ** zoom):
                    # Submit the download_tile function to the executor
                    future = executor.submit(download_tile_rate_limited, zoom, x, y, session)
                    futures.append(future)
        
        # Process the completed futures
        for future in as_completed(futures):
            # Update the progress bar when a future is completed
            pbar.update(1)
            try:
                # Get the result of the future
                result = future.result()
            except Exception as e:
                logging.error(f"Exception occurred: {e}")

    pbar.close()

# Execute the download function
download_tiles_to_mbtiles(url_template, zoom_levels, rate_limit, mbtiles_file, max_threads)





