import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
import threading
from tqdm import tqdm  # Progress bar library
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from shapely.geometry import shape, box  # Geometry handling library
import math

# Configure logging for tile downloading activities and skipped tiles.
logging.basicConfig(filename='tile_downloader.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Setup for logging skipped tiles to avoid clutter in the main log.
skipped_tiles_logger = logging.getLogger("skipped_tiles")
skipped_tiles_logger.setLevel(logging.INFO)
skipped_tiles_handler = logging.FileHandler('skipped_tiles.log')
skipped_tiles_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
skipped_tiles_logger.addHandler(skipped_tiles_handler)
skipped_tiles_logger.propagate = False  # Prevent logs from being propagated to parent loggers

# Configuration class for storing downloader settings.
class Config:
    def __init__(self, url_template, zoom_levels, rate_limit, mbtiles_file, user_agent, max_threads):
        # URL template for tile requests, includes placeholders for zoom/x/y.
        self.url_template = url_template
        # Tuple defining the minimum and maximum zoom levels to download.
        self.zoom_levels = zoom_levels
        # Limit on the number of requests per second.
        self.rate_limit = rate_limit
        # Path to the SQLite database file where tiles are stored.
        self.mbtiles_file = mbtiles_file
        # User-Agent header value for HTTP requests.
        self.user_agent = user_agent
        # Maximum number of concurrent download threads.
        self.max_threads = max_threads

# Manages HTTP sessions with retry logic for robustness.
class SessionManager:
    def __init__(self, user_agent):
        # Setup a requests session with custom User-Agent.
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
        # Configure retries for transient errors.
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def get(self, url):
        # Perform a GET request, streaming response for efficiency with large files.
        return self.session.get(url, stream=True)

# Handles interactions with the MBTiles SQLite database.
class MBTilesManager:
    def __init__(self, db_path):
        # Path to the SQLite database file.
        self.db_path = db_path
        # Thread-local storage for database connections.
        self.local_conn = threading.local()
        # Initialize the database schema.
        self.initialize_db()

    def get_conn(self):
        # Get or create a database connection for the current thread.
        if not hasattr(self.local_conn, "conn"):
            self.local_conn.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        return self.local_conn.conn

    def initialize_db(self):
        # Create tiles and metadata tables if they don't already exist.
        conn = self.get_conn()
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
        # Insert some default metadata values.
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

    def tile_exists(self, zoom, x, y):
        # Check if a tile already exists in the database to avoid re-downloading.
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (zoom, x, 2**zoom - 1 - y))
        exists = cur.fetchone()[0] > 0
        return exists

    def save_tile(self, zoom, x, y, tile_data):
        # Insert or replace a tile in the database.
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)", (zoom, x, 2**zoom - 1 - y, sqlite3.Binary(tile_data)))
        conn.commit()

    def close_conn(self):
        # Close the database connection for the current thread.
        if hasattr(self.local_conn, "conn"):
            self.local_conn.conn.close()
            del self.local_conn.conn

# Orchestrates tile downloading, including concurrency and rate limiting.
class TileDownloader:
    def __init__(self, config, session_manager, mbtiles_manager):
        self.config = config  # Configuration settings.
        self.session_manager = session_manager  # HTTP session manager.
        self.mbtiles_manager = mbtiles_manager  # SQLite database manager.
        self.semaphore = threading.Semaphore(config.rate_limit * config.max_threads)  # Semaphore for rate limiting.
        self.pbar_lock = threading.Lock()  # Lock for synchronizing progress bar updates.

    def download_tile(self, zoom, x, y, pbar, pbar_lock):
        # Download a single tile and save it to the database.
        thread_id = threading.get_ident()  # Get the current thread identifier for logging.
        tile_url = self.config.url_template.format(zoom=zoom, x=x, y=y)  # Construct the tile URL.
        logging.info(f"Thread {thread_id}: Starting download {tile_url}")
        try:
            response = self.session_manager.get(tile_url)
            if response.status_code == 200:
                self.mbtiles_manager.save_tile(zoom, x, y, response.content)
                logging.info(f"Thread {thread_id}: Downloaded {tile_url}")
            else:
                logging.warning(f"Thread {thread_id}: Tile download failed {tile_url}, status code: {response.status_code}")
        finally:
            with self.pbar_lock:  # Synchronize updates to the progress bar.
                pbar.update(1)
            self.mbtiles_manager.close_conn()  # Close the database connection for the current thread.

    def download_tiles(self, tiles, pbar):
        # Download multiple tiles using a thread pool.
        with ThreadPoolExecutor(max_workers=self.config.max_threads) as executor:
            futures = [executor.submit(self.download_tile_rate_limited, zoom, x, y, pbar, self.pbar_lock) for zoom, x, y in tiles]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Exception occurred: {e}")
                    with self.pbar_lock:  # Synchronize updates to the progress bar in case of an error.
                        pbar.update(1)

    def download_tile_rate_limited(self, zoom, x, y, pbar, pbar_lock):
        # Download a tile with rate limiting.
        with self.semaphore:  # Acquire a semaphore slot to enforce rate limiting.
            if not self.mbtiles_manager.tile_exists(zoom, x, y):
                self.download_tile(zoom, x, y, pbar, pbar_lock)
            else:
                skipped_tiles_logger.info(f"Skipped: Tile already exists: zoom {zoom}, x {x}, y {y}")  # Log skipped tiles.
                with pbar_lock:
                    pbar.update(1)  # Increment the progress bar for skipped tiles.

    def download_tiles_to_mbtiles(self, geojson_path=None):
        # Main method to download tiles and save them to an MBTiles file.
        priority_tiles = []
        if geojson_path:
            # Load GeoJSON and calculate priority tiles if a GeoJSON path is provided.
            geojson = self.load_geojson(geojson_path)
            for zoom in range(self.config.zoom_levels[0], self.config.zoom_levels[1] + 1):
                priority_tiles += self.tiles_in_priority_zones(zoom, geojson)

        # Calculate the total number of tiles to be downloaded.
        total_tiles = sum([4 ** z for z in range(self.config.zoom_levels[0], self.config.zoom_levels[1] + 1)])
        # Calculate non-priority tiles by excluding priority ones.
        non_priority_tiles = [(zoom, x, y) for zoom in range(self.config.zoom_levels[0], self.config.zoom_levels[1] + 1) for x in range(2 ** zoom) for y in range(2 ** zoom) if (zoom, x, y) not in priority_tiles]
        # Combine priority and non-priority tiles.
        all_tiles = priority_tiles + non_priority_tiles

        pbar = tqdm(total=total_tiles, desc='Downloading tiles')  # Initialize the progress bar.
        self.download_tiles(all_tiles, pbar)  # Download all tiles.
        pbar.close()  # Close the progress bar when done.

    def load_geojson(self, geojson_path):
        # Load a GeoJSON file from the specified path.
        with open(geojson_path, 'r') as f:
            return json.load(f)

    def tile_bounds(self, x, y, z):
        # Calculate the geographical bounds of a tile.
        n = 2.0 ** z
        lon_min = x / n * 360.0 - 180.0
        lat_min_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat_min = math.degrees(lat_min_rad)
        lon_max = (x + 1) / n * 360.0 - 180.0
        lat_max_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
        lat_max = math.degrees(lat_max_rad)
        return (lon_min, lat_min, lon_max, lat_max)

    def tiles_in_priority_zones(self, zoom, geojson):
        # Identify tiles within priority zones defined by a GeoJSON.
        tiles = []
        geom = shape(geojson['features'][0]['geometry'])
        for x in range(2 ** zoom):
            for y in range(2 ** zoom):
                bounds = self.tile_bounds(x, y, zoom)
                tile_poly = box(bounds[0], bounds[1], bounds[2], bounds[3])
                if tile_poly.intersects(geom):
                    tiles.append((zoom, x, y))
        return tiles

# Entry point for the script.
if __name__ == "__main__":
    # Setup configuration with URL template, zoom levels, rate limit, MBTiles file path, user agent, and max threads.
    config = Config(
        url_template="https://tile.openstreetmap.org/{zoom}/{x}/{y}.png",
        zoom_levels=(0, 3),  # Minimum and maximum zoom levels.
        rate_limit=100,  # Number of requests per second.
        mbtiles_file=r"",  # Specify the path to the MBTiles file.
        user_agent='OpenStreetMapTileDownloader/1.0',  # User-Agent for HTTP requests.
        max_threads=5  # Maximum number of concurrent download threads.
    )

    session_manager = SessionManager(config.user_agent)  # Initialize the session manager with the user agent.

    try:
        mbtiles_manager = MBTilesManager(config.mbtiles_file)  # Initialize the MBTiles manager with the database path.
        mbtiles_manager.initialize_db()  # Ensure that the database schema is set up.

        tile_downloader = TileDownloader(config, session_manager, mbtiles_manager)  # Initialize the tile downloader.
        tile_downloader.download_tiles_to_mbtiles(geojson_path=r"")  # Start downloading tiles, specify the GeoJSON path if needed.
    except Exception as e:
        logging.error(f"An error occurred: {e}")  # Log any exceptions that occur during execution.
