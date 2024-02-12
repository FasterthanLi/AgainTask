import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sqlite3
import threading
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from shapely.geometry import shape, box
import math

# Set up primary logging for general activity
logging.basicConfig(filename='tile_downloader.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Set up a separate logger for already downloaded tiles
skipped_tiles_logger = logging.getLogger("skipped_tiles")
skipped_tiles_logger.setLevel(logging.INFO)
skipped_tiles_handler = logging.FileHandler('skipped_tiles.log')
skipped_tiles_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
skipped_tiles_logger.addHandler(skipped_tiles_handler)
skipped_tiles_logger.propagate = False  # Prevent logs from being propagated to parent loggers



class Config:
    def __init__(self, url_template, zoom_levels, rate_limit, mbtiles_file, user_agent, max_threads):
        self.url_template = url_template
        self.zoom_levels = zoom_levels
        self.rate_limit = rate_limit
        self.mbtiles_file = mbtiles_file
        self.user_agent = user_agent
        self.max_threads = max_threads

class SessionManager:
    def __init__(self, user_agent):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def get(self, url):
        return self.session.get(url, stream=True)

class MBTilesManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.local_conn = threading.local()
        self.initialize_db()

    def get_conn(self):
        if not hasattr(self.local_conn, "conn"):
            self.local_conn.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        return self.local_conn.conn

    def initialize_db(self):
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
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (zoom, x, 2**zoom - 1 - y))
        exists = cur.fetchone()[0] > 0
        return exists

    def save_tile(self, zoom, x, y, tile_data):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)", (zoom, x, 2**zoom - 1 - y, sqlite3.Binary(tile_data)))
        conn.commit()

    def close_conn(self):
        if hasattr(self.local_conn, "conn"):
            self.local_conn.conn.close()
            del self.local_conn.conn

class TileDownloader:
    def __init__(self, config, session_manager, mbtiles_manager):
        self.config = config
        self.session_manager = session_manager
        self.mbtiles_manager = mbtiles_manager
        self.semaphore = threading.Semaphore(config.rate_limit * config.max_threads)
        # Initialize the progress bar lock
        self.pbar_lock = threading.Lock()

    def download_tile(self, zoom, x, y, pbar, pbar_lock):
        thread_id = threading.get_ident()
        tile_url = self.config.url_template.format(zoom=zoom, x=x, y=y)
        logging.info(f"Thread {thread_id}: Starting download {tile_url}")
        try:
            response = self.session_manager.get(tile_url)
            if response.status_code == 200:
                self.mbtiles_manager.save_tile(zoom, x, y, response.content)
                logging.info(f"Thread {thread_id}: Downloaded {tile_url}")
            else:
                logging.warning(f"Thread {thread_id}: Tile download failed {tile_url}, status code: {response.status_code}")
        finally:
            # Synchronize access to the progress bar
            with self.pbar_lock:
                pbar.update(1)
            # Close the SQLite connection for this thread
            self.mbtiles_manager.close_conn()

    def download_tiles(self, tiles, pbar):
        with ThreadPoolExecutor(max_workers=self.config.max_threads) as executor:
            # Pass the pbar_lock to each task
            futures = [executor.submit(self.download_tile_rate_limited, zoom, x, y, pbar, self.pbar_lock) for zoom, x, y in tiles]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Exception occurred: {e}")
                    # Synchronize access to the progress bar in case of an error
                    with self.pbar_lock:
                        pbar.update(1)

    def download_tile_rate_limited(self, zoom, x, y, pbar, pbar_lock):
        with self.semaphore:
            if not self.mbtiles_manager.tile_exists(zoom, x, y):
                self.download_tile(zoom, x, y, pbar, pbar_lock)
            else:
                skipped_tiles_logger.info(f"Skipped: Tile already exists: zoom {zoom}, x {x}, y {y}")  # Log skipped tiles with skipped_tiles_logger
                with pbar_lock:
                    pbar.update(1)


    
    def download_tiles_to_mbtiles(self, geojson_path=None):
        priority_tiles = []
        if geojson_path:
            geojson = self.load_geojson(geojson_path)
            for zoom in range(self.config.zoom_levels[0], self.config.zoom_levels[1] + 1):
                priority_tiles += self.tiles_in_priority_zones(zoom, geojson)

        total_tiles = sum([4 ** z for z in range(self.config.zoom_levels[0], self.config.zoom_levels[1] + 1)])
        non_priority_tiles = [(zoom, x, y) for zoom in range(self.config.zoom_levels[0], self.config.zoom_levels[1] + 1) for x in range(2 ** zoom) for y in range(2 ** zoom) if (zoom, x, y) not in priority_tiles]
        all_tiles = priority_tiles + non_priority_tiles

        pbar = tqdm(total=total_tiles, desc='Downloading tiles')
        self.download_tiles(all_tiles, pbar)  # Ensure this matches the method signature
        pbar.close()


    def load_geojson(self, geojson_path):
        with open(geojson_path, 'r') as f:
            return json.load(f)

    def tile_bounds(self, x, y, z):
        n = 2.0 ** z
        lon_min = x / n * 360.0 - 180.0
        lat_min_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat_min = math.degrees(lat_min_rad)
        lon_max = (x + 1) / n * 360.0 - 180.0
        lat_max_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
        lat_max = math.degrees(lat_max_rad)
        return (lon_min, lat_min, lon_max, lat_max)

    def tiles_in_priority_zones(self, zoom, geojson):
        tiles = []
        geom = shape(geojson['features'][0]['geometry'])
        for x in range(2 ** zoom):
            for y in range(2 ** zoom):
                bounds = self.tile_bounds(x, y, zoom)
                tile_poly = box(bounds[0], bounds[1], bounds[2], bounds[3])
                if tile_poly.intersects(geom):
                    tiles.append((zoom, x, y))
        return tiles


if __name__ == "__main__":
    config = Config(
        url_template="https://tile.openstreetmap.org/{zoom}/{x}/{y}.png",
        zoom_levels=(0, 3),
        rate_limit=100,
        mbtiles_file=r"C:\Users\1\Desktop\SQL\tiles.mbtiles",
        user_agent='OpenStreetMapTileDownloader/1.0',
        max_threads=5
    )

    session_manager = SessionManager(config.user_agent)

    try:
        mbtiles_manager = MBTilesManager(config.mbtiles_file)
        mbtiles_manager.initialize_db()  # Ensures that the database is initialized here

        tile_downloader = TileDownloader(config, session_manager, mbtiles_manager)
        tile_downloader.download_tiles_to_mbtiles(geojson_path=r"C:\Users\1\Desktop\Job\central_park_section.geojson")
    except Exception as e:
        logging.error(f"An error occurred: {e}")