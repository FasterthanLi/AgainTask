import unittest
from unittest.mock import patch, MagicMock
from task import Config, SessionManager, MBTilesManager, TileDownloader

class TestConfig(unittest.TestCase):
    def test_config_initialization(self):
        config = Config("url_template", (0, 5), 100, "path/to/mbtiles", "user_agent", 10)
        self.assertEqual(config.url_template, "url_template")
        self.assertEqual(config.zoom_levels, (0, 5))
        self.assertEqual(config.rate_limit, 100)
        self.assertEqual(config.mbtiles_file, "path/to/mbtiles")
        self.assertEqual(config.user_agent, "user_agent")
        self.assertEqual(config.max_threads, 10)

class TestSessionManager(unittest.TestCase):
    @patch('requests.Session')
    def test_session_setup(self, MockSession):
        user_agent = "TestAgent"
        session_manager = SessionManager(user_agent)
        MockSession.assert_called_once()
        self.assertEqual(session_manager.session.headers["User-Agent"], user_agent)

class TestMBTilesManager(unittest.TestCase):
    def setUp(self):
        self.mbtiles_manager = MBTilesManager(":memory:")

    @patch('sqlite3.connect')
    def test_initialize_db(self, mock_connect):
        self.mbtiles_manager.initialize_db()
        mock_connect.assert_called_with(":memory:")
        # Add more assertions here to check if SQL commands are executed

    @patch('sqlite3.connect')
    def test_tile_exists(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        exists = self.mbtiles_manager.tile_exists(1, 2, 3)
        self.assertTrue(exists)
        # Add more checks and scenarios

class TestTileDownloader(unittest.TestCase):
    @patch.object(SessionManager, 'get')
    @patch.object(MBTilesManager, 'save_tile')
    def test_download_tile(self, mock_save_tile, mock_get):
        config = Config("url_template", (0, 5), 100, "path/to/mbtiles", "user_agent", 10)
        session_manager = SessionManager(config.user_agent)
        mbtiles_manager = MBTilesManager(":memory:")
        tile_downloader = TileDownloader(config, session_manager, mbtiles_manager)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"tile data"
        mock_get.return_value = mock_response

        result = tile_downloader.download_tile(1, 1, 1)
        self.assertIn("Downloaded", result)
        mock_save_tile.assert_called_with(1, 1, 1, b"tile data")
        # Add more tests for different scenarios like failed downloads

# Add more test cases for other methods and functionalities

if __name__ == '__main__':
    unittest.main()
