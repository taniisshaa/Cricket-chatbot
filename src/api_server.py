import http.server
import socketserver
import asyncio
import json
import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
load_dotenv(override=True)
from src.environment.history_service import sync_recent_finished_matches
from src.utils.utils_core import get_logger
logger = get_logger("api_server", "API_SERVER.log")
PORT = 8000
class SyncHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/sync-finished'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            logger.info("Received Sync Request")
            try:
                result = asyncio.run(sync_recent_finished_matches(days_back=2))
                response = json.dumps(result, indent=2).encode('utf-8')
                self.wfile.write(response)
            except Exception as e:
                logger.error(f"Sync Error: {e}")
                err_resp = json.dumps({"status": "error", "message": str(e)}).encode('utf-8')
                self.wfile.write(err_resp)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found. Use /sync-finished')
def run_server():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), SyncHandler) as httpd:
        print(f"Serving API at http://localhost:{PORT}")
        print(f"Endpoint: GET http://localhost:{PORT}/sync-finished")
        logger.info(f"Server started on port {PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()
if __name__ == "__main__":
    run_server()