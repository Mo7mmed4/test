from http.server import BaseHTTPRequestHandler
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        data=b'{"ok":true,"service":"orbit-atlas-vercel","mode":"public-catalogue"}'
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers()
        self.wfile.write(data)
