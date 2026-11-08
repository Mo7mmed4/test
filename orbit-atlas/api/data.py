"""Vercel Python Function. Public CelesTrak data only; SGP4 runs in the browser."""
from http.server import BaseHTTPRequestHandler
import gzip,json,os,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from server import dataset
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data=json.loads(dataset())
            payload=json.dumps(data,ensure_ascii=False,separators=(',',':')).encode('utf8')
            if len(payload)>50_000_000:raise RuntimeError('result exceeded configured output cap')
            encoded=gzip.compress(payload,compresslevel=5)
            self.send_response(200)
            self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Content-Encoding','gzip')
            self.send_header('Content-Length',str(len(encoded)))
            self.send_header('Cache-Control','public, s-maxage=7200, stale-while-revalidate=86400')
            self.send_header('Vary','Accept-Encoding')
            self.end_headers()
            self.wfile.write(encoded)
        except Exception as e:
            raw=json.dumps({'error':'Unable to retrieve orbital data; upstream service unavailable','detail':str(e)[:180]}).encode()
            self.send_response(503)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(raw)))
            self.send_header('Cache-Control','no-store')
            self.end_headers()
            self.wfile.write(raw)
