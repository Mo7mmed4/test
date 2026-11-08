#!/usr/bin/env python3
"""ORBIT ATLAS public open-data Earth-orbit tracker. Not a live radar."""
import csv, gzip, io, json, os, pathlib, threading, time, urllib.request
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlsplit

ROOT=pathlib.Path(__file__).resolve().parent
CACHE=pathlib.Path(os.environ.get('ORBIT_ATLAS_CACHE_DIR', '/tmp/orbit-atlas'))
CACHE.mkdir(exist_ok=True)
SOURCES={
 'orbit':('https://celestrak.org/pub/TLE/catalog.csv',7440),
 'satcat':('https://celestrak.org/pub/satcat.csv',86400),
 'military':('https://celestrak.org/NORAD/elements/gp.php?GROUP=MILITARY&FORMAT=CSV',7440)
}
LOCK=threading.RLock()
STATE={'until':0,'plain':None,'compressed':None,'count':0}
OMM_KEYS={'OBJECT_NAME','OBJECT_ID','EPOCH','MEAN_MOTION','ECCENTRICITY','INCLINATION','RA_OF_ASC_NODE','ARG_OF_PERICENTER','MEAN_ANOMALY','EPHEMERIS_TYPE','CLASSIFICATION_TYPE','NORAD_CAT_ID','ELEMENT_SET_NO','REV_AT_EPOCH','BSTAR','MEAN_MOTION_DOT','MEAN_MOTION_DDOT'}
META_KEYS=('NORAD_CAT_ID','OBJECT_NAME','OBJECT_TYPE','OPS_STATUS_CODE','OWNER','LAUNCH_DATE','DECAY_DATE','PERIOD','INCLINATION','APOGEE','PERIGEE','ORBIT_CENTER')
def source(name):
 url,ttl=SOURCES[name]
 f=CACHE/(name+'.csv')
 mf=CACHE/(name+'.json')
 try: m=json.loads(mf.read_text()) if mf.exists() else {}
 except Exception: m={}
 now=time.time()
 age=now-float(m.get('retrieved',0))
 err=m.get('error')
 if (not f.exists() or age>=ttl) and now-float(m.get('attempt',0))>=ttl:
  m['attempt']=now
  try:
   req=urllib.request.Request(url,headers={'User-Agent':'OrbitAtlas/0.3 (+https://celestrak.org/usage-policy.php)','Accept':'text/csv'})
   with urllib.request.urlopen(req,timeout=38) as r:
    if r.status!=200: raise ValueError('upstream HTTP '+str(r.status))
    raw=r.read(75_000_001)
   if len(raw)>75_000_000:raise ValueError('file too large')
   txt=raw.decode('utf-8-sig')
   if 'NORAD_CAT_ID' not in txt[:2048]:raise ValueError('unexpected CSV fields')
   tmp=f.with_suffix('.tmp')
   tmp.write_text(txt,encoding='utf8');os.replace(tmp,f)
   m.update(retrieved=time.time(),error=None)
  except Exception as e:m['error']=str(e)
  mf.write_text(json.dumps(m),encoding='utf8')
  err=m.get('error')
 if not f.exists():return [],{'url':url,'rows':0,'stale':True,'updated':None,'error':err or 'no data'}
 with f.open(encoding='utf8-sig',newline='') as fp:rows=list(csv.DictReader(fp))
 return rows,{'url':url,'rows':len(rows),'stale':time.time()-float(m.get('retrieved',0))>ttl,'updated':m.get('retrieved'),'error':err}
def sid(row):
 try:return str(int(row['NORAD_CAT_ID']))
 except (TypeError,ValueError,KeyError):return None
def dataset():
 a,asrc=source('orbit')
 b,bsrc=source('satcat')
 c,csrc=source('military')
 mid={sid(r) for r in c if sid(r)}
 orbits={}
 for r in a+c:
  k=sid(r)
  if k and r.get('EPOCH') and r.get('MEAN_MOTION'):
   orbits[k]={x:r.get(x,'') for x in OMM_KEYS if x in r}
 catalog={}
 for r in b:
  if r.get('DECAY_DATE','').strip():continue
  center=(r.get('ORBIT_CENTER') or '').strip().upper()
  if center and center not in ('EA','EARTH','E'):continue
  k=sid(r)
  if k:catalog[k]=[r.get(x,'') for x in META_KEYS]
 data={'generated_at':time.time(),'orbits':list(orbits.values()),'catalog':catalog,
       'military_ids':list(mid),'sources':{'orbit':asrc,'satcat':bsrc,'military':csrc},
       'counts':{'elements':len(orbits),'catalog':len(catalog),'military':len(mid),'without_elements':len(set(catalog)-set(orbits))},
       'limitations':'Published orbit predictions, not real-time sensor measurements. Unknown and restricted objects cannot be claimed as detected.'}
 return json.dumps(data,ensure_ascii=False,separators=(',',':')).encode()
HTML=(ROOT/'index.html').read_bytes()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  path=urlsplit(self.path).path
  if path=='/health':self.reply(b'{"ok":true,"product":"ORBIT ATLAS"}','application/json');return
  if path in ('/','/index.html'):self.reply(HTML,'text/html; charset=utf-8');return
  if path=='/api/data':
   with LOCK:
    now=time.time()
    if STATE['plain'] is None or now>STATE['until']:
     try:
      blob=dataset()
      STATE.update(plain=blob,compressed=gzip.compress(blob,compresslevel=4),until=time.time()+120)
     except Exception as e:
      if STATE['plain'] is None:
       self.reply(json.dumps({'error':str(e)}).encode(),'application/json',503);return
    zipok='gzip' in self.headers.get('Accept-Encoding','')
    b=STATE['compressed'] if zipok else STATE['plain']
   self.reply(b,'application/json; charset=utf-8',extra={'Content-Encoding':'gzip'} if zipok else None);return
  self.send_error(404)
 def reply(self,b,typ,status=200,extra=None):
  self.send_response(status)
  self.send_header('Content-Type',typ)
  self.send_header('Content-Length',str(len(b)))
  self.send_header('Cache-Control','no-store' if typ.startswith('application/json') else 'public,max-age=120')
  self.send_header('X-Content-Type-Options','nosniff')
  if extra:
   for k,v in extra.items():self.send_header(k,v)
  self.end_headers()
  try:self.wfile.write(b)
  except (BrokenPipeError,ConnectionResetError):pass
 def log_message(self,fmt,*args):print('[orbit]',fmt%args,flush=True)
if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser()
 p.add_argument('--host',default='0.0.0.0')
 p.add_argument('--port',type=int,default=int(os.environ.get('PORT',10000)))
 a=p.parse_args()
 print('Serving on '+a.host+':'+str(a.port),flush=True)
 ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
