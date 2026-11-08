"""ORBIT ATLAS v0.4 — public-data fusion, not a direct radar connection.

CelesTrak GP refresh policy: no more than one large-group retrieval per ~2h.
CDN caches this entire response. Ephemeris positions are propagated client-side.
"""
import csv
import io
import json
import gzip
import math
import os
import pathlib
import re
import time
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler

ROOT = pathlib.Path('/tmp/orbit-atlas-v04')
ROOT.mkdir(parents=True, exist_ok=True)
NOW = lambda: time.time()

FEEDS = [
    ('cache-active', 'ReTLEctor: active GP cache', 'CelesTrak via ReTLEctor', 'orbit-cache', 'https://retlector.eu/active/json', 'primary'),
    ('celestrak-active', 'CelesTrak active GP', 'CelesTrak', 'orbit', 'https://celestrak.org/NORAD/elements/gp.php?GROUP=ACTIVE&FORMAT=JSON', 'fallback'),
    ('celestrak-military', 'CelesTrak public military GP', 'CelesTrak', 'orbit', 'https://celestrak.org/NORAD/elements/gp.php?GROUP=MILITARY&FORMAT=JSON', 'enabled'),
    ('celestrak-stations', 'CelesTrak space stations', 'CelesTrak', 'orbit', 'https://celestrak.org/NORAD/elements/gp.php?GROUP=STATIONS&FORMAT=JSON', 'enabled'),
    ('celestrak-visual', 'CelesTrak visual', 'CelesTrak', 'orbit', 'https://celestrak.org/NORAD/elements/gp.php?GROUP=VISUAL&FORMAT=JSON', 'enabled'),
    ('celestrak-cosmos-debris', 'COSMOS 2251 debris mirror', 'Satvisor mirror of CelesTrak', 'orbit', 'https://raw.githubusercontent.com/satvisorcom/satvisor-data/master/celestrak/json/cosmos-2251-debris.json', 'enabled'),
    ('celestrak-iridium-debris', 'IRIDIUM 33 debris mirror', 'Satvisor mirror of CelesTrak', 'orbit', 'https://raw.githubusercontent.com/satvisorcom/satvisor-data/master/celestrak/json/iridium-33-debris.json', 'enabled'),
    ('celestrak-fengyun-debris', 'Fengyun-1C debris mirror', 'Satvisor mirror of CelesTrak', 'orbit', 'https://raw.githubusercontent.com/satvisorcom/satvisor-data/master/celestrak/json/fengyun-1c-debris.json', 'enabled'),
    ('satnogs-tle', 'SatNOGS latest TLE records', 'SatNOGS', 'orbit-observatory', 'https://db.satnogs.org/api/tle/?page_size=100', 'enabled'),
    ('satnogs-satellites', 'SatNOGS satellite registry', 'SatNOGS', 'metadata', 'https://db.satnogs.org/api/satellites/?page_size=50', 'enabled'),
    ('satnogs-optical', 'SatNOGS optical observations', 'SatNOGS', 'observations', 'https://optical.satnogs.org/data', 'not-automatic'),
    ('satnogs-network', 'SatNOGS ground observations', 'SatNOGS', 'observations', 'https://network.satnogs.org/api/observations/?page_size=20', 'enabled'),
    ('satnogs-telemetry', 'SatNOGS downlinked telemetry', 'SatNOGS', 'observations', 'https://db.satnogs.org/api/telemetry/?page_size=20', 'available-not-polled'),
    ('satnogs-ground-stations', 'SatNOGS station registry', 'SatNOGS', 'metadata', 'https://network.satnogs.org/api/stations/', 'available-not-polled'),
    ('celestrak-satcat', 'CelesTrak catalog metadata', 'CelesTrak', 'metadata', 'https://celestrak.org/satcat/records.php?GROUP=STATIONS&FORMAT=JSON', 'available-not-polled'),
    ('spacetrack', 'Space-Track orbital catalog', 'US Space Force / Space-Track', 'restricted', 'https://www.space-track.org/', 'requires-account'),
    ('esa-discos', 'ESA DISCOS object database', 'ESA', 'restricted', 'https://discosweb.esoc.esa.int/', 'requires-auth'),
    ('eu-sst', 'EU SST service', 'EU SST', 'restricted', 'https://www.eusst.eu/', 'requires-auth'),
    ('leolabs-radar', 'LeoLabs commercial radar', 'LeoLabs', 'restricted', 'https://leolabs.space/', 'requires-provider-agreement'),
    ('ilrs-slr', 'International Laser Ranging Service', 'ILRS', 'measurements', 'https://ilrs.cddis.eosdis.nasa.gov/', 'not-automatic'),
]
assert len(FEEDS) == 20
META_KEYS = ('NORAD_CAT_ID','OBJECT_NAME','OBJECT_TYPE','OPS_STATUS_CODE','OWNER','LAUNCH_DATE','DECAY_DATE','ORBIT_CENTER')
OMM_KEYS = ('OBJECT_NAME','OBJECT_ID','EPOCH','MEAN_MOTION','ECCENTRICITY','INCLINATION',
            'RA_OF_ASC_NODE','ARG_OF_PERICENTER','MEAN_ANOMALY','NORAD_CAT_ID',
            'EPHEMERIS_TYPE','CLASSIFICATION_TYPE','ELEMENT_SET_NO','REV_AT_EPOCH',
            'BSTAR','MEAN_MOTION_DOT','MEAN_MOTION_DDOT')
BY_ID = {f[0]:f for f in FEEDS}
ALLOW = {f[0]:f[4] for f in FEEDS if f[5] in ('primary','fallback','enabled')}
UA = 'ORBIT-ATLAS/0.4 (scientific public data; CelesTrak usage policy compliant)'

def load_json(feed_id, timeout=10):
    url = ALLOW[feed_id]  # Strict allowlist: no caller supplied URLs.
    req = urllib.request.Request(url,headers={
        'User-Agent':UA,'Accept':'application/json, text/json;q=0.9, */*;q=0.5',
        'Accept-Encoding':'identity'})
    try:
        with urllib.request.urlopen(req,timeout=timeout) as resp:
            raw = resp.read(15_000_001)
            if len(raw)>15_000_000:raise ValueError('upstream size cap')
            if resp.headers.get('content-encoding')=='gzip':raw=gzip.decompress(raw)
        payload=json.loads(raw.decode('utf-8-sig'))
        if isinstance(payload,dict):
            if isinstance(payload.get('data'),list):payload=payload['data']
            elif isinstance(payload.get('results'),list):payload=payload['results']
            elif isinstance(payload.get('tle'),list):payload=payload['tle']
            elif isinstance(payload.get('satellites'),list):payload=payload['satellites']
            else:payload=[payload]
        if not isinstance(payload,list):raise ValueError('invalid upstream JSON shape')
        return payload,None
    except Exception as e:
        return [],(str(e)[:170] or type(e).__name__)

def find_id(obj):
    for k in ('NORAD_CAT_ID','norad_cat_id','norad_id','noradCatId','catalogNumber','norad'):
        v=obj.get(k)
        if v not in (None,''):
            try:return str(int(v))
            except (TypeError,ValueError):pass
    return None

def epoch_ts(value):
    if not value:return None
    try:return datetime.fromisoformat(str(value).replace('Z','+00:00')).replace(tzinfo=timezone.utc).timestamp() if '+' not in str(value) and str(value)[-1:]!='Z' else datetime.fromisoformat(str(value).replace('Z','+00:00')).timestamp()
    except Exception:return None

def normalize(obj):
    if not isinstance(obj,dict):return None
    sid=find_id(obj)
    if not sid:return None
    if all(k in obj for k in ('EPOCH','MEAN_MOTION','INCLINATION','MEAN_ANOMALY')):
        out={k:obj.get(k,'') for k in OMM_KEYS if k in obj}
        out['NORAD_CAT_ID']=sid
        return ('omm',out)
    # SatNOGS TLE (restricted to catalog numbers TLE can represent)
    t1=obj.get('tle1') or obj.get('TLE1') or obj.get('line1') or obj.get('tle_line1')
    t2=obj.get('tle2') or obj.get('TLE2') or obj.get('line2') or obj.get('tle_line2')
    if t1 and t2 and t1.startswith('1 ') and t2.startswith('2 '):
        return ('tle',{'NORAD_CAT_ID':sid,'OBJECT_NAME':obj.get('tle0') or obj.get('name') or ('NORAD '+sid),
            'TLE_LINE1':t1,'TLE_LINE2':t2,'EPOCH':obj.get('updated') or obj.get('created') or ''})
    return None

def run():
    t=NOW()
    # Primary cache is fetched first; official massive group only when the proxy is unavailable.
    primary,primary_error=load_json('cache-active',timeout=11)
    values={'cache-active':(primary,primary_error)}
    keys=['celestrak-military','celestrak-stations','celestrak-visual',
          'celestrak-cosmos-debris','celestrak-iridium-debris','celestrak-fengyun-debris',
          'satnogs-tle','satnogs-satellites','satnogs-network']
    if not primary:keys.insert(0,'celestrak-active')
    # Cap request concurrency; overlimit providers are never retried in a loop.
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs={pool.submit(load_json,k,8):k for k in keys}
        for fut in as_completed(futs):
            k=futs[fut]
            try:values[k]=fut.result()
            except Exception as e:values[k]=([],str(e)[:170])
    objects={}
    divergences=[]
    military=set()
    independent_matches=set()
    for feed_id in ('cache-active','celestrak-active','celestrak-military','celestrak-stations','celestrak-visual',
                    'celestrak-cosmos-debris','celestrak-iridium-debris','celestrak-fengyun-debris','satnogs-tle'):
        for raw in values.get(feed_id,([],None))[0]:
            norm=normalize(raw)
            if not norm:continue
            kind,omm=norm
            sid=omm['NORAD_CAT_ID']
            if feed_id=='celestrak-military':military.add(sid)
            cur=objects.get(sid)
            if not cur:
                cur={'id':sid,'name':str(omm.get('OBJECT_NAME') or 'NORAD '+sid),
                     'format':kind,'omm':omm,'sources':[feed_id],'epoch':omm.get('EPOCH','')}
                objects[sid]=cur
            else:
                if feed_id not in cur['sources']:cur['sources'].append(feed_id)
                if feed_id=='satnogs-tle' and cur['format']=='omm':
                    independent_matches.add(sid)
                    cur['cross_check']='Two different organizations list this NORAD ID (not a measured orbit verification)'
                # Prefer newer OMM epoch over older OMM; TLE is fallback only.
                old=epoch_ts(cur.get('epoch')) or 0
                new=epoch_ts(omm.get('EPOCH')) or 0
                if kind=='omm' and (cur['format']!='omm' or new>old):
                    cur.update(format=kind,omm=omm,epoch=omm.get('EPOCH',''),name=str(omm.get('OBJECT_NAME') or cur['name']))
                if kind=='omm' and cur['format']=='omm' and new>0 and old>0 and abs(new-old)<86400:
                    try:
                        delta=abs(float(omm.get('MEAN_MOTION') or 0)-float(cur['omm'].get('MEAN_MOTION') or 0))
                        if delta>.01 and len(divergences)<25:
                            divergences.append({'id':sid,'sources':cur['sources'],'mean_motion_diff':round(delta,6)})
                    except Exception:pass
    for sid,obj in objects.items():
        n=obj['name'].upper()
        if sid in military:cls='military'
        elif ' DEB' in n or n.endswith('DEB') or 'DEBRIS' in n:cls='debris'
        elif 'R/B' in n or 'ROCKET' in n:cls='rocket'
        else:cls='payload'
        obj['kind']=cls
        age=(t-(epoch_ts(obj.get('epoch')) or 0))/3600 if epoch_ts(obj.get('epoch')) else None
        obj['age_h']=round(age,1) if age is not None else None
        obj['freshness']='current' if age is not None and 0<=age<=24 else 'aging' if age is not None and 0<=age<=72 else 'stale'
        # Deduped per ID; independent source matches are informational, not independent ranging.
        obj['independent_cross_reference']=sid in independent_matches
    statuses=[]
    for id,name,provider,kind,url,mode in FEEDS:
        info=values.get(id)
        status=('ok' if info and info[0] else 'error' if info else 'not-requested')
        if mode.startswith('requires') or mode=='not-automatic':status=mode
        statuses.append({
            'id':id,'name':name,'provider':provider,'type':kind,'mode':mode,
            'status':status,'rows':len(info[0]) if info else 0,'error':info[1] if info else None,
            'checked_at':t if info else None,'url':url
        })
    c={'payload':0,'debris':0,'rocket':0,'military':0}
    for obj in objects.values():c[obj['kind']]+=1
    return {
        'generated_at':t,
        'records':list(objects.values()),
        'counts':{'orbits':len(objects),'categories':c,'independent_name_matches':len(independent_matches)},
        'sources':statuses,'total_source_modules':len(FEEDS),
        'actual_providers':sorted({f['provider'] for f in statuses if f['status']=='ok'}),
        'divergences':divergences,
        'notes':[
            'Orbit positions are propagated with SGP4, not measured in real time.',
            'Some catalog objects may have no public orbit elements and are absent.',
            'Feeds from CelesTrak are correlated, not independent measurements.',
            'Radar and military sensor systems require individual authorization and are not connected.',
            'SatNOGS observations/telemetry are not a ranging radar solution.'
        ]}

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            response=run()
            blob=json.dumps(response,ensure_ascii=False,separators=(',',':')).encode('utf-8')
            # Keep safely below Vercel function payload limit even for large catalogs.
            zipped=gzip.compress(blob,compresslevel=5)
            if len(zipped)>4_000_000:raise ValueError('compressed API payload exceeds hosting budget')
            self.send_response(200)
            self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Content-Encoding','gzip')
            self.send_header('Content-Length',str(len(zipped)))
            self.send_header('Cache-Control','public, s-maxage=7200, stale-while-revalidate=86400')
            self.send_header('Vary','Accept-Encoding')
            self.send_header('X-Content-Type-Options','nosniff')
            self.end_headers()
            self.wfile.write(zipped)
        except Exception as e:
            payload=json.dumps({'error':'Source fusion failed','detail':str(e)[:200]}).encode()
            self.send_response(503)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(payload)))
            self.send_header('Cache-Control','no-store')
            self.end_headers()
            self.wfile.write(payload)
