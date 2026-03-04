"""
Gold Tracker API Server
Flask backend — all API keys stay server-side, never in frontend.
Run with: gunicorn -w 2 -b 127.0.0.1:5000 app:app
"""
import json
import os
import time
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from fetcher import fetch_fred, fetch_cot, fetch_yfinance

app = Flask(__name__)
CORS(app, origins=['http://gold.hb-labs.com', 'http://159.223.44.23'])

CACHE_DIR = '/opt/gold-tracker/api/cache'
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_TTL = {
    'fred.json': 24 * 3600,       # 24 hours
    'cot.json':  7 * 24 * 3600,   # 7 days
    'yfinance.json': 24 * 3600,   # 24 hours
}


def cache_path(filename):
    return os.path.join(CACHE_DIR, filename)


def is_fresh(filename):
    path = cache_path(filename)
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age < CACHE_TTL.get(filename, 24 * 3600)


def read_cache(filename):
    with open(cache_path(filename)) as f:
        return json.load(f)


def write_cache(filename, data):
    with open(cache_path(filename), 'w') as f:
        json.dump(data, f)
    return data


STATUS_JSON_PATH = '/var/www/gold-tracker/data/status.json'


def compute_analytics():
    """Compute all dashboard analytics from cached data. Returns a dict."""
    def load(name):
        p = cache_path(name)
        if not os.path.exists(p):
            return None, None
        with open(p) as f:
            return json.load(f), os.path.getmtime(p)

    fred, fred_mtime = load('fred.json')
    cot_data, cot_mtime = load('cot.json')
    yf, yf_mtime = load('yfinance.json')
    if not fred or not yf:
        return None

    # --- Monthly pairs for dual-regime regression ---
    tips_monthly = [t for t in (fred.get('tips_monthly') or []) if t.get('value') is not None]
    gold_monthly = fred.get('gold_monthly') or []
    gm_map = {g['date']: g['value'] for g in gold_monthly}
    pairs = sorted(
        [{'date': t['date'], 'tips': t['value'], 'gold': gm_map[t['date']]}
         for t in tips_monthly if t['date'] in gm_map],
        key=lambda x: x['date'],
    )
    latest = pairs[-1] if pairs else None
    regime1 = [(p['tips'], p['gold']) for p in pairs if p['date'] <= '2021-12-31']
    regime2 = [(p['tips'], p['gold']) for p in pairs if p['date'] >= '2022-01-01']

    def ols(pts):
        n = len(pts)
        if n < 2:
            return None
        sx = sum(x for x, y in pts); sy = sum(y for x, y in pts)
        sxy = sum(x * y for x, y in pts); sx2 = sum(x * x for x, y in pts)
        d = n * sx2 - sx * sx
        if not d:
            return None
        sl = (n * sxy - sx * sy) / d
        ic = (sy - sl * sx) / n
        ym = sy / n
        ss_tot = sum((y - ym) ** 2 for x, y in pts)
        ss_res = sum((y - (sl * x + ic)) ** 2 for x, y in pts)
        r2v = 1 - ss_res / ss_tot if ss_tot else 0
        return {'slope': round(sl, 2), 'intercept': round(ic, 2), 'r2': round(r2v, 4), 'n': n}

    r1 = ols(regime1)
    r2 = ols(regime2)
    current_tips = latest['tips'] if latest else None
    current_gold = latest['gold'] if latest else None
    r1_pred = round(r1['slope'] * current_tips + r1['intercept'], 2) if r1 and current_tips is not None else None
    r2_pred = round(r2['slope'] * current_tips + r2['intercept'], 2) if r2 and current_tips is not None else None
    premium = round(current_gold - r1_pred, 2) if current_gold is not None and r1_pred is not None else None
    premium_pct = round(premium / r1_pred * 100, 1) if premium is not None and r1_pred else None

    # --- Real rates ---
    def last_valid(series):
        arr = [x for x in (series if isinstance(series, list) else []) if x.get('value') is not None]
        return arr[-1] if arr else None

    tips_daily = sorted([x for x in (fred.get('tips') or []) if x.get('value') is not None], key=lambda x: x['date'])
    dxy_daily  = sorted([x for x in (fred.get('dxy')  or []) if x.get('value') is not None], key=lambda x: x['date'])
    sofr_rec   = last_valid(fred.get('sofr', []))
    tips_3m_chg = round(tips_daily[-1]['value'] - tips_daily[max(0, len(tips_daily) - 63)]['value'], 3) if len(tips_daily) >= 2 else None
    dxy_90d_chg = round(dxy_daily[-1]['value']  - dxy_daily[max(0, len(dxy_daily) - 90)]['value'],   3) if len(dxy_daily) >= 2 else None

    # --- COT ---
    cot_rows   = (cot_data or {}).get('data', [])
    cot_latest = cot_rows[-1] if cot_rows else {}
    cot_net    = cot_latest.get('net_long', 0)
    window     = sorted(r['net_long'] for r in cot_rows[-260:]) if cot_rows else [cot_net]
    cot_pctile = round(sum(1 for v in window if v <= cot_net) / len(window) * 100, 1) if window else None

    # --- Ratios ---
    def yf_close(key):
        arr = yf.get(key, [])
        return (arr[-1] if isinstance(arr, list) and arr else {}).get('close')

    gld_p = yf_close('gld'); gdx_p = yf_close('gdx')
    gf_p  = yf_close('gold_futures'); sf_p = yf_close('silver_futures')
    GLD_OZ = 0.09334
    spot   = gld_p / GLD_OZ if gld_p else None
    cobasis = round(spot - gf_p, 2) if spot is not None and gf_p else None
    gsr     = round(gf_p / sf_p, 2) if gf_p and sf_p else None
    gdxgld  = round(gdx_p / gld_p, 4) if gdx_p and gld_p else None
    gld_arr = yf.get('gld', []); gdx_arr = yf.get('gdx', [])
    gdxgld_3yavg = None
    if isinstance(gld_arr, list) and isinstance(gdx_arr, list) and len(gld_arr) >= 756:
        gm2 = {x['date']: x['close'] for x in gld_arr}
        r3y = [g['close'] / gm2[g['date']] for g in gdx_arr[-756:] if g['date'] in gm2]
        gdxgld_3yavg = round(sum(r3y) / len(r3y), 4) if r3y else None

    # --- Scorecard signals (9 signals, matching analytics.js buildScorecard) ---
    def sig(v, bull_fn, bear_fn):
        if v is None:
            return 'neutral'
        if bull_fn(v):
            return 'bullish'
        if bear_fn(v):
            return 'bearish'
        return 'neutral'

    gdxgld_vs_avg = round(gdxgld - gdxgld_3yavg, 4) if gdxgld and gdxgld_3yavg else None
    signals = {
        'realRateTrend':    {'value': tips_3m_chg,   'signal': sig(tips_3m_chg,   lambda v: v < -0.1, lambda v: v > 0.1),  'label': '3M TIPS change'},
        'rateModelPremium': {'value': premium_pct,   'signal': sig(premium_pct,   lambda v: v < 10,   lambda v: v > 20),   'label': 'Regime break premium %'},
        'dxyTrend':         {'value': dxy_90d_chg,   'signal': sig(dxy_90d_chg,   lambda v: v < -1,   lambda v: v > 1),    'label': '90d DXY change'},
        'cotPercentile':    {'value': cot_pctile,    'signal': sig(cot_pctile,    lambda v: v < 20,   lambda v: v > 80),   'label': '5Y COT percentile'},
        'cobasis':          {'value': cobasis,       'signal': sig(cobasis,       lambda v: v > 0,    lambda v: v < -2),   'label': 'Spot minus futures ($/oz)'},
        'gdxGldVs3yAvg':    {'value': gdxgld_vs_avg, 'signal': sig(gdxgld_vs_avg, lambda v: v > 0,    lambda v: v < -0.01),'label': 'GDX/GLD vs 3Y avg'},
        'goldSilverRatio':  {'value': gsr,           'signal': sig(gsr,           lambda v: v < 70,   lambda v: v > 80),   'label': 'GC=F / SI=F'},
        'cbDemand':         {'value': None,           'signal': 'neutral',                                                   'label': 'Central bank demand (WGC)'},
        'etfFlowTrend':     {'value': None,           'signal': 'neutral',                                                   'label': 'ETF flow momentum'},
    }
    bullish_count = sum(1 for s in signals.values() if s['signal'] == 'bullish')

    def freshness(mtime):
        if not mtime:
            return None
        return {
            'lastUpdated': datetime.utcfromtimestamp(mtime).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'ageHours': round((time.time() - mtime) / 3600, 1),
        }

    return {
        'lastUpdated':            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'lastDate':               latest['date'] if latest else None,
        'currentGoldPrice':       current_gold,
        'currentTIPS':            current_tips,
        'regimeBreakPremium':     premium,
        'regimeBreakPremiumPct':  premium_pct,
        'regime1': {
            **(r1 or {}),
            'label':                   '2006-2021',
            'predictedAtCurrentRate':  r1_pred,
        },
        'regime2': {
            **(r2 or {}),
            'label':                   '2022-present',
            'predictedAtCurrentRate':  r2_pred,
        },
        'realRates': {
            'tips10y':      current_tips,
            'tips3mChange': tips_3m_chg,
            'dxy':          dxy_daily[-1]['value'] if dxy_daily else None,
            'dxy90dChange': dxy_90d_chg,
            'sofr':         sofr_rec['value'] if sofr_rec else None,
        },
        'cot': {
            'netLong':       cot_net,
            'netLongPct':    cot_latest.get('net_long_pct'),
            'openInterest':  cot_latest.get('open_interest'),
            'percentile5y':  cot_pctile,
            'date':          cot_latest.get('date'),
        },
        'ratios': {
            'goldSilverRatio': gsr,
            'cobasis':         cobasis,
            'gdxGld':          gdxgld,
            'gdxGld3yAvg':     gdxgld_3yavg,
        },
        'scorecard': {
            'bullishSignals': bullish_count,
            'totalSignals':   len(signals),
            'composite':      f'{bullish_count}/{len(signals)}',
            'signals':        signals,
        },
        'dataFreshness': {
            'fred':     freshness(fred_mtime),
            'cot':      freshness(cot_mtime),
            'yfinance': freshness(yf_mtime),
        },
    }


def write_status_json():
    """Compute analytics and write to /var/www/gold-tracker/data/status.json."""
    try:
        data = compute_analytics()
        if data is None:
            return
        os.makedirs(os.path.dirname(STATUS_JSON_PATH), exist_ok=True)
        with open(STATUS_JSON_PATH, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass  # Never let this break a data request


def serve(filename, fetcher_fn, force=False):
    """Generic cache-or-fetch handler."""
    if not force and is_fresh(filename):
        try:
            data = read_cache(filename)
            data['_cache'] = 'hit'
            return jsonify(data)
        except Exception:
            pass
    try:
        data = fetcher_fn()
        write_cache(filename, data)
        data['_cache'] = 'miss'
        write_status_json()  # Regenerate status.json after every fresh fetch
        return jsonify(data)
    except Exception as e:
        # Return cached data even if stale, rather than error
        if os.path.exists(cache_path(filename)):
            try:
                data = read_cache(filename)
                data['_cache'] = 'stale'
                data['_error'] = str(e)
                return jsonify(data)
            except Exception:
                pass
        return jsonify({'error': str(e), 'fetched_at': None}), 503


@app.route('/api/fred')
def fred_data():
    force = request.args.get('refresh') == '1'
    return serve('fred.json', fetch_fred, force=force)


@app.route('/api/cot')
def cot_data():
    force = request.args.get('refresh') == '1'
    return serve('cot.json', fetch_cot, force=force)


@app.route('/api/yfinance')
def yfinance_data():
    force = request.args.get('refresh') == '1'
    return serve('yfinance.json', fetch_yfinance, force=force)


@app.route('/api/status')
def status():
    files = {
        'fred': 'fred.json',
        'cot': 'cot.json',
        'yfinance': 'yfinance.json',
    }
    out = {}
    for key, fname in files.items():
        path = cache_path(fname)
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            out[key] = {
                'last_updated': datetime.utcfromtimestamp(mtime).isoformat() + 'Z',
                'fresh': is_fresh(fname),
                'age_hours': round((time.time() - mtime) / 3600, 1),
            }
        else:
            out[key] = {'last_updated': None, 'fresh': False, 'age_hours': None}
    return jsonify(out)


@app.route('/api/goldprice')
def gold_price():
    """Lightweight endpoint for the live gold price header stat.
    Reads from yfinance cache — fast, no external call needed."""
    try:
        yf_path = cache_path('yfinance.json')
        if not os.path.exists(yf_path):
            return jsonify({'error': 'No cache yet'}), 503

        with open(yf_path) as f:
            yf = json.load(f)

        futures = yf.get('gold_futures', [])
        if not isinstance(futures, list) or len(futures) < 2:
            return jsonify({'error': 'Insufficient data'}), 503

        current  = futures[-1]
        prev_day = futures[-2]
        price    = current['close']
        prev     = prev_day['close']
        day_chg  = price - prev
        day_pct  = day_chg / prev * 100

        # YoY: find ~252 trading days ago (1 year)
        yoy_idx   = max(0, len(futures) - 252)
        yoy_price = futures[yoy_idx]['close']
        yoy_pct   = (price - yoy_price) / yoy_price * 100

        return jsonify({
            'price':     round(price, 2),
            'day_chg':   round(day_chg, 2),
            'day_pct':   round(day_pct, 2),
            'yoy_pct':   round(yoy_pct, 2),
            'date':      current['date'],
            'as_of':     datetime.utcfromtimestamp(os.path.getmtime(yf_path)).isoformat() + 'Z',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 503


@app.route('/api/analytics')
def analytics_export():
    """Return the latest computed analytics as JSON.
    Also regenerates /data/status.json. Useful for forcing a refresh."""
    data = compute_analytics()
    if data is None:
        return jsonify({'error': 'Data not yet cached'}), 503
    write_status_json()
    return jsonify(data)


@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'time': datetime.utcnow().isoformat() + 'Z'})


SNAPSHOT_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gold Tracker — Market Data Snapshot</title>
<style>
  body {{ font-family: monospace; background: #0f1419; color: #e6edf3; padding: 24px; max-width: 800px; margin: 0 auto; }}
  h1   {{ color: #d29922; margin-bottom: 4px; }}
  h2   {{ color: #58a6ff; margin-top: 24px; border-bottom: 1px solid #2d3a4a; padding-bottom: 6px; }}
  table{{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
  td,th{{ padding: 8px 12px; border-bottom: 1px solid #2d3a4a; text-align: left; }}
  th   {{ color: #6e7681; font-size: 12px; text-transform: uppercase; }}
  .bull{{ color: #3fb950; }} .bear{{ color: #f85149; }} .neut{{ color: #8b949e; }}
  .price{{ font-size: 28px; font-weight: bold; color: #d29922; }}
  .meta {{ color: #6e7681; font-size: 12px; }}
</style>
</head>
<body>
<h1>Gold Tracker — Market Snapshot</h1>
<p class="meta">Generated: {generated} | Data sources: FRED, CFTC, Yahoo Finance</p>
<p class="meta"><a href="/" style="color:#58a6ff;">← Back to dashboard</a></p>

<h2>Gold Price</h2>
<table>
<tr><th>Metric</th><th>Value</th><th>As of</th></tr>
<tr><td>Spot / Front-month Futures (GC=F)</td><td class="price">${gold_price:,.0f}/oz</td><td>{gold_date}</td></tr>
<tr><td>Day Change</td><td class="{day_cls}">{day_sign}{day_chg:,.0f} ({day_sign}{day_pct:.1f}%)</td><td>vs prev close</td></tr>
<tr><td>YoY Change</td><td class="{yoy_cls}">{yoy_sign}{yoy_pct:.1f}%</td><td>~252 trading days</td></tr>
</table>

<h2>Real Rates (FRED)</h2>
<table>
<tr><th>Series</th><th>Value</th><th>Date</th></tr>
<tr><td>10Y TIPS Yield (Real Rate)</td><td>{tips_val:.2f}%</td><td>{tips_date}</td></tr>
<tr><td>USD Broad Trade-Weighted Index (DXY)</td><td>{dxy_val:.2f}</td><td>{dxy_date}</td></tr>
<tr><td>SOFR</td><td>{sofr_val:.2f}%</td><td>{sofr_date}</td></tr>
<tr><td>Rate Model Implied Gold Price</td><td>${model_price:,.0f}/oz</td><td>OLS: gold = {slope:.0f} × TIPS + {intercept:.0f}</td></tr>
<tr><td>Speculative Premium vs Model</td><td class="{prem_cls}">{prem_sign}{premium:.1f}%</td><td>actual vs model-implied</td></tr>
</table>

<h2>CFTC Positioning (Non-Commercial / Speculative)</h2>
<table>
<tr><th>Metric</th><th>Value</th><th>Date</th></tr>
<tr><td>Net Long Contracts</td><td>{cot_net:,}</td><td>{cot_date}</td></tr>
<tr><td>Net Long % of Open Interest</td><td>{cot_pct:.1f}%</td><td></td></tr>
<tr><td>Positioning Percentile (5-year history)</td><td class="{cot_cls}">{cot_percentile:.0f}th percentile</td><td></td></tr>
<tr><td>Open Interest</td><td>{cot_oi:,}</td><td></td></tr>
</table>

<h2>Market Ratios</h2>
<table>
<tr><th>Metric</th><th>Value</th><th>Signal</th></tr>
<tr><td>Gold / Silver Ratio (GC=F ÷ SI=F)</td><td>{gsr:.1f}</td><td class="{gsr_cls}">{gsr_signal}</td></tr>
<tr><td>GDX / GLD Ratio (Miners vs Physical)</td><td>{gdxgld:.4f}</td><td class="{gdxgld_cls}">{gdxgld_signal}</td></tr>
<tr><td>Gold Cobasis (Spot − Futures, $/oz)</td><td>{cobasis:+.2f}</td><td class="{cb_cls}">{cb_state}</td></tr>
</table>

<h2>Bull Market Scorecard</h2>
<p><strong>Composite: <span class="{score_cls}">{bullish}/9 Bullish Signals</span></strong></p>
<table>
<tr><th>#</th><th>Signal</th><th>Value</th><th>Status</th></tr>
{scorecard_rows}
</table>

<h2>Data Freshness</h2>
<table>
<tr><th>Source</th><th>Last Updated (UTC)</th><th>Status</th></tr>
{freshness_rows}
</table>
</body>
</html>"""


def build_snapshot():
    """Compute all signals from cached data and return rendered HTML."""
    import json, os, time
    from datetime import datetime

    CACHE_DIR = '/opt/gold-tracker/api/cache'

    def load(name):
        p = os.path.join(CACHE_DIR, name)
        if not os.path.exists(p):
            return None, None
        with open(p) as f:
            return json.load(f), os.path.getmtime(p)

    fred, fred_mtime = load('fred.json')
    cot_data, cot_mtime   = load('cot.json')
    yf, yf_mtime   = load('yfinance.json')

    if not fred or not yf:
        return "<p>Data not yet cached. Try again shortly.</p>"

    # ---- Gold price ----
    gold_series = fred.get('gold', [])
    gf = sorted([x for x in (gold_series if isinstance(gold_series, list) else []) if x.get('value')], key=lambda x: x['date'])
    gold_price = gf[-1]['value'] if gf else 0
    gold_date  = gf[-1]['date']  if gf else '—'
    prev_price = gf[-2]['value'] if len(gf) >= 2 else gold_price
    day_chg = gold_price - prev_price
    day_pct = day_chg / prev_price * 100 if prev_price else 0
    yoy_price = gf[max(0, len(gf)-252)]['value'] if gf else gold_price
    yoy_pct = (gold_price - yoy_price) / yoy_price * 100 if yoy_price else 0

    # ---- TIPS, DXY, SOFR ----
    def last_valid(series):
        arr = [x for x in (series if isinstance(series, list) else []) if x.get('value') is not None]
        return arr[-1] if arr else {'value': 0, 'date': '—'}

    tips = last_valid(fred.get('tips', []))
    dxy  = last_valid(fred.get('dxy',  []))
    sofr = last_valid(fred.get('sofr', []))

    # ---- OLS regression: gold = slope * TIPS + intercept ----
    gold_map = {x['date']: x['value'] for x in gf}
    tips_series = [x for x in (fred.get('tips', []) or []) if x.get('value') is not None]
    pairs = [(t['value'], gold_map[t['date']]) for t in tips_series if t['date'] in gold_map]
    slope = intercept = r2 = 0
    model_price = premium = 0
    if len(pairs) >= 2:
        xs = [p[0] for p in pairs]; ys = [p[1] for p in pairs]
        n = len(xs); sx = sum(xs); sy = sum(ys)
        sxy = sum(x*y for x,y in zip(xs,ys)); sxx = sum(x*x for x in xs)
        denom = n*sxx - sx*sx
        if denom:
            slope = (n*sxy - sx*sy) / denom
            intercept = (sy - slope*sx) / n
        model_price = slope * tips['value'] + intercept
        premium = (gold_price - model_price) / model_price * 100 if model_price else 0

    # ---- COT ----
    cot_rows = (cot_data or {}).get('data', [])
    cot_latest = cot_rows[-1] if cot_rows else {}
    cot_net  = cot_latest.get('net_long', 0)
    cot_pct_oi = cot_latest.get('net_long_pct', 0)
    cot_oi   = cot_latest.get('open_interest', 0)
    cot_date_val = cot_latest.get('date', '—')
    # Percentile vs 5-year history
    five_yr_ago = cot_rows[max(0, len(cot_rows)-260)]['net_long'] if cot_rows else 0
    window = [r['net_long'] for r in cot_rows[-260:]] if cot_rows else [cot_net]
    window_sorted = sorted(window)
    cot_percentile = sum(1 for v in window_sorted if v <= cot_net) / len(window_sorted) * 100

    # ---- Ratios ----
    def yf_close(key):
        arr = yf.get(key, [])
        return (arr[-1] if isinstance(arr, list) and arr else {}).get('close', 0)

    gld_p  = yf_close('gld');  gdx_p = yf_close('gdx')
    gf_p   = yf_close('gold_futures'); sf_p = yf_close('silver_futures')
    GLD_OZ = 0.09334
    spot_price = gld_p / GLD_OZ if gld_p else 0
    cobasis = spot_price - gf_p if spot_price and gf_p else 0
    gsr = gf_p / sf_p if sf_p else 0
    gdxgld = gdx_p / gld_p if gld_p else 0

    # 3Y average GDX/GLD
    gld_arr = yf.get('gld', []); gdx_arr = yf.get('gdx', [])
    if isinstance(gld_arr, list) and isinstance(gdx_arr, list) and len(gld_arr) >= 756:
        gld_map_yf = {x['date']: x['close'] for x in gld_arr}
        ratios_3y = [g['close']/gld_map_yf[g['date']] for g in gdx_arr[-756:] if g['date'] in gld_map_yf]
        gdxgld_3yavg = sum(ratios_3y)/len(ratios_3y) if ratios_3y else gdxgld
    else:
        gdxgld_3yavg = gdxgld

    # ---- Signals ----
    # TIPS 3-month change
    tips_series_sorted = sorted(tips_series, key=lambda x: x['date'])
    tips_3m = tips_series_sorted[-1]['value'] - tips_series_sorted[max(0,len(tips_series_sorted)-63)]['value'] if tips_series_sorted else 0
    # DXY 90-day change
    dxy_sorted = sorted([x for x in (fred.get('dxy',[]) or []) if x.get('value') is not None], key=lambda x: x['date'])
    dxy_90d = dxy_sorted[-1]['value'] - dxy_sorted[max(0,len(dxy_sorted)-90)]['value'] if dxy_sorted else 0

    def sig(v, bull_test, bear_test, bull_label='Bullish', bear_label='Cautionary', neut_label='Neutral'):
        if bull_test(v): return 'bull', bull_label
        if bear_test(v): return 'bear', bear_label
        return 'neut', neut_label

    signals = [
        ('Real Rate Trend (3M TIPS Δ)',      f'{tips_3m:+.2f}%',             *sig(tips_3m,      lambda v: v < -0.1,  lambda v: v > 0.1)),
        ('Rate Model Premium',               f'{premium:+.1f}%',              *sig(premium,      lambda v: v < 10,    lambda v: v > 20)),
        ('DXY Trend (90d)',                  f'{dxy_90d:+.2f}',               *sig(dxy_90d,      lambda v: v < -1,    lambda v: v > 1)),
        ('COT Positioning Percentile',       f'{cot_percentile:.0f}th pct',   *sig(cot_percentile, lambda v: v < 20, lambda v: v > 80)),
        ('Gold Cobasis',                     f'${cobasis:+.2f}/oz',           *sig(cobasis,      lambda v: v > 0,     lambda v: v < -2, 'Backwardation', 'Deep Contango')),
        ('Miner Confirmation (GDX/GLD)',     f'{gdxgld:.4f}',                 *sig(gdxgld - gdxgld_3yavg, lambda v: v > 0, lambda v: v < -0.01, 'Above 3Y avg', 'Below 3Y avg')),
        ('Gold/Silver Ratio',                f'{gsr:.1f}',                    *sig(gsr,          lambda v: v < 70,    lambda v: v > 80, 'Below 70 (normal)', 'Above 80 (late-cycle)')),
        ('CB Demand (latest quarter)',       f'{cot_net:,} net longs',        'neut', 'See WGC data'),
        ('ETF Flow Trend',                   '—',                             'neut', 'See dashboard'),
    ]

    bullish = sum(1 for s in signals if s[2] == 'bull')
    score_cls = 'bull' if bullish >= 6 else 'bear' if bullish <= 3 else 'neut'

    scorecard_rows = '\n'.join(
        f'<tr><td>{i+1}</td><td>{s[0]}</td><td>{s[1]}</td><td class="{s[2]}">{s[3]}</td></tr>'
        for i, s in enumerate(signals)
    )

    def fresh_row(label, mtime):
        if not mtime: return f'<tr><td>{label}</td><td>—</td><td class="bear">No cache</td></tr>'
        dt = datetime.utcfromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
        age_h = (time.time() - mtime) / 3600
        cls = 'bull' if age_h < 25 else 'neut' if age_h < 170 else 'bear'
        return f'<tr><td>{label}</td><td>{dt}</td><td class="{cls}">{age_h:.1f}h ago</td></tr>'

    freshness_rows = '\n'.join([
        fresh_row('FRED (TIPS, Gold, DXY, SOFR)', fred_mtime),
        fresh_row('CFTC COT (Speculative Positioning)', cot_mtime),
        fresh_row('Yahoo Finance (GDX, GLD, SLV, GC=F)', yf_mtime),
    ])

    return SNAPSHOT_TEMPLATE.format(
        generated=datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        gold_price=gold_price, gold_date=gold_date,
        day_sign='+' if day_chg >= 0 else '', day_chg=day_chg, day_pct=day_pct,
        day_cls='bull' if day_chg >= 0 else 'bear',
        yoy_sign='+' if yoy_pct >= 0 else '', yoy_pct=yoy_pct,
        yoy_cls='bull' if yoy_pct >= 0 else 'bear',
        tips_val=tips['value'], tips_date=tips['date'],
        dxy_val=dxy['value'],   dxy_date=dxy['date'],
        sofr_val=sofr['value'], sofr_date=sofr.get('date','—'),
        slope=slope, intercept=intercept, model_price=model_price,
        prem_sign='+' if premium >= 0 else '', premium=premium,
        prem_cls='bear' if premium > 20 else 'bull' if premium < 10 else 'neut',
        cot_net=cot_net, cot_pct=cot_pct_oi, cot_oi=cot_oi, cot_date=cot_date_val,
        cot_percentile=cot_percentile,
        cot_cls='bear' if cot_percentile > 80 else 'bull' if cot_percentile < 20 else 'neut',
        gsr=gsr, gsr_cls='bear' if gsr > 80 else 'bull' if gsr < 70 else 'neut',
        gsr_signal='Late-cycle (>80)' if gsr > 80 else 'Normal (<70)' if gsr < 70 else 'Elevated (70-80)',
        gdxgld=gdxgld, gdxgld_3yavg=gdxgld_3yavg,
        gdxgld_cls='bull' if gdxgld > gdxgld_3yavg else 'bear',
        gdxgld_signal=f'Above 3Y avg ({gdxgld_3yavg:.4f})' if gdxgld > gdxgld_3yavg else f'Below 3Y avg ({gdxgld_3yavg:.4f})',
        cobasis=cobasis,
        cb_cls='bull' if cobasis > 0 else 'bear' if cobasis < -2 else 'neut',
        cb_state='Backwardation (bullish)' if cobasis > 0 else 'Deep Contango' if cobasis < -2 else 'Contango',
        bullish=bullish, score_cls=score_cls,
        scorecard_rows=scorecard_rows,
        freshness_rows=freshness_rows,
    )


@app.route('/snapshot')
def snapshot():
    """Plain-HTML snapshot readable by AI crawlers, curl, screen readers."""
    from flask import Response
    html = build_snapshot()
    return Response(html, mimetype='text/html')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)
