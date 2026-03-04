"""
Gold Tracker Data Fetcher
Fetches from FRED, CFTC, and yfinance. All API keys stay server-side.
"""
import requests
import json
import os
from datetime import datetime, timedelta

FRED_API_KEY = os.environ.get('FRED_API_KEY', '2b85c4ec0784560d342a8159fa277e7b')
FRED_BASE = 'https://api.stlouisfed.org/fred/series/observations'

START_DATE_10Y = (datetime.now() - timedelta(days=365 * 10)).strftime('%Y-%m-%d')
START_DATE_20Y = (datetime.now() - timedelta(days=365 * 20)).strftime('%Y-%m-%d')


def fetch_fred_series(series_id, start_date=None):
    if not start_date:
        start_date = START_DATE_20Y
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'sort_order': 'asc',
    }
    r = requests.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    observations = r.json().get('observations', [])
    return [
        {'date': o['date'], 'value': float(o['value']) if o['value'] not in ('.', '') else None}
        for o in observations
    ]


def fetch_fred_series_monthly(series_id, start_date='2006-01-01'):
    """Fetch a FRED series as monthly averages (frequency=m, aggregation_method=avg)."""
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'frequency': 'm',
        'aggregation_method': 'avg',
        'sort_order': 'asc',
    }
    r = requests.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    observations = r.json().get('observations', [])
    return [
        {'date': o['date'], 'value': float(o['value']) if o['value'] not in ('.', '') else None}
        for o in observations
    ]


def fetch_fred():
    """Fetch all FRED series. Returns dict with keys: tips, gold, dxy, sofr,
    tips_monthly, gold_monthly.
    Gold price falls back to yfinance GC=F when FRED gold series is unavailable.
    """
    series_map = {
        'tips': 'DFII10',
        'dxy':  'DTWEXBGS',
        'sofr': 'SOFR',
    }
    result = {}
    for key, sid in series_map.items():
        try:
            result[key] = fetch_fred_series(sid)
        except Exception as e:
            result[key] = {'error': str(e), 'data': []}

    # Gold price: use yfinance GC=F (front-month gold futures, USD/oz)
    # FRED gold series (GOLDAMGBD228NLBW) is not available via this API key
    try:
        import yfinance as yf
        ticker = yf.Ticker('GC=F')
        hist = ticker.history(period='20y', interval='1d')
        if not hist.empty:
            result['gold'] = [
                {'date': str(idx.date()), 'value': round(float(row['Close']), 2)}
                for idx, row in hist.iterrows()
            ]
        else:
            result['gold'] = {'error': 'GC=F returned empty data', 'data': []}
    except Exception as e:
        result['gold'] = {'error': str(e), 'data': []}

    # Monthly TIPS — for dual-regime regression scatter (reduces dot overlap)
    try:
        result['tips_monthly'] = fetch_fred_series_monthly('DFII10', start_date='2006-01-01')
    except Exception as e:
        result['tips_monthly'] = []

    # Monthly gold — GC=F resampled to monthly start averages
    try:
        import yfinance as yf
        import pandas as pd
        ticker = yf.Ticker('GC=F')
        hist = ticker.history(period='20y', interval='1d')
        if not hist.empty:
            hist.index = hist.index.tz_localize(None) if hist.index.tz else hist.index
            monthly = hist['Close'].resample('MS').mean().dropna()
            result['gold_monthly'] = [
                {'date': str(idx.date()), 'value': round(float(v), 2)}
                for idx, v in monthly.items()
                if str(idx.date()) >= '2006-01-01'
            ]
        else:
            result['gold_monthly'] = []
    except Exception as e:
        result['gold_monthly'] = []

    result['fetched_at'] = datetime.utcnow().isoformat() + 'Z'
    return result


def fetch_cot():
    """Fetch CFTC COT gold data. No API key required."""
    url = 'https://publicreporting.cftc.gov/resource/jun7-fc8e.json'
    params = {
        '$where': "market_and_exchange_names='GOLD - COMMODITY EXCHANGE INC.'",
        '$limit': 300,
        '$order': 'report_date_as_yyyy_mm_dd DESC',
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        rows = r.json()
        processed = []
        for row in rows:
            try:
                # Legacy COT: use non-commercial positions as speculative proxy
                mm_long = int(row.get('noncomm_positions_long_all', 0) or 0)
                mm_short = int(row.get('noncomm_positions_short_all', 0) or 0)
                oi = int(row.get('open_interest_all', 1) or 1) or 1
                net_long = mm_long - mm_short
                net_long_pct = round(net_long / oi * 100, 2)
                # Normalize date: strip ISO timestamp, keep only YYYY-MM-DD
                raw_date = row.get('report_date_as_yyyy_mm_dd', '')
                date_str = raw_date[:10] if raw_date else ''
                if not date_str:
                    continue
                processed.append({
                    'date': date_str,
                    'mm_long': mm_long,
                    'mm_short': mm_short,
                    'net_long': net_long,
                    'open_interest': oi,
                    'net_long_pct': net_long_pct,
                })
            except (ValueError, KeyError, TypeError):
                continue
        processed.sort(key=lambda x: x['date'])
        return {'data': processed, 'fetched_at': datetime.utcnow().isoformat() + 'Z'}
    except Exception as e:
        return {'error': str(e), 'data': [], 'fetched_at': datetime.utcnow().isoformat() + 'Z'}


def fetch_yfinance():
    """Fetch market data via yfinance. Falls back gracefully on failure."""
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        return {'error': 'yfinance not installed', 'data': {}, 'fetched_at': datetime.utcnow().isoformat() + 'Z'}

    tickers = {
        'gold_futures': 'GC=F',
        'gdx': 'GDX',
        'gld': 'GLD',
        'slv': 'SLV',
        'silver_futures': 'SI=F',
    }
    result = {}
    for key, ticker in tickers.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period='10y', interval='1d')
            if hist.empty:
                result[key] = {'error': f'No data returned for {ticker}', 'data': []}
                continue
            result[key] = [
                {
                    'date': str(idx.date()),
                    'close': round(float(row['Close']), 4),
                }
                for idx, row in hist.iterrows()
            ]
        except Exception as e:
            result[key] = {'error': str(e), 'data': []}
    result['fetched_at'] = datetime.utcnow().isoformat() + 'Z'
    return result


if __name__ == '__main__':
    print('Fetching FRED...')
    fred = fetch_fred()
    print(f"  tips: {len(fred.get('tips', []))} obs")
    print(f"  gold: {len(fred.get('gold', []))} obs")
    print(f"  dxy:  {len(fred.get('dxy', []))} obs")
    print('Fetching COT...')
    cot = fetch_cot()
    print(f"  cot rows: {len(cot.get('data', []))}")
    print('Fetching yfinance...')
    yf_data = fetch_yfinance()
    for k in ['gold_futures', 'gdx', 'gld', 'slv', 'silver_futures']:
        v = yf_data.get(k, {})
        if isinstance(v, list):
            print(f"  {k}: {len(v)} rows")
        else:
            print(f"  {k}: {v}")
