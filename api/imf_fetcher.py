"""
IMF IFS gold reserve holdings fetcher.

Fetches quarterly gold reserve data (RAFA_USD series) for key central banks
from the IMF International Financial Statistics API. Returns holdings in tonnes,
computed by dividing USD values by the quarterly average gold price.

Endpoint: https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/Q.{countries}.RAFA_USD
- No authentication required
- Returns JSON with quarterly observations per country
- Values are in USD millions
- Lags by approximately one quarter
"""

import re
import json
import requests
from datetime import datetime

# Countries to fetch — ISO 2-letter codes as used by IMF IFS
IMF_COUNTRIES = {
    'CN': 'China',
    'IN': 'India',
    'RU': 'Russia',
    'TR': 'Turkey',
    'PL': 'Poland',
    'SG': 'Singapore',
    'CZ': 'Czech Republic',
    'QA': 'Qatar',
    'KZ': 'Kazakhstan',
    'UZ': 'Uzbekistan',
    'BR': 'Brazil',
    'DE': 'Germany',
    'US': 'United States',
    'FR': 'France',
    'IT': 'Italy',
    'JP': 'Japan',
    'NL': 'Netherlands',
    'CH': 'Switzerland',
    'GB': 'United Kingdom',
    'SA': 'Saudi Arabia',
    'AU': 'Australia',
}

TROY_OZ_PER_TONNE = 32150.7
IMF_IFS_BASE = 'https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS'

# Approximate quarterly avg gold prices (USD/oz) used for USD → tonnes conversion
# when WGC quarterly.json data is unavailable for a given quarter.
FALLBACK_PRICES = {
    '2015-Q1': 1218, '2015-Q2': 1193, '2015-Q3': 1124, '2015-Q4': 1104,
    '2016-Q1': 1183, '2016-Q2': 1259, '2016-Q3': 1335, '2016-Q4': 1222,
    '2017-Q1': 1220, '2017-Q2': 1257, '2017-Q3': 1278, '2017-Q4': 1276,
    '2018-Q1': 1329, '2018-Q2': 1306, '2018-Q3': 1213, '2018-Q4': 1227,
    '2019-Q1': 1304, '2019-Q2': 1310, '2019-Q3': 1472, '2019-Q4': 1481,
    '2020-Q1': 1583, '2020-Q2': 1711, '2020-Q3': 1909, '2020-Q4': 1874,
    '2021-Q1': 1794, '2021-Q2': 1816, '2021-Q3': 1790, '2021-Q4': 1795,
    '2022-Q1': 1877, '2022-Q2': 1871, '2022-Q3': 1729, '2022-Q4': 1726,
    '2023-Q1': 1890, '2023-Q2': 1976, '2023-Q3': 1929, '2023-Q4': 1972,
    '2024-Q1': 2070, '2024-Q2': 2338, '2024-Q3': 2474, '2024-Q4': 2663,
    '2025-Q1': 2860, '2025-Q2': 3280, '2025-Q3': 3457, '2025-Q4': 4135,
}


def normalize_quarter(imf_period):
    """Convert IMF period format '2024-Q4' or '2024Q4' to 'YYYY-QN'."""
    s = str(imf_period).strip()
    m = re.match(r'^(\d{4})-?Q([1-4])$', s)
    if m:
        return f'{m.group(1)}-Q{m.group(2)}'
    return None


def fetch_imf_cb_holdings(gold_price_by_quarter=None):
    """
    Fetch quarterly gold reserve holdings for key central banks from IMF IFS.

    Args:
        gold_price_by_quarter: dict mapping 'YYYY-QN' -> avg USD/oz price (from quarterly.json).
                               Used to convert USD values to tonnes.
                               Falls back to FALLBACK_PRICES then 3000.0 if unknown.

    Returns dict with structure:
    {
        'countries': {
            'CN': {
                'name': 'China',
                'quarters': {'2020-Q1': 1948.3, ...}  # tonnes
            },
            ...
        },
        'total_by_quarter': {'2020-Q1': 35241.0, ...},
        'latest_quarter': '2024-Q4',
        'fetched_at': '2026-03-05T06:00:00Z',
        'warnings': []
    }
    Raises ValueError if the API is unreachable.
    """
    warnings = []
    countries_str = '+'.join(IMF_COUNTRIES.keys())
    url = f'{IMF_IFS_BASE}/Q.{countries_str}.RAFA_USD'

    try:
        resp = requests.get(url, timeout=30, headers={'Accept': 'application/json'})
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        raise ValueError(f'IMF IFS API unreachable: {e}')

    # Navigate JSON structure: CompactData -> DataSet -> Series
    try:
        dataset = raw['CompactData']['DataSet']
        series_list = dataset.get('Series', [])
        if isinstance(series_list, dict):
            series_list = [series_list]  # single series returned as dict, not list
    except (KeyError, TypeError) as e:
        raise ValueError(f'Unexpected IMF API response structure: {e}')

    def get_price(quarter):
        if gold_price_by_quarter and quarter in gold_price_by_quarter:
            p = gold_price_by_quarter[quarter]
            if p and float(p) > 0:
                return float(p)
        return FALLBACK_PRICES.get(quarter, 3000.0)

    def usd_millions_to_tonnes(usd_millions, quarter):
        price = get_price(quarter)
        return (float(usd_millions) * 1_000_000) / (price * TROY_OZ_PER_TONNE)

    result_countries = {}
    all_quarters_seen = set()

    for series in series_list:
        ref_area = series.get('@REF_AREA', '')
        if ref_area not in IMF_COUNTRIES:
            continue

        obs_list = series.get('Obs', [])
        if isinstance(obs_list, dict):
            obs_list = [obs_list]

        quarters = {}
        for obs in obs_list:
            period = normalize_quarter(obs.get('@TIME_PERIOD', ''))
            value  = obs.get('@OBS_VALUE')
            if period is None or value is None:
                continue
            try:
                usd = float(value)
                if usd <= 0:
                    continue
                quarters[period] = round(usd_millions_to_tonnes(usd, period), 1)
                all_quarters_seen.add(period)
            except (ValueError, TypeError):
                continue

        if not quarters:
            warnings.append(f'No data returned for {ref_area}')
            continue

        result_countries[ref_area] = {
            'name':     IMF_COUNTRIES[ref_area],
            'quarters': quarters,
        }

    if not result_countries:
        raise ValueError(
            'IMF API returned no usable country data. '
            'Check network access and API endpoint.'
        )

    # Compute total holdings by quarter (sum across all fetched countries)
    all_quarters_sorted = sorted(all_quarters_seen)
    total_by_quarter = {}
    for q in all_quarters_sorted:
        total = sum(c['quarters'].get(q, 0) for c in result_countries.values())
        if total > 0:
            total_by_quarter[q] = round(total, 1)

    latest_quarter = all_quarters_sorted[-1] if all_quarters_sorted else None

    return {
        'countries':        result_countries,
        'total_by_quarter': total_by_quarter,
        'latest_quarter':   latest_quarter,
        'fetched_at':       datetime.utcnow().isoformat() + 'Z',
        'warnings':         warnings,
    }
