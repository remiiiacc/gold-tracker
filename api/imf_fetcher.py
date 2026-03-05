"""
IMF IRFCL gold reserve holdings fetcher.

Fetches monthly gold reserve data (IRFCLDT1_IRFCL56_USD indicator) for key central
banks from the IMF International Reserves and Foreign Currency Liquidity (IRFCL)
dataset via the new IMF SDMX 3.0 REST API (api.imf.org).

Returns holdings in tonnes, computed by dividing USD values by the end-of-quarter
gold price.

Endpoint:
  https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL/+/{countries}.IRFCLDT1_IRFCL56_USD.S1X+S1XS1311.M
  - Dataset:    IRFCL (International Reserves and Foreign Currency Liquidity)
  - Indicator:  IRFCLDT1_IRFCL56_USD (gold reserves, approximate market value in USD)
  - Sectors:    S1X (monetary authorities) + S1XS1311 (monetary auth + central govt)
  - Frequency:  M (monthly)
  - Country codes: 3-letter ISO codes
  - Values are in absolute USD
  - Only end-of-quarter months (03=Q1, 06=Q2, 09=Q3, 12=Q4) are kept

Note: Old API (dataservices.imf.org) was retired November 2025.
      New API is at api.imf.org with SDMX 3.0 format and different indicator codes.
"""

import re
import requests
from datetime import datetime

# Countries to fetch — 3-letter ISO codes as used by IMF IRFCL new API
# Mapped to (2-letter code used in frontend, display name)
IMF_COUNTRIES = {
    'CHN': ('CN', 'China'),
    'IND': ('IN', 'India'),
    'RUS': ('RU', 'Russia'),
    'TUR': ('TR', 'Turkey'),
    'POL': ('PL', 'Poland'),
    'SGP': ('SG', 'Singapore'),
    'CZE': ('CZ', 'Czech Republic'),
    'QAT': ('QA', 'Qatar'),
    'KAZ': ('KZ', 'Kazakhstan'),
    'UZB': ('UZ', 'Uzbekistan'),
    'BRA': ('BR', 'Brazil'),
    'DEU': ('DE', 'Germany'),
    'FRA': ('FR', 'France'),
    'ITA': ('IT', 'Italy'),
    'JPN': ('JP', 'Japan'),
    'NLD': ('NL', 'Netherlands'),
    'CHE': ('CH', 'Switzerland'),
    'GBR': ('GB', 'United Kingdom'),
    'SAU': ('SA', 'Saudi Arabia'),
    'AUS': ('AU', 'Australia'),
}
# Note: USA is excluded — the US Treasury reports IRFCL gold at historical book
# value ($42.22/oz), not market value, making tonnage conversion incorrect (~174t
# instead of the actual 8,134t). The frontend uses WGC data for US holdings.

TROY_OZ_PER_TONNE = 32150.7

# New IMF SDMX 3.0 API base URL
IMF_BASE = 'https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IRFCL/+'

# Gold indicator in the new IRFCL API (approximate market value of gold in USD)
GOLD_INDICATOR = 'IRFCLDT1_IRFCL56_USD'

# Sectors to query: S1X (monetary authorities) and S1XS1311 (monetary auth + central govt)
# Some countries report under S1X, others under S1XS1311 (values are equivalent)
GOLD_SECTORS = 'S1X+S1XS1311'

# End-of-quarter months → quarter label
EOQ_MONTH_TO_Q = {'03': 'Q1', '06': 'Q2', '09': 'Q3', '12': 'Q4'}

# Approximate end-of-quarter gold prices (USD/oz) for USD → tonnes conversion
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


def imf_period_to_quarter(period):
    """
    Convert IMF SDMX 3.0 monthly period 'YYYY-Mxx' or 'YYYY-MM' to quarter key 'YYYY-QN'.
    Returns None for non-end-of-quarter months.
    """
    if not period:
        return None
    s = str(period).strip()
    # Handle 'YYYY-Mxx' format (SDMX 3.0 monthly)
    m = re.match(r'^(\d{4})-M(\d{1,2})$', s)
    if m:
        year, month = m.group(1), m.group(2).zfill(2)
        q = EOQ_MONTH_TO_Q.get(month)
        return f'{year}-{q}' if q else None
    # Handle 'YYYY-MM' format (fallback)
    m = re.match(r'^(\d{4})-(\d{2})$', s)
    if m:
        year, month = m.group(1), m.group(2)
        q = EOQ_MONTH_TO_Q.get(month)
        return f'{year}-{q}' if q else None
    return None


def fetch_imf_cb_holdings(gold_price_by_quarter=None):
    """
    Fetch quarterly gold reserve holdings for key central banks from IMF IRFCL.

    Uses IRFCL dataset + IRFCLDT1_IRFCL56_USD indicator (gold specifically, in
    absolute USD) reported by the monetary authority. Monthly data is filtered to
    end-of-quarter months only to produce quarterly snapshots.

    Args:
        gold_price_by_quarter: dict mapping 'YYYY-QN' -> avg USD/oz price (from
                               quarterly.json). Used to convert absolute USD to tonnes.
                               Falls back to FALLBACK_PRICES then 3000.0 if unknown.

    Returns dict:
    {
        'countries': {
            'CN': {'name': 'China', 'quarters': {'2020-Q1': 1948.3, ...}},
            ...
        },
        'total_by_quarter': {'2020-Q1': 35241.0, ...},
        'latest_quarter': '2025-Q3',
        'fetched_at': '2026-03-05T06:00:00Z',
        'warnings': []
    }
    Raises ValueError if the API is unreachable or returns no usable data.
    """
    warnings = []

    countries_str = '+'.join(IMF_COUNTRIES.keys())
    url = f'{IMF_BASE}/{countries_str}.{GOLD_INDICATOR}.{GOLD_SECTORS}.M'

    params = {
        'dimensionAtObservation': 'TIME_PERIOD',
        'attributes': 'dsd',
        'measures': 'all',
        'c[TIME_PERIOD]': 'ge:2015-M01+le:2025-M12',
    }

    try:
        resp = requests.get(url, params=params, timeout=45, headers={'Accept': 'application/json'})
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        raise ValueError(f'IMF IRFCL API unreachable: {e}')

    # Parse SDMX JSON 2.0 structure
    try:
        data_block = raw.get('data', {})
        datasets = data_block.get('dataSets', [])
        structures = data_block.get('structures', [])
        if not datasets or not structures:
            raise ValueError('Empty dataSets or structures in response')

        dataset = datasets[0]
        structure = structures[0]
        series_map = dataset.get('series', {})

        # Decode dimension value arrays from structure
        series_dims = structure.get('dimensions', {}).get('series', [])
        obs_dims = structure.get('dimensions', {}).get('observation', [])

        country_vals   = [v.get('id') for v in (series_dims[0].get('values', []) if len(series_dims) > 0 else [])]
        indicator_vals = [v.get('id') for v in (series_dims[1].get('values', []) if len(series_dims) > 1 else [])]
        sector_vals    = [v.get('id') for v in (series_dims[2].get('values', []) if len(series_dims) > 2 else [])]
        # TIME_PERIOD uses 'value' key (not 'id')
        time_vals      = [v.get('value') for v in (obs_dims[0].get('values', []) if obs_dims else [])]

    except (KeyError, TypeError, IndexError) as e:
        raise ValueError(f'Unexpected IMF API response structure: {e}')

    def get_price(quarter):
        if gold_price_by_quarter and quarter in gold_price_by_quarter:
            p = gold_price_by_quarter[quarter]
            if p and float(p) > 0:
                return float(p)
        return FALLBACK_PRICES.get(quarter, 3000.0)

    def usd_to_tonnes(usd_absolute, quarter):
        """Convert absolute USD value to tonnes using gold price."""
        price = get_price(quarter)
        return float(usd_absolute) / (price * TROY_OZ_PER_TONNE)

    # Per-country data: country_3letter -> {quarter -> tonnes}
    # Prefer S1X sector over S1XS1311 when both exist (values are equal)
    country_quarters = {}  # country_3letter -> {quarter -> (tonnes, sector)}

    for series_key, series_val in series_map.items():
        try:
            indices = series_key.split(':')
            ci = int(indices[0])
            si = int(indices[2])  # sector index
        except (ValueError, IndexError):
            continue

        country_3 = country_vals[ci] if ci < len(country_vals) else None
        sector    = sector_vals[si] if si < len(sector_vals) else None

        if country_3 not in IMF_COUNTRIES:
            continue

        observations = series_val.get('observations', {})
        for ti_str, obs_val in observations.items():
            try:
                ti = int(ti_str)
                period = time_vals[ti] if ti < len(time_vals) else None
                quarter = imf_period_to_quarter(period)
                if quarter is None:
                    continue
                value = obs_val[0] if isinstance(obs_val, list) else obs_val
                if value is None:
                    continue
                usd = float(value)
                if usd <= 0:
                    continue
                tonnes = round(usd_to_tonnes(usd, quarter), 1)

                if country_3 not in country_quarters:
                    country_quarters[country_3] = {}

                existing = country_quarters[country_3].get(quarter)
                # Prefer S1X over S1XS1311
                if existing is None or (sector == 'S1X' and existing[1] != 'S1X'):
                    country_quarters[country_3][quarter] = (tonnes, sector)
            except (ValueError, TypeError, IndexError):
                continue

    if not country_quarters:
        raise ValueError(
            'IMF IRFCL API returned no usable gold data. '
            f'Diagnostic URL: GET {url}'
        )

    # Build result in the expected format (using 2-letter country codes for frontend)
    result_countries = {}
    all_quarters_seen = set()

    for country_3, qmap in country_quarters.items():
        code2, name = IMF_COUNTRIES[country_3]
        if not qmap:
            warnings.append(f'No data for {country_3} ({name})')
            continue
        quarters = {q: t for q, (t, _) in qmap.items()}
        result_countries[code2] = {'name': name, 'quarters': quarters}
        all_quarters_seen.update(quarters.keys())

    # Countries with no data
    for country_3, (code2, name) in IMF_COUNTRIES.items():
        if country_3 not in country_quarters:
            warnings.append(f'No data returned for {country_3} ({name})')

    if not result_countries:
        raise ValueError('No usable country data after parsing IMF IRFCL response.')

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


if __name__ == '__main__':
    import json, sys

    print('Testing IMF IRFCL gold data fetch via new api.imf.org SDMX 3.0 API...')
    print()

    # Diagnostic: test single country (Poland) first
    diag_countries = 'POL'
    diag_url = f'{IMF_BASE}/{diag_countries}.{GOLD_INDICATOR}.{GOLD_SECTORS}.M'
    diag_params = {
        'dimensionAtObservation': 'TIME_PERIOD',
        'attributes': 'dsd',
        'measures': 'all',
        'c[TIME_PERIOD]': 'ge:2024-M01+le:2024-M12',
    }
    print(f'Diagnostic URL: {diag_url}')
    print(f'Params: {diag_params}')
    try:
        r = requests.get(diag_url, params=diag_params, timeout=30, headers={'Accept': 'application/json'})
        print(f'HTTP {r.status_code}')
        raw = r.json()
        ds = raw.get('data', {}).get('dataSets', [])
        series = ds[0].get('series', {}) if ds else {}
        obs_dims = raw.get('data', {}).get('structures', [{}])[0].get('dimensions', {}).get('observation', [])
        time_vals = [v.get('value') for v in (obs_dims[0].get('values', []) if obs_dims else [])]
        print(f'Series count: {len(series)}')
        print(f'Time periods: {time_vals[:4]}...')
        for sk, sv in list(series.items())[:2]:
            obs = sv.get('observations', {})
            print(f'  Series {sk}: {len(obs)} observations')
            for ti_str, val in list(obs.items())[:3]:
                ti = int(ti_str)
                period = time_vals[ti] if ti < len(time_vals) else ti_str
                print(f'    {period}: {val[0] if isinstance(val, list) else val}')
    except Exception as e:
        print(f'DIAGNOSTIC FAILED: {e}')
        sys.exit(1)

    print()
    print('Fetching all countries...')
    try:
        result = fetch_imf_cb_holdings()
        print(f"Countries fetched: {len(result['countries'])}")
        print(f"Latest quarter: {result['latest_quarter']}")
        for code, info in list(result['countries'].items())[:5]:
            qs = sorted(info['quarters'].keys())
            print(f"  {code} ({info['name']}): {len(qs)} quarters, "
                  f"latest={qs[-1]} {info['quarters'][qs[-1]]}t")
        if result['warnings']:
            print(f"Warnings: {result['warnings']}")
    except ValueError as e:
        print(f'FAILED: {e}')
