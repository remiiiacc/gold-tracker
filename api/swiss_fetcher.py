"""
Swiss Federal Customs gold trade data fetcher.
Downloads gold imports by country from the FOCBS/BAZG dataset published on I14Y.
Tariff heading 7108.12 (gold bars, non-monetary), monthly granularity, 2021-present.

The I14Y distributions page (https://www.i14y.admin.ch/en/catalog/datasets/BAZG_GOLD_LAND)
is an Angular SPA — CSV links are resolved via the I14Y public API. The downloaded CSV
is hosted on ocean.nivel.bazg.admin.ch and requires a Referer header from i14y.admin.ch.
"""
import io
from calendar import monthrange
from datetime import datetime, date

import pandas as pd
import requests

# ── I14Y public API ──────────────────────────────────────────────────────────
I14Y_API_URL = 'https://api.i14y.admin.ch/api/public/v1/datasets'

# Fallback URL discovered when the gold imports dataset was found via I14Y API.
# Dataset: "Goods: Foreign trade – Gold imports by country"
# I14Y dataset ID: 9b7bcf84-cc04-440e-b8ab-5a4cb56f684f
FALLBACK_CSV_URL = (
    'https://ocean.nivel.bazg.admin.ch/open-data-reports/'
    'TN8_controlCode_Gold_IMP_en_v1/TN8_controlCode_Gold_IMP_en_v1.csv'
)

# CloudFront on ocean.nivel.bazg.admin.ch requires both a browser-like User-Agent
# and a Referer from i14y.admin.ch to serve the CSV (otherwise returns 403).
DOWNLOAD_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (X11; Linux x86_64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://www.i14y.admin.ch/',
    'Origin': 'https://www.i14y.admin.ch',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# ── Country code maps (ISO Alpha-2, matching CSV's Country_isoAlpha2 column) ─
IMPORTS_FROM = {
    'RU': 'Russia',
    'GB': 'United Kingdom',
    'AU': 'Australia',
    'US': 'United States',
    'ZA': 'South Africa',
}

EXPORTS_TO = {
    'CN': 'China',
    'HK': 'Hong Kong',
    'IN': 'India',
    'TR': 'Turkey',
    'AE': 'United Arab Emirates',
    'GB': 'United Kingdom',
}

ASIAN_EXPORT_CODES = {'CN', 'HK', 'IN'}


# ── Internal helpers ─────────────────────────────────────────────────────────

def _find_csv_url_via_api():
    """
    Query I14Y public API and return the English CSV download URL for the
    Swiss gold imports dataset.  Returns None if not found.
    """
    try:
        r = requests.get(
            I14Y_API_URL,
            timeout=30,
            headers={'User-Agent': 'gold-tracker/1.0'},
        )
        r.raise_for_status()
        for item in r.json().get('data', []):
            title_en = ((item.get('title') or {}).get('en') or '').lower()
            if 'gold import' in title_en:
                # Found the dataset — look for English CSV in distributions
                for dist in item.get('distributions', []):
                    url = ((dist.get('downloadUrl') or {}).get('uri') or '')
                    if url.endswith('.csv') and '_en_' in url:
                        return url
                # Fallback: any CSV in distributions
                for dist in item.get('distributions', []):
                    url = ((dist.get('downloadUrl') or {}).get('uri') or '')
                    if url.endswith('.csv'):
                        return url
    except Exception as e:
        print(f'Swiss I14Y: API lookup failed ({e}), will use fallback URL')
    return None


def _find_col(col_map, *candidates):
    """Return the first column name (case-insensitive) that matches a candidate."""
    for cand in candidates:
        if cand.lower() in col_map:
            return col_map[cand.lower()]
    return None


def _parse_csv_bytes(raw_content):
    """Parse raw CSV bytes into a DataFrame, trying utf-8 then latin-1."""
    for encoding in ('utf-8', 'latin-1'):
        try:
            df = pd.read_csv(io.BytesIO(raw_content), sep=';', encoding=encoding)
            if len(df.columns) > 2:
                return df
        except Exception:
            pass
    raise ValueError('Could not parse CSV with semicolon separator and utf-8/latin-1 encoding')


# ── Public API ───────────────────────────────────────────────────────────────

def fetch_swiss_gold_imports():
    """
    Download and parse Swiss gold import data from the FOCBS/BAZG dataset.

    Returns a dict compatible with the serve() cache pattern:
      data               – list of {period, country, country_name, quantity_tonnes, value_chf}
      columns_found      – original CSV column names (for debugging)
      row_count          – number of aggregated rows
      periods_available  – sorted list of YYYY-MM period strings
      latest_period      – most recent period
      fetched_at         – UTC ISO string
      source             – data attribution string
    """
    # Step 1: Resolve the CSV URL
    csv_url = _find_csv_url_via_api() or FALLBACK_CSV_URL
    print(f'Swiss I14Y: downloading CSV from {csv_url}')

    # Step 2: Download
    r = requests.get(csv_url, timeout=60, headers=DOWNLOAD_HEADERS, stream=True)
    r.raise_for_status()
    raw_content = r.content

    # Sanity check: must look like CSV, not HTML error page
    preview = raw_content[:200].decode('utf-8', errors='replace').lower()
    if '<html' in preview or '<!doctype' in preview:
        raise Exception(
            f'I14Y returned HTML instead of CSV from {csv_url} — '
            'Referer/CORS check may have changed'
        )

    # Step 3: Parse
    df = _parse_csv_bytes(raw_content)
    print(f'Swiss I14Y: columns found: {list(df.columns)}')
    print(f'Swiss I14Y: raw rows: {len(df)}')

    col_map = {c.lower(): c for c in df.columns}

    # Step 4: Identify columns
    year_col     = _find_col(col_map, 'year', 'jahr', 'annee', 'año')
    month_col    = _find_col(col_map, 'month', 'monat', 'mois', 'mes')
    country_col  = _find_col(col_map, 'country_isoalpha2', 'country_isocalpha2', 'laendercode',
                             'pays_code', 'country_code', 'iso2', 'iso_alpha2')
    country_name = _find_col(col_map, 'country_txt', 'land', 'pays_txt', 'country_name',
                             'land_txt', 'pays')
    qty_col      = _find_col(col_map, 'quantity_kg', 'menge_kg', 'quantite_kg',
                             'quantity', 'menge', 'quantite', 'kg')
    val_col      = _find_col(col_map, 'value_chf', 'wert_chf', 'valeur_chf',
                             'value', 'wert', 'valeur', 'chf')
    tariff_col   = _find_col(col_map, 'tariffnumber8', 'zolltarifnummer', 'numérotarifaire',
                             'tarifnummer', 'tariff')
    dir_col      = _find_col(col_map, 'traffic_direction', 'richtung', 'direction',
                             'verkehrsrichtung')

    if not year_col or not month_col:
        raise ValueError(
            f'Cannot identify year/month columns. Columns found: {list(df.columns)}'
        )
    if not country_col:
        raise ValueError(
            f'Cannot identify country column. Columns found: {list(df.columns)}'
        )
    if not qty_col:
        raise ValueError(
            f'Cannot identify quantity_kg column. Columns found: {list(df.columns)}'
        )

    # Step 5: Filter for imports only
    if dir_col:
        df = df[df[dir_col].astype(str).str.upper().str[:1] == 'I'].copy()

    # Step 6: Filter by tariff (gold bars 7108.12xx)
    if tariff_col:
        tariff_str = df[tariff_col].astype(str).str.replace('.', '', regex=False).str.strip()
        gold_mask = tariff_str.str.startswith('710812')
        filtered = df[gold_mask].copy()
        if len(filtered) == 0:
            sample = df[tariff_col].dropna().unique()[:8].tolist()
            raise ValueError(
                f'Tariff filter (710812*) returned 0 rows. '
                f'Sample tariff values in file: {sample}'
            )
        df = filtered

    # Step 7: Build YYYY-MM period column
    df = df.copy()
    df['period'] = (
        df[year_col].astype(int).astype(str)
        + '-'
        + df[month_col].astype(int).apply(lambda x: f'{x:02d}')
    )

    # Step 8: Numeric conversions
    df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
    if val_col:
        df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)

    # Step 9: Aggregate by (period, country code, country name)
    name_col_src = country_name or country_col
    agg = {qty_col: 'sum'}
    if val_col:
        agg[val_col] = 'sum'
    grp = ['period', country_col, name_col_src]
    agg_df = df.groupby(grp, as_index=False).agg(agg).sort_values('period')

    # Step 10: Build output rows
    data_rows = []
    for _, row in agg_df.iterrows():
        rec = {
            'period':          row['period'],
            'country':         str(row[country_col]),
            'country_name':    str(row[name_col_src]),
            'quantity_tonnes': round(float(row[qty_col]) / 1000, 3),
        }
        if val_col:
            rec['value_chf'] = int(round(float(row[val_col])))
        data_rows.append(rec)

    periods = sorted(agg_df['period'].unique().tolist())

    return {
        'data':              data_rows,
        'columns_found':     list(df.columns.tolist()),
        'row_count':         len(data_rows),
        'periods_available': periods,
        'latest_period':     periods[-1] if periods else None,
        'fetched_at':        datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source':            'Swiss Federal Customs (FOCBS) via I14Y — tariff 7108.12',
    }


def compute_swiss_signals(imports_data, exports_data=None):
    """
    Compute analytical signals from Swiss gold trade data.

    imports_data: dict returned by fetch_swiss_gold_imports() (or loaded from cache)
    exports_data: same format, from manual upload (or None)

    Returns a signal dict with keys: signal, signal_reason, signal_basis,
    imports, exports, data_period, data_lag_days, has_exports_data.
    """
    imports_rows = (imports_data or {}).get('data', []) if imports_data else []
    exports_rows = (exports_data or {}).get('data', []) if exports_data else []

    if not imports_rows:
        return {
            'signal':        'neutral',
            'signal_reason': 'No Swiss import data — fetch via /api/swiss-trade?refresh=1',
            'signal_basis':  'no_data',
            'imports':       None,
            'exports':       None,
            'data_period':   None,
            'data_lag_days': None,
            'has_exports_data': bool(exports_rows),
        }

    # ── Imports ──────────────────────────────────────────────────────────────
    imp_df = pd.DataFrame(imports_rows)
    imp_df['quantity_tonnes'] = pd.to_numeric(imp_df['quantity_tonnes'], errors='coerce').fillna(0)

    # Aggregate total imports per period (all countries)
    imp_total = imp_df.groupby('period')['quantity_tonnes'].sum().sort_index()
    periods_sorted = list(imp_total.index)

    latest_period = periods_sorted[-1] if periods_sorted else None

    # data_lag_days: calendar days since end of latest period
    data_lag_days = None
    if latest_period:
        try:
            yr, mo = int(latest_period[:4]), int(latest_period[5:7])
            last_day = monthrange(yr, mo)[1]
            period_end = date(yr, mo, last_day)
            data_lag_days = (date.today() - period_end).days
        except Exception:
            pass

    def safe_val(series, idx):
        try:
            return float(series.iloc[idx])
        except (IndexError, TypeError, ValueError):
            return None

    total_latest   = safe_val(imp_total, -1)
    total_3m       = float(imp_total.iloc[-3:].sum()) if len(imp_total) >= 3 else float(imp_total.sum())
    total_6m_avg   = float(imp_total.iloc[-6:].mean()) if len(imp_total) >= 6 else float(imp_total.mean())
    total_yoy_pct  = None
    if len(imp_total) >= 13:
        prev_yr = safe_val(imp_total, -13)
        if prev_yr and prev_yr != 0:
            total_yoy_pct = round((imp_total.iloc[-1] - prev_yr) / abs(prev_yr) * 100, 1)

    # Per-country lookups for latest period
    latest_rows = imp_df[imp_df['period'] == latest_period] if latest_period else imp_df.iloc[0:0]

    def country_val(code):
        rows = latest_rows[latest_rows['country'] == code]
        return round(float(rows['quantity_tonnes'].sum()), 2) if len(rows) else None

    russia_latest  = country_val('RU')
    uk_imp_latest  = country_val('GB')
    mining_supply  = sum(
        v for v in [country_val('RU'), country_val('AU'), country_val('ZA')]
        if v is not None
    ) or None

    # Top 5 countries by volume for latest period
    top5 = (
        latest_rows.groupby('country_name')['quantity_tonnes']
        .sum()
        .nlargest(5)
        .round(2)
        .to_dict()
    )

    imports_dict = {
        'total_latest_tonnes':    round(total_latest, 2) if total_latest is not None else None,
        'total_3m_rolling_tonnes': round(total_3m, 2),
        'total_6m_avg_tonnes':    round(total_6m_avg, 2),
        'total_yoy_pct':          total_yoy_pct,
        'russia_latest_tonnes':   russia_latest,
        'uk_latest_tonnes':       uk_imp_latest,
        'mining_supply_latest_tonnes': round(mining_supply, 2) if mining_supply else None,
        'by_country_latest':      top5,
    }

    # ── Exports (optional) ───────────────────────────────────────────────────
    exports_dict = None
    has_exports = bool(exports_rows)

    if has_exports:
        exp_df = pd.DataFrame(exports_rows)
        exp_df['quantity_tonnes'] = pd.to_numeric(
            exp_df['quantity_tonnes'], errors='coerce'
        ).fillna(0)

        # Asian exports: CN + HK + IN
        asian_df  = exp_df[exp_df['country'].isin(ASIAN_EXPORT_CODES)]
        asian_tot = asian_df.groupby('period')['quantity_tonnes'].sum().sort_index()

        asian_latest   = round(float(asian_tot.iloc[-1]), 2)  if len(asian_tot) else None
        asian_3m       = round(float(asian_tot.iloc[-3:].sum()), 2) if len(asian_tot) >= 3 else \
                         round(float(asian_tot.sum()), 2)
        asian_6m_avg   = round(float(asian_tot.iloc[-6:].mean()), 2) if len(asian_tot) >= 6 else \
                         round(float(asian_tot.mean()), 2)
        asian_yoy_pct  = None
        if len(asian_tot) >= 13:
            prev = float(asian_tot.iloc[-13])
            if prev != 0:
                asian_yoy_pct = round((float(asian_tot.iloc[-1]) - prev) / abs(prev) * 100, 1)

        exp_latest_period = list(asian_tot.index)[-1] if len(asian_tot) else None
        exp_latest_rows   = exp_df[exp_df['period'] == exp_latest_period]

        uk_exp_latest = None
        uk_rows = exp_latest_rows[exp_latest_rows['country'] == 'GB']
        if len(uk_rows):
            uk_exp_latest = round(float(uk_rows['quantity_tonnes'].sum()), 2)

        top5_exp = (
            exp_latest_rows.groupby('country_name')['quantity_tonnes']
            .sum()
            .nlargest(5)
            .round(2)
            .to_dict()
        )

        exports_dict = {
            'asian_exports_latest_tonnes': asian_latest,
            'asian_exports_3m_rolling':    asian_3m,
            'asian_exports_6m_avg':        asian_6m_avg,
            'asian_exports_yoy_pct':       asian_yoy_pct,
            'uk_exports_latest_tonnes':    uk_exp_latest,
            'by_country_latest':           top5_exp,
        }

    # ── Signal computation ───────────────────────────────────────────────────
    if has_exports and exports_dict:
        a3m  = exports_dict['asian_exports_3m_rolling']
        a6m  = exports_dict['asian_exports_6m_avg']
        ayoy = exports_dict['asian_exports_yoy_pct']
        if a3m is not None and a6m is not None:
            if a3m > a6m and (ayoy is None or ayoy > 0):
                signal = 'bullish'
                signal_reason = (
                    f'Asian exports {a3m:.0f}t (3M rolling) above '
                    f'6M avg {a6m:.0f}t'
                    + (f', up {ayoy:+.1f}% YoY' if ayoy is not None else '')
                )
            elif a3m < a6m and (ayoy is not None and ayoy < 0):
                signal = 'bearish'
                signal_reason = (
                    f'Asian exports {a3m:.0f}t (3M rolling) below '
                    f'6M avg {a6m:.0f}t, down {ayoy:.1f}% YoY'
                )
            else:
                signal = 'neutral'
                signal_reason = (
                    f'Asian exports {a3m:.0f}t (3M) vs 6M avg {a6m:.0f}t'
                    + (f' ({ayoy:+.1f}% YoY)' if ayoy is not None else ' (no YoY yet)')
                )
        else:
            signal = 'neutral'
            signal_reason = 'Insufficient exports history for signal'
        signal_basis = 'exports'

    else:
        # Imports proxy
        t3m  = imports_dict['total_3m_rolling_tonnes']
        t6m  = imports_dict['total_6m_avg_tonnes']
        tyoy = imports_dict['total_yoy_pct']

        if not has_exports:
            no_exp_note = ' — upload exports CSV via /api/swiss-trade/upload-exports'
        else:
            no_exp_note = ''

        if len(imp_total) < 6:
            signal = 'neutral'
            signal_reason = f'Insufficient history for signal ({len(imp_total)} months){no_exp_note}'
        elif tyoy is None:
            signal = 'neutral'
            signal_reason = (
                f'No exports data — import proxy: 3M {t3m:.0f}t vs 6M avg {t6m:.0f}t, '
                f'need 13+ months for YoY{no_exp_note}'
            )
        elif t3m > t6m and tyoy > 0:
            signal = 'bullish'
            signal_reason = (
                f'Import proxy: 3M rolling {t3m:.0f}t above 6M avg {t6m:.0f}t, '
                f'up {tyoy:+.1f}% YoY{no_exp_note}'
            )
        elif t3m < t6m and tyoy < 0:
            signal = 'bearish'
            signal_reason = (
                f'Import proxy: 3M rolling {t3m:.0f}t below 6M avg {t6m:.0f}t, '
                f'down {tyoy:.1f}% YoY{no_exp_note}'
            )
        else:
            signal = 'neutral'
            signal_reason = (
                f'Import proxy: 3M {t3m:.0f}t vs 6M avg {t6m:.0f}t '
                f'({tyoy:+.1f}% YoY){no_exp_note}'
            )
        signal_basis = 'imports_proxy'

    return {
        'signal':           signal,
        'signal_reason':    signal_reason,
        'signal_basis':     signal_basis,
        'imports':          imports_dict,
        'exports':          exports_dict,
        'data_period':      latest_period,
        'data_lag_days':    data_lag_days,
        'has_exports_data': has_exports,
    }
