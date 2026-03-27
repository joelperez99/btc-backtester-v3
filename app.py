#!/usr/bin/env python3
"""
BitPredict Backtester v2 — Streamlit + Google Sheets Cache
==========================================================
Igual que btc_backtest_v2 pero con cache persistente en Google Sheets:
  - Cada mes de datos se guarda en una hoja "1m_YYYY_MM" / "5m_YYYY_MM"
  - Si la hoja ya existe, se usa sin re-descargar de Binance
  - Años disponibles desde 2020
"""

import streamlit as st
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import calendar as cal_lib
import io
import time
from pathlib import Path
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials

# ── Config ─────────────────────────────────────────────────────────────────────
SYMBOL     = "BTCUSDT"
BET_SIZE   = 10.0
SHEET_ID   = "1B1AHIHt-yoELcL2p_7ItSeJUPYiIVW8KXFmXjB5bO50"
CREDS_PATH = Path(__file__).parent / "master-plateau-489706-m4-0a4d7843f42f.json"
SCOPES     = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
START_YEAR = 2020  # año más antiguo disponible


def _build_credentials() -> Credentials:
    """
    Carga credenciales desde st.secrets (Streamlit Cloud)
    o desde el archivo JSON local (desarrollo).
    """
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        # Restaurar saltos de línea en private_key si vienen escapados
        if "private_key" in info:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    # Fallback: JSON local
    return Credentials.from_service_account_file(str(CREDS_PATH), scopes=SCOPES)

_BINANCE_ENDPOINTS = [
    "https://api.binance.com/api/v3",
    "https://api1.binance.com/api/v3",
    "https://api2.binance.com/api/v3",
    "https://api3.binance.com/api/v3",
    "https://api4.binance.com/api/v3",
    "https://api.binance.us/api/v3",
]

TIER_DEF = {
    "S": {"min_volume": 500, "min_abs_move": 0.3},
    "A": {"min_volume": 300, "min_abs_move": 0.2},
    "B": {"min_volume": 200, "min_abs_move": 0.1},
    "C": {"min_volume": 100, "min_abs_move": 0.0},
}
TIER_ORDER      = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4}
TIER_COLORS_HEX = {"S": "#ffd700", "A": "#00d68f", "B": "#7c6fff",
                   "C": "#f7931a", "D": "#ff4757"}
_CST_SHIFT = pd.Timedelta(hours=6)
_CST_DELTA = timedelta(hours=-6)

# Columnas de las hojas de velas
KLINE_COLS = ["open_time_ms", "open", "high", "low", "close", "volume", "close_time_ms"]


# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS — CACHE
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_spreadsheet():
    """Retorna el objeto Spreadsheet (cacheado durante la sesión)."""
    gc = gspread.authorize(_build_credentials())
    return gc.open_by_key(SHEET_ID)


def _sname(interval: str, year: int, month: int) -> str:
    return f"{interval}_{year}_{month:02d}"


def _ws_exists(ss, name: str) -> bool:
    try:
        ss.worksheet(name)
        return True
    except gspread.exceptions.WorksheetNotFound:
        return False


def load_month_from_sheets(ss, interval: str, year: int, month: int) -> pd.DataFrame:
    """Carga un mes desde Google Sheets. Retorna DataFrame vacío si no existe."""
    name = _sname(interval, year, month)
    try:
        ws   = ss.worksheet(name)
        data = ws.get_all_values()
        if len(data) <= 1:
            return pd.DataFrame()
        df = pd.DataFrame(data[1:], columns=data[0])
        for c in KLINE_COLS:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.dropna(subset=["open_time_ms"], inplace=True)
        df = df.astype({"open_time_ms": "int64", "close_time_ms": "int64"})
        df["open_time"]  = pd.to_datetime(df["open_time_ms"],  unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)
        return (df[["open_time", "open", "high", "low", "close", "volume", "close_time"]]
                .reset_index(drop=True))
    except gspread.exceptions.WorksheetNotFound:
        return pd.DataFrame()


def save_month_to_sheets(ss, interval: str, df: pd.DataFrame,
                          year: int, month: int, status_fn=None):
    """Guarda (o sobrescribe) un mes completo en Google Sheets."""
    if df.empty:
        return
    name = _sname(interval, year, month)
    rows = [
        [int(r["open_time"].value  // 1_000_000),
         float(r["open"]), float(r["high"]), float(r["low"]),
         float(r["close"]), float(r["volume"]),
         int(r["close_time"].value // 1_000_000)]
        for _, r in df.iterrows()
    ]
    if status_fn:
        status_fn(f"Guardando {interval} {year}-{month:02d} ({len(rows):,} velas)…")
    try:
        ws = ss.worksheet(name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=len(rows) + 5, cols=7)
    ws.update([KLINE_COLS] + rows, value_input_option="RAW")


def get_sheets_stats() -> dict:
    """Retorna stats de los datos cacheados en Sheets."""
    try:
        ss    = get_spreadsheet()
        names = [ws.title for ws in ss.worksheets()]
        result = {}
        for interval in ("1m", "5m"):
            dates = []
            for n in names:
                parts = n.split("_")
                if len(parts) == 3 and parts[0] == interval:
                    try:
                        dates.append((int(parts[1]), int(parts[2])))
                    except ValueError:
                        pass
            if dates:
                result[interval] = {
                    "months": len(dates),
                    "min":    f"{min(dates)[0]}-{min(dates)[1]:02d}",
                    "max":    f"{max(dates)[0]}-{max(dates)[1]:02d}",
                }
            else:
                result[interval] = {"months": 0, "min": None, "max": None}
        return result
    except Exception as ex:
        return {"1m": {"months": 0, "error": str(ex)},
                "5m": {"months": 0, "error": str(ex)}}


def fetch_and_cache(interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    Devuelve velas para [start_ms, end_ms).
    Lee de Google Sheets cuando hay cache; descarga de Binance lo que falte.
    """
    ss     = get_spreadsheet()
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp((end_ms - 1) / 1000, tz=timezone.utc)

    # Construir lista de meses: incluimos el mes anterior para warmup
    pm, py = (start_dt.month - 1, start_dt.year) if start_dt.month > 1 \
             else (12, start_dt.year - 1)
    months = [(py, pm)]
    y, m   = start_dt.year, start_dt.month
    while (y, m) <= (end_dt.year, end_dt.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1; y += 1
    months = sorted(set(months))

    now    = datetime.now(tz=timezone.utc)
    all_dfs = []

    for year, month in months:
        current_month = (year == now.year and month == now.month)
        df_cached     = load_month_from_sheets(ss, interval, year, month)

        if not df_cached.empty:
            if not current_month:
                all_dfs.append(df_cached)
                continue
            # Mes actual: verificar si está actualizado
            interval_ms = 60_000 if interval == "1m" else 300_000
            cached_max  = int(df_cached["open_time"].max().value // 1_000_000)
            if cached_max >= end_ms - interval_ms * 5:
                all_dfs.append(df_cached)
                continue
            # Faltan velas recientes: descargar delta
            new_df = fetch_klines_range(interval, cached_max + 1, min(end_ms, now_ms))
            if not new_df.empty:
                df_full = (pd.concat([df_cached, new_df])
                           .drop_duplicates("open_time")
                           .sort_values("open_time")
                           .reset_index(drop=True))
                save_month_to_sheets(ss, interval, df_full, year, month)
                all_dfs.append(df_full)
            else:
                all_dfs.append(df_cached)
            continue

        # Sin cache → descargar mes completo de Binance
        month_start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
        last_day    = cal_lib.monthrange(year, month)[1]
        month_end   = month_start + pd.Timedelta(days=last_day)

        fetch_s = int(month_start.value // 1_000_000)
        fetch_e = int(min(month_end.value // 1_000_000, now_ms))

        if fetch_s >= now_ms:
            continue  # mes futuro

        new_df = fetch_klines_range(interval, fetch_s, fetch_e)
        if not new_df.empty:
            save_month_to_sheets(ss, interval, new_df, year, month)
            all_dfs.append(new_df)
        time.sleep(0.3)  # margen para API de Sheets

    if not all_dfs:
        return pd.DataFrame()

    result = (pd.concat(all_dfs, ignore_index=True)
              .drop_duplicates("open_time")
              .sort_values("open_time")
              .reset_index(drop=True))

    start_ts = pd.Timestamp(start_ms, unit="ms", tz="UTC")
    end_ts   = pd.Timestamp(end_ms,   unit="ms", tz="UTC")
    return result[(result["open_time"] >= start_ts) &
                  (result["open_time"] <  end_ts)].reset_index(drop=True)


def download_history(start_year: int = START_YEAR, progress_cb=None) -> dict:
    """Descarga todos los meses desde start_year hasta hoy y los guarda en Sheets."""
    ss     = get_spreadsheet()
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    now    = datetime.now(tz=timezone.utc)
    counts = {"1m": 0, "5m": 0}

    months = []
    y, m   = start_year, 1
    while (y, m) <= (now.year, now.month):
        months.append((y, m)); m += 1
        if m > 12: m = 1; y += 1

    total = len(months) * 2
    done  = 0

    for interval in ("1m", "5m"):
        for year, month in months:
            done += 1
            pct  = done / total
            name = _sname(interval, year, month)
            current = (year == now.year and month == now.month)

            if _ws_exists(ss, name) and not current:
                if progress_cb:
                    progress_cb(f"✓ {interval} {year}-{month:02d} ya guardado", pct)
                continue

            month_start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
            last_day    = cal_lib.monthrange(year, month)[1]
            month_end   = month_start + pd.Timedelta(days=last_day)
            fetch_s = int(month_start.value // 1_000_000)
            fetch_e = int(min(month_end.value // 1_000_000, now_ms))

            if progress_cb:
                progress_cb(f"Descargando {interval} {year}-{month:02d}…", pct)

            new_df = fetch_klines_range(interval, fetch_s, fetch_e)
            if not new_df.empty:
                save_month_to_sheets(ss, interval, new_df, year, month)
                counts[interval] += len(new_df)
            time.sleep(1.0)

    return counts


def update_current_month(progress_cb=None) -> dict:
    """Actualiza solo el mes actual en Google Sheets."""
    ss     = get_spreadsheet()
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    now    = datetime.now(tz=timezone.utc)
    counts = {"1m": 0, "5m": 0}

    for interval in ("1m", "5m"):
        year, month  = now.year, now.month
        df_cached    = load_month_from_sheets(ss, interval, year, month)
        month_start  = pd.Timestamp(year=year, month=month, day=1, tz="UTC")

        if not df_cached.empty:
            fetch_s = int(df_cached["open_time"].max().value // 1_000_000) + 1
        else:
            fetch_s = int(month_start.value // 1_000_000)

        if progress_cb:
            progress_cb(f"Descargando {interval} nuevas velas…")

        new_df = fetch_klines_range(interval, fetch_s, now_ms)
        if not new_df.empty:
            if not df_cached.empty:
                df_full = (pd.concat([df_cached, new_df])
                           .drop_duplicates("open_time")
                           .sort_values("open_time")
                           .reset_index(drop=True))
            else:
                df_full = new_df
            save_month_to_sheets(ss, interval, df_full, year, month)
            counts[interval] = len(new_df)

        if progress_cb:
            progress_cb(f"{interval}: +{counts[interval]} velas nuevas")

    return counts


# ══════════════════════════════════════════════════════════════════════════════
# BINANCE DATA
# ══════════════════════════════════════════════════════════════════════════════

def _try_fetch_page(base_url: str, interval: str, current: int, end_ms: int) -> list:
    r = requests.get(
        f"{base_url}/klines",
        params={"symbol": "BTCUSDT", "interval": interval,
                "startTime": current, "endTime": end_ms, "limit": 1000},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def fetch_klines_range(interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    all_rows   = []
    current    = start_ms
    active_url = None

    while current < end_ms:
        rows     = None
        last_err = None
        for endpoint in _BINANCE_ENDPOINTS:
            try:
                rows       = _try_fetch_page(endpoint, interval, current, end_ms)
                active_url = endpoint
                break
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (451, 403):
                    last_err = e; continue
                raise
            except Exception as e:
                last_err = e; continue

        if rows is None:
            raise ConnectionError(
                f"Ningún endpoint de Binance respondió.\nÚltimo error: {last_err}")
        if not rows:
            break
        all_rows.extend(rows)
        current = rows[-1][6] + 1
        if len(rows) < 1000:
            break

    if not all_rows:
        return pd.DataFrame()

    cols = ["open_time","open","high","low","close","volume",
            "close_time","qvol","trades","tb","tq","ignore"]
    df = pd.DataFrame(all_rows, columns=cols)
    for c in ("open","high","low","close","volume"):
        df[c] = df[c].astype(float)
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return (df.drop_duplicates("open_time")
            .sort_values("open_time")
            .reset_index(drop=True))


# ══════════════════════════════════════════════════════════════════════════════
# INDICADORES
# ══════════════════════════════════════════════════════════════════════════════

def _ema(s, n):   return s.ewm(span=n, adjust=False).mean()

def _rsi(s, n=14):
    d = s.diff()
    g = d.clip(lower=0).rolling(n).mean()
    l = (-d.clip(upper=0)).rolling(n).mean()
    return float((100 - 100 / (1 + g / l.replace(0, np.nan))).iloc[-1])

def _macd_hist(s):
    m = _ema(s, 12) - _ema(s, 26)
    return float((m - _ema(m, 9)).iloc[-1])

def _bb_pct(s, n=20):
    mid = s.rolling(n).mean(); std = s.rolling(n).std()
    lo  = (mid - 2*std).iloc[-1]; hi = (mid + 2*std).iloc[-1]
    return float((s.iloc[-1] - lo) / ((hi - lo) + 1e-9))

def _mom(s, n=5):
    return float((s.iloc[-1] - s.iloc[-(n+1)]) / s.iloc[-(n+1)] * 100)

def _vol_ratio(v, recent=5, hist=20):
    return float(v.iloc[-recent:].mean() / (v.iloc[-(hist+recent):-recent].mean() + 1e-9))


def predict_from_df(df1m: pd.DataFrame):
    c, v = df1m["close"], df1m["volume"]
    votes = []

    r_val = _rsi(c)
    if   r_val < 42: votes.append(("RSI", "UP",      min((42 - r_val) / 42, 1)))
    elif r_val > 58: votes.append(("RSI", "DOWN",    min((r_val - 58) / 42, 1)))
    else:            votes.append(("RSI", "NEUTRAL", 0.0))

    mh = _macd_hist(c)
    votes.append(("MACD", "UP" if mh > 0 else "DOWN", min(abs(mh) / 30, 1)))

    e9  = float(_ema(c,  9).iloc[-1]); e21 = float(_ema(c, 21).iloc[-1])
    dp  = (e9 - e21) / e21 * 100
    votes.append(("EMA", "UP" if e9 > e21 else "DOWN", min(abs(dp) / 0.4, 1)))

    bb = _bb_pct(c)
    if   bb < 0.30: votes.append(("Bollinger", "UP",      (0.30 - bb) / 0.30))
    elif bb > 0.70: votes.append(("Bollinger", "DOWN",    (bb - 0.70) / 0.30))
    else:           votes.append(("Bollinger", "NEUTRAL", 0.0))

    mom_v = _mom(c, 5)
    if   mom_v >  0.04: votes.append(("Momentum", "UP",   min(mom_v / 0.3, 1)))
    elif mom_v < -0.04: votes.append(("Momentum", "DOWN", min(abs(mom_v) / 0.3, 1)))
    else:               votes.append(("Momentum", "NEUTRAL", 0.0))

    vr = _vol_ratio(v)
    if vr > 1.25:
        votes.append(("Volume", "UP" if mom_v >= 0 else "DOWN", min((vr - 1) / 1.5, 1)))

    e50 = float(_ema(c, 50).iloc[-1])
    votes.append(("Trend50", "UP" if c.iloc[-1] > e50 else "DOWN", 0.4))

    up_s  = sum(s for _, d, s in votes if d == "UP")
    dn_s  = sum(s for _, d, s in votes if d == "DOWN")
    total = up_s + dn_s or 1e-9
    up_pct = up_s / total * 100; dn_pct = dn_s / total * 100
    direction = "UP" if up_s >= dn_s else "DOWN"
    return direction, max(up_pct, dn_pct), up_pct, dn_pct, votes


# ══════════════════════════════════════════════════════════════════════════════
# TIER / FILTER
# ══════════════════════════════════════════════════════════════════════════════

def get_trade_tier(volume: float, abs_move: float) -> str:
    if volume >= TIER_DEF["S"]["min_volume"] and abs_move >= TIER_DEF["S"]["min_abs_move"]:
        return "S"
    if volume >= TIER_DEF["A"]["min_volume"] and abs_move >= TIER_DEF["A"]["min_abs_move"]:
        return "A"
    if volume >= TIER_DEF["B"]["min_volume"] and abs_move >= TIER_DEF["B"]["min_abs_move"]:
        return "B"
    if volume >= TIER_DEF["C"]["min_volume"]:
        return "C"
    return "D"


def passes_filter(volume, abs_move, hour, confidence, filters):
    if filters["min_volume"]     and volume     < filters["min_volume"]:     return False
    if filters["min_abs_move"]   and abs_move   < filters["min_abs_move"]:   return False
    if filters["allowed_hours"]  and hour not in filters["allowed_hours"]:   return False
    if filters["min_confidence"] and confidence < filters["min_confidence"]: return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# PROCESAMIENTO DE VELAS
# ══════════════════════════════════════════════════════════════════════════════

def _process_candles(df5m_range: pd.DataFrame, df1m_full: pd.DataFrame,
                     filters: dict) -> list:
    results = []
    n = len(df5m_range)
    for i in range(n - 1):
        c5      = df5m_range.iloc[i]
        c5_next = df5m_range.iloc[i + 1]

        ctx = df1m_full[df1m_full["open_time"] < c5["close_time"]].tail(120)
        if len(ctx) < 50:
            continue

        direction, conf, up_pct, dn_pct, votes = predict_from_df(ctx)

        sig_vol  = float(c5["volume"])
        sig_move = abs((float(c5["close"]) - float(c5["open"])) / float(c5["open"]) * 100)

        open_price  = float(c5["close"])
        close_price = float(c5_next["close"])
        actual      = "UP" if close_price >= open_price else "DOWN"
        correct     = direction == actual
        pct_move    = (close_price - open_price) / open_price * 100

        tier   = get_trade_tier(sig_vol, sig_move)
        c_open = c5["close_time"]
        hour   = c_open.to_pydatetime().astimezone().hour
        in_flt = passes_filter(sig_vol, sig_move, hour, conf, filters)

        m1s = df1m_full[
            (df1m_full["open_time"] >= c5_next["open_time"]) &
            (df1m_full["open_time"] <  c5_next["close_time"])
        ].reset_index(drop=True)
        min_correct = {}
        for mi, m1r in m1s.iterrows():
            mn = mi + 1
            mc = float(m1r["close"])
            min_correct[mn] = ("UP" if mc >= open_price else "DOWN") == direction

        sigs = "|".join(f"{v[0]}:{v[1]}" for v in votes)
        loc  = c5_next["open_time"].to_pydatetime().astimezone()
        utc  = c5_next["open_time"].to_pydatetime()

        results.append({
            "date":            loc.strftime("%Y-%m-%d"),
            "time":            loc.strftime("%H:%M"),
            "hour":            loc.hour,
            "timestamp_utc":   utc.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_local": loc.strftime("%Y-%m-%d %H:%M:%S"),
            "open_price":      open_price,
            "close_price":     close_price,
            "pct_move":        pct_move,
            "abs_move":        abs(pct_move),
            "prediction":      direction,
            "actual":          actual,
            "correct":         correct,
            "confidence":      conf,
            "up_pct":          up_pct,
            "dn_pct":          dn_pct,
            "signals":         sigs,
            "minute_correct":  min_correct,
            "high":            float(c5_next["high"]),
            "low":             float(c5_next["low"]),
            "volume":          float(c5_next["volume"]),
            "signal_volume":   sig_vol,
            "signal_move":     sig_move,
            "tier":            tier,
            "in_filter":       in_flt,
        })
    return results


# ══════════════════════════════════════════════════════════════════════════════
# P&L
# ══════════════════════════════════════════════════════════════════════════════

def simulate_pnl(df: pd.DataFrame, bet_size: float = BET_SIZE,
                 min_tier: str = "B") -> dict:
    filtered = df[df["tier"].map(TIER_ORDER) <= TIER_ORDER.get(min_tier, 4)]
    n = len(filtered)
    if n == 0:
        return dict(total_trades=0, wins=0, losses=0, accuracy=0,
                    total_pnl=0.0, pnl_per_day=0.0, roi_pct=0.0, days=0)
    wins      = int(filtered["correct"].sum())
    losses    = n - wins
    total_pnl = (wins - losses) * bet_size
    days      = max(1, filtered["date"].nunique())
    return dict(
        total_trades=n, wins=wins, losses=losses,
        accuracy=round(wins / n * 100, 2),
        total_pnl=round(total_pnl, 2),
        pnl_per_day=round(total_pnl / days, 2),
        roi_pct=round(total_pnl / (n * bet_size) * 100, 2),
        days=days,
    )


def filtered_pnl(df: pd.DataFrame, tiers: list, bet_size: float = BET_SIZE) -> dict:
    sub = df[df["tier"].isin(tiers)] if tiers else df.iloc[0:0]
    n   = len(sub)
    if n == 0:
        return dict(total_trades=0, wins=0, losses=0, accuracy=0,
                    total_pnl=0.0, pnl_per_day=0.0, roi_pct=0.0, days=0)
    wins      = int(sub["correct"].sum())
    losses    = n - wins
    total_pnl = (wins - losses) * bet_size
    days      = max(1, sub["date"].nunique())
    return dict(
        total_trades=n, wins=wins, losses=losses,
        accuracy=round(wins / n * 100, 2),
        total_pnl=round(total_pnl, 2),
        pnl_per_day=round(total_pnl / days, 2),
        roi_pct=round(total_pnl / (n * bet_size) * 100, 2),
        days=days,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ESTADÍSTICAS
# ══════════════════════════════════════════════════════════════════════════════

def _compute_stats(df: pd.DataFrame) -> dict:
    total  = len(df)
    wins   = int(df["correct"].sum())
    losses = total - wins
    acc    = wins / total * 100

    by_hour = df.groupby("hour")["correct"].agg(["sum","count"]).rename(
        columns={"sum":"wins","count":"total"})
    by_hour["accuracy"] = by_hour["wins"] / by_hour["total"] * 100

    bins_c = [50,55,60,65,70,75,80,85,90,95,101]
    labs_c = ["50-55","55-60","60-65","65-70","70-75",
              "75-80","80-85","85-90","90-95","95-100"]
    df2 = df.copy()
    df2["conf_bin"] = pd.cut(df2["confidence"], bins=bins_c, labels=labs_c, right=False)
    by_conf = df2.groupby("conf_bin", observed=False)["correct"].agg(["sum","count"]).rename(
        columns={"sum":"wins","count":"total"})
    by_conf["accuracy"] = by_conf.apply(
        lambda r: r["wins"]/r["total"]*100 if r["total"]>0 else float("nan"), axis=1)

    minute_acc = {}
    for mn in range(1, 6):
        col   = df["minute_correct"].apply(lambda d: d.get(mn))
        valid = col.dropna()
        if len(valid):
            minute_acc[mn] = {"correct": int(valid.sum()), "total": int(len(valid)),
                               "accuracy": valid.mean() * 100}
    best_minute = max(minute_acc, key=lambda m: abs(minute_acc[m]["accuracy"]-50), default=None)

    all_sigs = []
    for sig_str in df["signals"]:
        for part in sig_str.split("|"):
            if ":" in part:
                sn, sd = part.split(":", 1)
                all_sigs.append({"signal": sn, "direction": sd})
    sig_df = pd.DataFrame(all_sigs)
    signal_bias = {}
    if not sig_df.empty:
        for sn, grp in sig_df.groupby("signal"):
            signal_bias[sn] = {
                "UP":      int((grp["direction"]=="UP").sum()),
                "DOWN":    int((grp["direction"]=="DOWN").sum()),
                "NEUTRAL": int((grp["direction"]=="NEUTRAL").sum()),
                "total":   int(len(grp)),
            }

    by_pred = df.groupby("prediction")["correct"].agg(["sum","count"])
    by_pred["accuracy"] = by_pred["sum"] / by_pred["count"] * 100

    max_ws = max_ls = cw = cl = 0
    for c in df["correct"]:
        if c: cw += 1; cl = 0
        else: cl += 1; cw = 0
        max_ws = max(max_ws, cw); max_ls = max(max_ls, cl)

    pct_desc  = df["pct_move"].describe()
    conf_win  = df[df["correct"]]["confidence"].mean()  if wins   else float("nan")
    conf_loss = df[~df["correct"]]["confidence"].mean() if losses else float("nan")

    bhs = by_hour[by_hour["total"] >= 3].sort_values("accuracy", ascending=False)
    best_hour  = int(bhs.index[0])  if len(bhs) else None
    worst_hour = int(bhs.index[-1]) if len(bhs) else None

    vol_win  = df[df["correct"]]["signal_volume"].mean()  if wins   else float("nan")
    vol_loss = df[~df["correct"]]["signal_volume"].mean() if losses else float("nan")

    df2["range_pct"] = (df2["high"] - df2["low"]) / df2["open_price"] * 100
    range_win  = df2[df2["correct"]]["range_pct"].mean()  if wins   else float("nan")
    range_loss = df2[~df2["correct"]]["range_pct"].mean() if losses else float("nan")

    by_day = None
    if "date" in df.columns and df["date"].nunique() > 1:
        by_day = df.groupby("date")["correct"].agg(["sum","count"]).rename(
            columns={"sum":"wins","count":"total"})
        by_day["accuracy"] = by_day["wins"] / by_day["total"] * 100
        by_day = by_day.sort_index()

    by_tier = df.groupby("tier")["correct"].agg(["sum","count"]).rename(
        columns={"sum":"wins","count":"total"})
    by_tier["accuracy"] = by_tier["wins"] / by_tier["total"] * 100
    by_tier = by_tier.reindex([t for t in ["S","A","B","C","D"] if t in by_tier.index])

    pnl_rows = []
    for mt in ["S", "A", "B", "C", "D"]:
        p = simulate_pnl(df, bet_size=BET_SIZE, min_tier=mt)
        p["min_tier"] = mt
        pnl_rows.append(p)
    pnl_table = pd.DataFrame(pnl_rows).set_index("min_tier")

    vol_bins = [0, 50, 100, 200, 300, 500, 1000, float("inf")]
    vol_labs = ["0-50","50-100","100-200","200-300","300-500","500-1000","1000+"]
    df2["sig_vol_bin"] = pd.cut(df2["signal_volume"], bins=vol_bins, labels=vol_labs, right=False)
    by_signal_vol = df2.groupby("sig_vol_bin", observed=False)["correct"].agg(["sum","count"]).rename(
        columns={"sum":"wins","count":"total"})
    by_signal_vol["accuracy"] = by_signal_vol.apply(
        lambda r: r["wins"]/r["total"]*100 if r["total"]>0 else float("nan"), axis=1)

    df2["tgt_vol_bin"] = pd.cut(df2["volume"], bins=vol_bins, labels=vol_labs, right=False)
    by_target_vol = df2.groupby("tgt_vol_bin", observed=False)["correct"].agg(["sum","count"]).rename(
        columns={"sum":"wins","count":"total"})
    by_target_vol["accuracy"] = by_target_vol.apply(
        lambda r: r["wins"]/r["total"]*100 if r["total"]>0 else float("nan"), axis=1)

    filt_df      = df[df["tier"].map(TIER_ORDER) <= TIER_ORDER["B"]]
    filt_acc     = filt_df["correct"].mean() * 100 if len(filt_df) else float("nan")
    filt_total   = len(filt_df)
    n_days       = max(1, df["date"].nunique())
    filt_per_day = filt_total / n_days

    return dict(
        total=total, wins=wins, losses=losses, accuracy=acc,
        by_hour=by_hour, by_conf=by_conf,
        minute_acc=minute_acc, best_minute=best_minute,
        signal_bias=signal_bias, by_pred=by_pred,
        max_win_streak=max_ws, max_loss_streak=max_ls,
        pct_desc=pct_desc, conf_win=conf_win, conf_loss=conf_loss,
        best_hour=best_hour, worst_hour=worst_hour,
        vol_win=vol_win, vol_loss=vol_loss,
        range_win=range_win, range_loss=range_loss,
        by_day=by_day, by_tier=by_tier, pnl_table=pnl_table,
        by_signal_vol=by_signal_vol, by_target_vol=by_target_vol,
        filt_acc=filt_acc, filt_total=filt_total, filt_per_day=filt_per_day,
    )


# ══════════════════════════════════════════════════════════════════════════════
# CST
# ══════════════════════════════════════════════════════════════════════════════

def _to_cst(utc_str: str) -> str:
    dt  = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    cst = dt + _CST_DELTA
    return cst.strftime("%Y-%m-%d %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# MOTORES DE BACKTEST
# ══════════════════════════════════════════════════════════════════════════════

def run_backtest(date_str: str, filters: dict) -> dict:
    day_start    = pd.Timestamp(date_str, tz="UTC") + _CST_SHIFT
    day_end      = day_start + pd.Timedelta(days=1)
    warmup_start = day_start - pd.Timedelta(hours=2)
    fetch_end    = day_end   + pd.Timedelta(minutes=10)
    start_ms = int(warmup_start.value // 1_000_000)
    end_ms   = int(fetch_end.value    // 1_000_000)

    df1m = fetch_and_cache("1m", start_ms, end_ms)
    df5m = fetch_and_cache("5m", start_ms, end_ms)
    if df1m.empty or df5m.empty:
        raise ValueError("No se obtuvieron datos para esa fecha.")

    df5m_day = df5m[(df5m["open_time"] >= day_start) &
                    (df5m["open_time"] <  day_end)].reset_index(drop=True)
    last_signal_close = df5m_day.iloc[-1]["close_time"] if not df5m_day.empty else day_end
    df5m_window = df5m[(df5m["open_time"] >= day_start) &
                       (df5m["open_time"] <= last_signal_close)].reset_index(drop=True)

    records = _process_candles(df5m_window, df1m, filters)
    if not records:
        raise ValueError("No hay velas 5m procesables para ese día.")

    df = pd.DataFrame(records)
    df = df[df["timestamp_utc"].apply(lambda u: _to_cst(u)[:10] == date_str)].reset_index(drop=True)
    if df.empty:
        raise ValueError("No hay velas 5m en fecha CST para ese día.")

    stats = _compute_stats(df)
    return {"records": df.to_dict("records"), "df": df, "stats": stats,
            "label": date_str, "mode": "day"}


def run_backtest_month(year_str: str, month_str: str, filters: dict,
                       progress_cb=None) -> dict:
    year  = int(year_str); month = int(month_str)
    month_start  = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    last_day     = cal_lib.monthrange(year, month)[1]
    month_end    = month_start + pd.Timedelta(days=last_day)
    warmup_start = month_start - pd.Timedelta(hours=2)
    fetch_end    = month_end   + pd.Timedelta(minutes=10)
    start_ms = int(warmup_start.value // 1_000_000)
    end_ms   = int(fetch_end.value    // 1_000_000)

    df1m = fetch_and_cache("1m", start_ms, end_ms)
    df5m = fetch_and_cache("5m", start_ms, end_ms)
    if df1m.empty or df5m.empty:
        raise ValueError("No se obtuvieron datos para ese mes.")

    df5m_month = df5m[(df5m["open_time"] >= month_start) &
                      (df5m["open_time"] <  fetch_end)].reset_index(drop=True)
    all_records = []
    for idx in range(last_day):
        day_ts     = month_start + pd.Timedelta(days=idx)
        day_end_ts = day_ts + pd.Timedelta(days=1)
        df5m_day = df5m_month[
            (df5m_month["open_time"] >= day_ts) &
            (df5m_month["open_time"] <  day_end_ts + pd.Timedelta(minutes=5))
        ].reset_index(drop=True)
        if not df5m_day.empty:
            all_records.extend(_process_candles(df5m_day, df1m, filters))
        if progress_cb:
            progress_cb(idx + 1, last_day)

    if not all_records:
        raise ValueError("No hay velas 5m procesables para ese mes.")

    df    = pd.DataFrame(all_records)
    stats = _compute_stats(df)
    return {"records": all_records, "df": df, "stats": stats,
            "label": f"{year_str}-{month_str}", "mode": "month"}


def run_backtest_year(year_str: str, filters: dict, progress_cb=None) -> dict:
    year         = int(year_str)
    year_start   = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
    year_end     = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
    warmup_start = year_start - pd.Timedelta(hours=2)
    fetch_end    = year_end   + pd.Timedelta(minutes=10)
    start_ms     = int(warmup_start.value // 1_000_000)
    end_ms       = int(fetch_end.value    // 1_000_000)

    if progress_cb:
        progress_cb("Cargando datos 1m y 5m del año…", 0.02)
    df1m = fetch_and_cache("1m", start_ms, end_ms)
    df5m = fetch_and_cache("5m", start_ms, end_ms)
    if df1m.empty or df5m.empty:
        raise ValueError("No se obtuvieron datos para ese año.")

    all_records = []
    for month in range(1, 13):
        month_start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
        last_day    = cal_lib.monthrange(year, month)[1]
        month_end_t = month_start + pd.Timedelta(days=last_day)

        df5m_month = df5m[
            (df5m["open_time"] >= month_start) &
            (df5m["open_time"] <  month_end_t + pd.Timedelta(minutes=10))
        ].reset_index(drop=True)

        for idx in range(last_day):
            day_ts     = month_start + pd.Timedelta(days=idx)
            day_end_ts = day_ts + pd.Timedelta(days=1)
            df5m_day   = df5m_month[
                (df5m_month["open_time"] >= day_ts) &
                (df5m_month["open_time"] <  day_end_ts + pd.Timedelta(minutes=5))
            ].reset_index(drop=True)
            if not df5m_day.empty:
                all_records.extend(_process_candles(df5m_day, df1m, filters))

        if progress_cb:
            month_names = ["Ene","Feb","Mar","Abr","May","Jun",
                           "Jul","Ago","Sep","Oct","Nov","Dic"]
            progress_cb(f"Procesado {month_names[month-1]} {year} ({month}/12)",
                        0.05 + 0.90 * month / 12)

    if not all_records:
        raise ValueError("No hay velas procesables para ese año.")

    df    = pd.DataFrame(all_records)
    stats = _compute_stats(df)
    return {"records": all_records, "df": df, "stats": stats,
            "label": year_str, "mode": "year"}


# ══════════════════════════════════════════════════════════════════════════════
# EXCEL EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def save_excel_bytes(result: dict, bet_size: float = BET_SIZE) -> bytes:
    df = result["df"].copy()
    s  = result["stats"]
    pred_cols = ["date","time","hour","tier","in_filter","prediction","actual","correct",
                 "confidence","pct_move","abs_move","signal_volume","signal_move",
                 "volume","open_price","close_price","high","low"]
    n_days = max(1, df["date"].nunique())
    resumen_rows = [
        ("Período",                  result["label"]),
        ("Lógica",                   "v2 — predice al cierre, evalúa en siguiente vela"),
        ("Días analizados",          n_days),
        ("Total velas",              s["total"]),
        ("Aciertos",                 s["wins"]),
        ("Fallos",                   s["losses"]),
        ("Precisión global %",       round(s["accuracy"], 2)),
        ("── Stats S+A+B ──",        ""),
        ("Trades filtrados",         s["filt_total"]),
        ("Trades filtrados / día",   round(s["filt_per_day"], 1)),
        ("Precisión filtrada %",     round(s["filt_acc"], 2) if not np.isnan(s["filt_acc"]) else ""),
        ("── Métricas ──",           ""),
        ("Racha max aciertos",       s["max_win_streak"]),
        ("Racha max fallos",         s["max_loss_streak"]),
        ("Conf. media aciertos",     round(s["conf_win"],  2) if not np.isnan(s["conf_win"])  else ""),
        ("Conf. media fallos",       round(s["conf_loss"], 2) if not np.isnan(s["conf_loss"]) else ""),
    ]
    df_resumen = pd.DataFrame(resumen_rows, columns=["Métrica", "Valor"])
    min_rows = [
        {"Minuto": mn, "Aciertos": info["correct"], "Total": info["total"],
         "Precisión%": round(info["accuracy"], 2),
         "★": "★" if mn == s["best_minute"] else ""}
        for mn, info in sorted(s["minute_acc"].items())
    ]
    df_min  = pd.DataFrame(min_rows)
    df_sigs = pd.DataFrame([
        {"Señal": sn, "UP": v["UP"], "DOWN": v["DOWN"],
         "NEUTRAL": v["NEUTRAL"], "Total": v["total"]}
        for sn, v in s["signal_bias"].items()
    ])
    df_tier = s["by_tier"].copy().reset_index()
    df_tier.columns = ["Tier", "Aciertos", "Total", "Precisión%"]
    pnl_df = s["pnl_table"].reset_index()
    pnl_df.columns = ["Min Tier","Trades","Wins","Losses","Accuracy%","P&L Total($)","P&L/Día($)","ROI%","Días"]
    df_sigvol = s["by_signal_vol"].reset_index()
    df_sigvol.columns = ["Vol Vela Señal (BTC)","Aciertos","Total","Precisión%"]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_resumen.to_excel(writer, sheet_name="Resumen",       index=False)
        df[pred_cols].to_excel(writer, sheet_name="Predicciones", index=False)
        s["by_hour"].reset_index().to_excel(writer, sheet_name="Por Hora",      index=False)
        s["by_conf"].reset_index().to_excel(writer, sheet_name="Por Confianza", index=False)
        df_min.to_excel(writer,  sheet_name="Por Minuto",   index=False)
        s["by_pred"].reset_index().to_excel(writer, sheet_name="Por Direccion", index=False)
        df_sigs.to_excel(writer, sheet_name="Señales",       index=False)
        if s["by_day"] is not None:
            s["by_day"].reset_index().to_excel(writer, sheet_name="Por Dia", index=False)
        df_tier.to_excel(writer, sheet_name="Por Tier",      index=False)
        pnl_df.to_excel(writer,  sheet_name="P&L Simulado",  index=False)
        df_sigvol.to_excel(writer, sheet_name="Vol Vela Señal", index=False)
    return output.getvalue()


def save_excel_detail_bytes(result: dict) -> bytes:
    df = result["df"].copy()
    detail_rows = []
    for _, r in df.iterrows():
        utc_str = r.get("timestamp_utc", f"{r['date']} {r['time']}:00")
        cst_str = _to_cst(utc_str)
        detail_rows.append({
            "Timestamp CST":        cst_str,
            "Hora Local":           r.get("timestamp_local", ""),
            "Open Price (to beat)": round(r["open_price"],    2),
            "Prediccion":           r["prediction"],
            "Confianza %":          round(r["confidence"],    2),
            "UP %":                 round(r["up_pct"],        2),
            "DOWN %":               round(r["dn_pct"],        2),
            "Vol Señal BTC":        round(r["signal_volume"], 2),
            "Move Señal %":         round(r["signal_move"],   4),
            "Tier":                 r["tier"],
            "En Filtro":            "SI" if r["in_filter"] else "NO",
            "Close Price":          round(r["close_price"],   2),
            "Direccion Real":       r["actual"],
            "Correcto":             "SI" if r["correct"] else "NO",
            "Pct Move %":           round(r["pct_move"],      4),
            "Señales":              r["signals"],
        })
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(detail_rows).to_excel(writer, sheet_name="Detalle", index=False)
    return output.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# UI HELPER — DETALLE DE DÍA
# ══════════════════════════════════════════════════════════════════════════════

def find_daily_limit_hit(day_df: pd.DataFrame, stake: float, limit: float) -> dict:
    running = 0.0
    for i, (_, row) in enumerate(day_df.iterrows()):
        running += stake if row["correct"] else -stake
        if running >= limit:
            return {"hit": "UP",   "amount": running, "op": i + 1, "time": row["time"]}
        if running <= -limit:
            return {"hit": "DOWN", "amount": running, "op": i + 1, "time": row["time"]}
    return {"hit": None, "amount": running, "op": None, "time": None}


def render_day_detail(df_main: pd.DataFrame, date_str: str,
                      sel_tiers: list, stake: float):
    day_df = df_main[
        (df_main["date"] == date_str) & (df_main["tier"].isin(sel_tiers))
    ].reset_index(drop=True)
    if day_df.empty:
        st.info("Sin trades para los tiers seleccionados en este día.")
        return

    wins     = int(day_df["correct"].sum())
    losses   = len(day_df) - wins
    total    = len(day_df)
    net_pnl  = (wins - losses) * stake
    win_rate = wins / total * 100 if total else 0.0

    BANCO_INI = max(100_000.0, stake * 100)
    running   = BANCO_INI
    curve     = [running]
    min_bank = max_bank = running
    min_idx  = max_idx  = 0
    for i, row in day_df.iterrows():
        running += stake if row["correct"] else -stake
        curve.append(running)
        if running < min_bank: min_bank, min_idx = running, int(i) + 1
        if running > max_bank: max_bank, max_idx = running, int(i) + 1

    tier_str = "+".join(sel_tiers) if sel_tiers else "ninguno"
    st.markdown(
        f"<div class='section-hdr'>🔍 Detalle — {date_str} &nbsp;·&nbsp; "
        f"Tiers {tier_str} &nbsp;·&nbsp; ${stake:,.0f}/trade</div>",
        unsafe_allow_html=True)

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Total operaciones", total)
    d2.metric("✅ Gana (SI)", wins)
    d3.metric("❌ Pierde (NO)", losses)
    d4.metric("Balance neto", f"${net_pnl:+,.0f}")

    d5, d6, d7, d8 = st.columns(4)
    d5.metric("Banco inicial", f"${BANCO_INI:,.0f}")
    d6.metric("Banco final",   f"${curve[-1]:,.0f}")
    d7.metric("Win rate",      f"{win_rate:.1f}%")
    d8.metric("Resultado neto",f"${net_pnl:+,.0f}")

    d9, d10 = st.columns(2)
    d9.metric( f"Banco mínimo (op #{min_idx})", f"${min_bank:,.0f}")
    d10.metric(f"Banco máximo (op #{max_idx})", f"${max_bank:,.0f}")

    line_color = "#00d68f" if net_pnl >= 0 else "#ff4757"
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=list(range(len(curve))), y=curve, mode="lines",
        line=dict(color=line_color, width=2),
        fill="tozeroy",
        fillcolor="rgba(0,214,143,0.08)" if net_pnl >= 0 else "rgba(255,71,87,0.08)",
        hovertemplate="Op #%{x}<br>Banco: $%{y:,.0f}<extra></extra>",
        name="Banco",
    ))
    fig.add_hline(y=BANCO_INI, line_dash="dash", line_color="#6e7191", line_width=1)
    fig.update_layout(
        plot_bgcolor="#0d0f1a", paper_bgcolor="#0d0f1a",
        font_color="#e8eaf6", height=280,
        margin=dict(l=20, r=20, t=10, b=30),
        yaxis=dict(gridcolor="#1e2236", tickprefix="$"),
        xaxis=dict(gridcolor="#1e2236", title="Operación #"),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Secuencia de resultados**")
    badges = ""
    for _, row in day_df.iterrows():
        ok  = bool(row["correct"])
        clr = "#00d68f" if ok else "#ff4757"
        bg  = "#0a2e1e" if ok else "#2e0a0a"
        lbl = "SI" if ok else "NO"
        badges += (f"<span style='background:{bg};color:{clr};font-weight:bold;"
                   f"padding:3px 7px;border-radius:4px;margin:2px;font-size:11px;"
                   f"display:inline-block;'>{lbl}</span>")
    st.markdown(f"<div style='line-height:2.2;'>{badges}</div>", unsafe_allow_html=True)

    st.markdown("**Operaciones del día**")
    tbl_rows = []
    for _, row in day_df.iterrows():
        ok    = bool(row["correct"])
        t_pnl = stake if ok else -stake
        tbl_rows.append({
            "Hora":        row["time"],
            "Predicción":  row["prediction"],
            "Real":        row["actual"],
            "Correcto":    "✅ SI" if ok else "❌ NO",
            "Confianza %": f"{row['confidence']:.1f}",
            "Tier":        row["tier"],
            "En Filtro":   "SI" if row["in_filter"] else "NO",
            f"P&L (${stake:.0f})": f"${t_pnl:+,.0f}",
        })
    st.dataframe(pd.DataFrame(tbl_rows), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# STREAMLIT APP
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="BitPredict Backtester v2",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.stApp { background-color: #0d0f1a; }
section[data-testid="stSidebar"] { background-color: #161929; }
section[data-testid="stSidebar"] * { color: #e8eaf6 !important; }
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span { color: #e8eaf6 !important; font-size: 13px !important; }
section[data-testid="stSidebar"] .stRadio > label { color: #b0b3c8 !important; font-weight: 600 !important; }
section[data-testid="stSidebar"] .stSelectbox > label,
section[data-testid="stSidebar"] .stNumberInput > label,
section[data-testid="stSidebar"] .stCheckbox > label { color: #c8cbe0 !important; font-weight: 600 !important; }
section[data-testid="stSidebar"] [data-baseweb="select"] div,
section[data-testid="stSidebar"] input { color: #e8eaf6 !important; background-color: #1e2236 !important; }
section[data-testid="stSidebar"] .stDivider { border-color: #2a2d45 !important; }
section[data-testid="stSidebar"] .stButton > button {
    background-color: #7c6fff !important; color: #ffffff !important;
    border: none !important; font-weight: bold !important;
}
section[data-testid="stSidebar"] .stButton > button:hover { background-color: #9d8fff !important; }
div[data-testid="metric-container"] {
    background-color: #161929; border: 1px solid #1e2236;
    border-radius: 8px; padding: 12px 16px;
}
div[data-testid="metric-container"] label { color: #8a8daa !important; }
div[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #e8eaf6 !important; }
.stApp p, .stApp span, .stApp div { color: #e8eaf6; }
.section-hdr {
    font-size: 14px; font-weight: bold; color: #7c6fff;
    border-bottom: 1px solid #1e2236;
    padding-bottom: 6px; margin: 20px 0 12px 0;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        "<div style='font-size:20px;font-weight:bold;color:#f7931a;margin-bottom:4px;'>"
        "₿ BitPredict Backtester v2</div>"
        "<div style='font-size:11px;color:#6e7191;margin-bottom:16px;'>"
        "Predice al cierre → evalúa siguiente vela</div>",
        unsafe_allow_html=True)
    st.divider()

    mode   = st.radio("Modo", ["Por día", "Por mes", "Por año"], horizontal=True)
    today  = datetime.now()
    years  = list(range(START_YEAR, today.year + 1))
    months = list(range(1, 13))

    if mode == "Por año":
        year_sel  = st.selectbox("Año", years, index=len(years) - 1)
        month_sel = 1
        day_sel   = 1
    else:
        cy, cm = st.columns(2)
        with cy:
            year_sel  = st.selectbox("Año", years, index=len(years) - 1)
        with cm:
            month_sel = st.selectbox("Mes", months, index=today.month - 1,
                                      format_func=lambda m: f"{m:02d}")
        if mode == "Por día":
            days_in_month = cal_lib.monthrange(year_sel, month_sel)[1]
            default_day   = min(today.day - 1, days_in_month - 1)
            day_sel = st.selectbox("Día", list(range(1, days_in_month + 1)),
                                    index=max(0, default_day - 1),
                                    format_func=lambda d: f"{d:02d}")
        else:
            day_sel = 1

    st.divider()
    st.markdown("**Filtros de señal**")
    cv, cm2 = st.columns(2)
    with cv:
        flt_vol  = st.number_input("Vol ≥ (BTC)", value=200.0, step=50.0, min_value=0.0)
    with cm2:
        flt_move = st.number_input("|move| ≥ %",  value=0.1,   step=0.05, min_value=0.0)

    st.divider()
    stake = st.number_input("Stake $ / trade", value=10.0, step=5.0, min_value=1.0)

    st.divider()
    st.markdown("**Tiers activos**")
    tcols = st.columns(5)
    defaults_t = {"S": True, "A": True, "B": True, "C": False, "D": False}
    tier_sel = {}
    for i, tier in enumerate(["S","A","B","C","D"]):
        with tcols[i]:
            tier_sel[tier] = st.checkbox(tier, value=defaults_t[tier], key=f"tier_{tier}")
    selected_tiers = [t for t, v in tier_sel.items() if v]

    st.divider()
    run_btn = st.button("▶  Ejecutar Backtest", use_container_width=True, type="primary")

    # ── Google Sheets Cache ───────────────────────────────────────────────────
    st.divider()
    st.markdown("**☁ Cache Google Sheets**")

    try:
        gs_stats = get_sheets_stats()
        def _fmt_ym(v):
            return v if v else "—"
        st.markdown(
            f"<div style='font-size:11px;color:#8a8daa;line-height:1.8;'>"
            f"🕐 1m: <b style='color:#e8eaf6;'>{gs_stats['1m']['months']}</b> meses"
            f" &nbsp;|&nbsp; {_fmt_ym(gs_stats['1m'].get('min'))} → {_fmt_ym(gs_stats['1m'].get('max'))}<br>"
            f"📊 5m: <b style='color:#e8eaf6;'>{gs_stats['5m']['months']}</b> meses"
            f" &nbsp;|&nbsp; {_fmt_ym(gs_stats['5m'].get('min'))} → {_fmt_ym(gs_stats['5m'].get('max'))}"
            f"</div>", unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"Error conectando a Sheets: {e}")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    start_year_dl = st.selectbox(
        "Desde año:", list(range(START_YEAR, today.year + 1)),
        index=0, key="dl_start_year")
    hist_btn   = st.button(f"📥 Descargar Historia ({start_year_dl}–hoy)",
                            use_container_width=True, key="btn_hist")
    update_btn = st.button("🔄 Actualizar mes actual",
                            use_container_width=True, key="btn_update")

# ── Header ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='display:flex;align-items:center;gap:14px;margin-bottom:10px;'>
  <div style='width:44px;height:44px;background:#f7931a;border-radius:50%;
              display:flex;align-items:center;justify-content:center;
              font-size:22px;font-weight:bold;color:white;flex-shrink:0;'>₿</div>
  <div>
    <div style='font-size:22px;font-weight:bold;color:#e8eaf6;line-height:1.2;'>
      Backtester v2 — Cache Google Sheets</div>
    <div style='font-size:12px;color:#6e7191;'>
      Tier = vol+|move| de la vela SEÑAL &nbsp;•&nbsp;
      contexto 1m hasta cierre señal &nbsp;•&nbsp;
      velas cacheadas en Google Sheets (sin re-descargar)</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Run ──────────────────────────────────────────────────────────────────────────
filters = {
    "min_volume":     flt_vol,
    "min_abs_move":   flt_move,
    "allowed_hours":  None,
    "min_confidence": None,
}

# ── Descargar historia ────────────────────────────────────────────────────────
if hist_btn:
    status_box = st.empty()
    prog_bar   = st.progress(0.0, text="Iniciando descarga…")
    log_lines  = []

    def _hist_cb(msg, pct=0.0):
        log_lines.append(msg)
        prog_bar.progress(min(float(pct), 1.0), text=msg)
        status_box.markdown(
            "<br>".join(f"• {l}" for l in log_lines[-6:]),
            unsafe_allow_html=True)

    try:
        counts = download_history(start_year=start_year_dl, progress_cb=_hist_cb)
        prog_bar.progress(1.0, text="✅ Descarga completa")
        st.success(
            f"Historia guardada en Sheets — "
            f"1m: +{counts['1m']:,} velas  |  5m: +{counts['5m']:,} velas")
        st.rerun()
    except Exception as e:
        prog_bar.empty()
        st.error(f"Error durante la descarga: {e}")

# ── Actualizar mes actual ─────────────────────────────────────────────────────
if update_btn:
    msgs = []
    with st.spinner("Actualizando mes actual desde Binance…"):
        try:
            def _upd_cb(msg): msgs.append(msg)
            counts = update_current_month(progress_cb=_upd_cb)
            total  = counts["1m"] + counts["5m"]
            if total == 0:
                st.info("Ya tienes el mes actual al día. No hay velas nuevas.")
            else:
                st.success(
                    f"✅ Actualización completa — 1m: +{counts['1m']:,}  |  5m: +{counts['5m']:,}")
            st.rerun()
        except Exception as e:
            st.error(f"Error al actualizar: {e}")

# ── Ejecutar backtest ─────────────────────────────────────────────────────────
if run_btn:
    if mode == "Por día":
        date_str = f"{year_sel:04d}-{month_sel:02d}-{day_sel:02d}"
        with st.spinner(f"Cargando datos (Sheets / Binance) para {date_str}…"):
            try:
                result = run_backtest(date_str, filters)
                st.session_state["result"] = result
            except Exception as e:
                st.error(f"Error: {e}")

    elif mode == "Por mes":
        year_s  = f"{year_sel:04d}"
        month_s = f"{month_sel:02d}"
        prog    = st.progress(0, text="Cargando datos del mes…")
        def _prog_cb_m(done, total):
            prog.progress(done / total, text=f"Procesando día {done}/{total}…")
        try:
            result = run_backtest_month(year_s, month_s, filters, _prog_cb_m)
            st.session_state["result"] = result
            prog.empty()
        except Exception as e:
            prog.empty()
            st.error(f"Error: {e}")

    else:  # Por año
        year_s = f"{year_sel:04d}"
        prog   = st.progress(0.0, text=f"Iniciando backtest año {year_s}…")
        def _prog_cb_y(msg, pct):
            prog.progress(min(pct, 1.0), text=msg)
        try:
            result = run_backtest_year(year_s, filters, _prog_cb_y)
            st.session_state["result"] = result
            prog.progress(1.0, text=f"✅ Año {year_s} completado")
            prog.empty()
        except Exception as e:
            prog.empty()
            st.error(f"Error: {e}")

# ── Display ──────────────────────────────────────────────────────────────────────
if "result" in st.session_state:
    result  = st.session_state["result"]
    s       = result["stats"]
    df_main = result["df"]
    mode_r  = result["mode"]
    n_days  = max(1, df_main["date"].nunique())
    pnl_sel = filtered_pnl(df_main, selected_tiers, stake)
    tier_label = "+".join(selected_tiers) if selected_tiers else "ninguno"

    extra = ""
    if mode_r == "month": extra = f"  ·  {n_days} días"
    if mode_r == "year":  extra = f"  ·  {n_days} días  ·  {df_main['date'].str[:7].nunique()} meses"
    st.markdown(f"<div class='section-hdr'>📊 Backtest v2 — {result['label']}{extra}</div>",
                unsafe_allow_html=True)
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Total velas",          s["total"])
    c2.metric("Aciertos",             s["wins"])
    c3.metric("Fallos",               s["losses"])
    c4.metric("Precisión global",     f"{s['accuracy']:.1f}%")
    c5.metric("Racha max. aciertos",  s["max_win_streak"])
    c6.metric("Racha max. fallos",    s["max_loss_streak"])

    filt_acc_dyn = pnl_sel["accuracy"] if pnl_sel["total_trades"] else float("nan")
    st.markdown(
        f"<div class='section-hdr'>🎯 Rendimiento Filtrado &nbsp;·&nbsp; "
        f"Tiers: {tier_label} &nbsp;·&nbsp; Vol≥{flt_vol} &nbsp;·&nbsp; |move|≥{flt_move}%</div>",
        unsafe_allow_html=True)
    cf1,cf2,cf3,cf4,cf5,cf6,cf7 = st.columns(7)
    cf1.metric(f"Trades ({tier_label})", pnl_sel["total_trades"])
    cf2.metric("Precisión",  f"{filt_acc_dyn:.1f}%" if not np.isnan(filt_acc_dyn) else "n/a")
    cf3.metric("Trades/día", f"{pnl_sel['total_trades']/n_days:.1f}")
    cf4.metric(f"P&L (${stake:.0f}/tr)", f"${pnl_sel['total_pnl']:+,.0f}")
    cf5.metric("P&L/día",   f"${pnl_sel['pnl_per_day']:+.1f}")
    cf6.metric("ROI%",      f"{pnl_sel['roi_pct']:+.1f}%")
    cf7.metric("Wins/Losses", f"{pnl_sel['wins']}/{pnl_sel['losses']}")

    st.markdown(
        "<div class='section-hdr'>🏆 Rendimiento por Tier &nbsp;·&nbsp;"
        " S: Vol≥500 &amp; |mov|≥0.3%  A: Vol≥300 &amp; |mov|≥0.2%"
        "  B: Vol≥200 &amp; |mov|≥0.1%  C: Vol≥100  D: resto</div>",
        unsafe_allow_html=True)
    tcols5 = st.columns(5)
    for i, tier in enumerate(["S","A","B","C","D"]):
        sub   = df_main[df_main["tier"] == tier]
        n_t   = len(sub)
        w_n   = int(sub["correct"].sum()) if n_t > 0 else 0
        w_pct = w_n / n_t * 100 if n_t > 0 else 0.0
        pnl   = (w_n - (n_t - w_n)) * stake
        col   = TIER_COLORS_HEX[tier]
        acc_col = "#00d68f" if w_pct >= 55 else ("#ff4757" if w_pct < 45 else "#a0a3b1")
        pnl_col = "#00d68f" if pnl   >= 0  else "#ff4757"
        with tcols5[i]:
            st.markdown(f"""
            <div style='background:#161929;border:2px solid {col};border-radius:10px;
                        padding:14px 8px;text-align:center;'>
              <div style='color:{col};font-size:17px;font-weight:bold;'>Tier {tier}</div>
              <div style='color:{acc_col};font-size:28px;font-weight:bold;'>{w_pct:.1f}%</div>
              <div style='color:#6e7191;font-size:12px;'>{w_n}/{n_t}</div>
              <div style='color:{pnl_col};font-size:13px;font-weight:bold;'>P&L ${pnl:+.0f}</div>
              <div style='color:#6e7191;font-size:11px;'>{n_t/n_days:.1f}/día</div>
            </div>""", unsafe_allow_html=True)

    st.markdown(f"<div class='section-hdr'>💰 Simulación P&L — ${stake:.0f} por trade, apuesta fija</div>",
                unsafe_allow_html=True)
    pnl_rows_list = []
    for mt in ["S","A","B","C","D"]:
        p = simulate_pnl(df_main, stake, mt)
        pnl_rows_list.append({
            "Min Tier": f"≥ {mt}", "Trades": p["total_trades"],
            "Acc%": f"{p['accuracy']:.1f}%",
            "P&L Total": f"${p['total_pnl']:+,.0f}",
            "P&L/Día": f"${p['pnl_per_day']:+.1f}",
            "ROI%": f"{p['roi_pct']:+.1f}%",
            "Wins": p["wins"], "Losses": p["losses"],
        })
    st.dataframe(pd.DataFrame(pnl_rows_list), use_container_width=True, hide_index=True)

    st.markdown("<div class='section-hdr'>📦 Precisión por Volumen de la Vela SEÑAL (BTC)</div>",
                unsafe_allow_html=True)
    by_svol  = s["by_signal_vol"]
    non_zero = [(vb, rd) for vb, rd in by_svol.iterrows() if int(rd["total"]) > 0]
    if non_zero:
        vcols = st.columns(len(non_zero))
        for i, (vbin, rd) in enumerate(non_zero):
            av  = rd["accuracy"]
            clr = "#00d68f" if av >= 60 else ("#ff4757" if av < 50 else "#a0a3b1")
            with vcols[i]:
                st.markdown(f"""
                <div style='background:#161929;border-radius:8px;padding:10px 6px;text-align:center;'>
                  <div style='color:#6e7191;font-size:10px;font-weight:bold;'>{vbin}</div>
                  <div style='color:{clr};font-size:20px;font-weight:bold;'>{av:.0f}%</div>
                  <div style='color:#6e7191;font-size:10px;'>{int(rd["wins"])}/{int(rd["total"])}</div>
                </div>""", unsafe_allow_html=True)

    st.markdown("<div class='section-hdr'>🕐 Precisión por Hora (hora local, apertura vela TARGET)</div>",
                unsafe_allow_html=True)
    by_h   = s["by_hour"].reset_index()
    clrs_h = ["#00d68f" if a >= 60 else ("#ff4757" if a < 50 else "#a0a3b1")
               for a in by_h["accuracy"]]
    fig_h = go.Figure(go.Bar(
        x=[f"{int(h):02d}h" for h in by_h["hour"]],
        y=by_h["accuracy"], marker_color=clrs_h,
        text=[f"{a:.0f}%" for a in by_h["accuracy"]], textposition="outside",
        customdata=by_h["total"],
        hovertemplate="%{x}<br>Acc: %{y:.1f}%<br>n=%{customdata}<extra></extra>",
    ))
    fig_h.add_hline(y=50, line_dash="dash", line_color="#6e7191", line_width=1)
    fig_h.update_layout(
        plot_bgcolor="#0d0f1a", paper_bgcolor="#0d0f1a", font_color="#e8eaf6",
        height=300, showlegend=False,
        margin=dict(l=20, r=20, t=30, b=20),
        yaxis=dict(range=[0, 110], gridcolor="#1e2236"),
        xaxis=dict(gridcolor="#1e2236"),
    )
    st.plotly_chart(fig_h, use_container_width=True)

    if s["minute_acc"]:
        st.markdown("<div class='section-hdr'>⏱ Precisión por Minuto intra-vela TARGET</div>",
                    unsafe_allow_html=True)
        mcols = st.columns(5)
        for mn, info in sorted(s["minute_acc"].items()):
            av      = info["accuracy"]
            clr     = "#00d68f" if av >= 60 else ("#ff4757" if av < 50 else "#a0a3b1")
            is_best = mn == s["best_minute"]
            bdr     = "2px solid #ffd700" if is_best else "1px solid #1e2236"
            with mcols[mn - 1]:
                st.markdown(f"""
                <div style='background:#161929;border:{bdr};border-radius:8px;
                            padding:12px 6px;text-align:center;'>
                  <div style='color:{"#ffd700" if is_best else "#6e7191"};font-size:11px;font-weight:bold;'>
                    {"★ " if is_best else ""}min {mn}</div>
                  <div style='color:{clr};font-size:22px;font-weight:bold;'>{av:.1f}%</div>
                  <div style='color:#6e7191;font-size:10px;'>{info["correct"]}/{info["total"]}</div>
                </div>""", unsafe_allow_html=True)

    # ── Calendario mensual ──────────────────────────────────────────────────
    if mode_r == "month":
        parts  = result["label"].split("-")
        yr_cal = int(parts[0]); mo_cal = int(parts[1])

        filt_df_cal = (df_main[df_main["tier"].isin(selected_tiers)].copy()
                       if selected_tiers else df_main.iloc[0:0].copy())
        by_date_cal: dict = {}
        for ds, grp in filt_df_cal.groupby("date"):
            w = int(grp["correct"].sum()); tot = len(grp)
            by_date_cal[ds] = {"wins": w, "losses": tot-w, "total": tot,
                                "pnl": (w-(tot-w))*stake}

        if by_date_cal:
            all_pnl_c    = [v["pnl"] for v in by_date_cal.values()]
            total_pnl_c  = sum(all_pnl_c)
            days_gain_c  = sum(1 for p in all_pnl_c if p > 0)
            days_loss_c  = sum(1 for p in all_pnl_c if p < 0)
            best_date_c  = max(by_date_cal, key=lambda d: by_date_cal[d]["pnl"])
            worst_date_c = min(by_date_cal, key=lambda d: by_date_cal[d]["pnl"])
            best_pnl_c   = by_date_cal[best_date_c]["pnl"]
            worst_pnl_c  = by_date_cal[worst_date_c]["pnl"]
        else:
            total_pnl_c = days_gain_c = days_loss_c = 0
            best_date_c = worst_date_c = None
            best_pnl_c  = worst_pnl_c  = 0.0

        st.markdown(
            f"<div class='section-hdr'>📅 Calendario P&L — "
            f"${stake:.0f}/trade · Tiers: {tier_label}</div>",
            unsafe_allow_html=True)
        cc1,cc2,cc3,cc4,cc5 = st.columns(5)
        cc1.metric("P&L Total Mes",  f"${total_pnl_c:+,.0f}")
        cc2.metric("Días ganancia",  days_gain_c)
        cc3.metric("Días pérdida",   days_loss_c)
        cc4.metric("Mejor día",
                   (f"Día {best_date_c.split('-')[2]} (${best_pnl_c:+,.0f})"
                    if best_date_c else "—"))
        cc5.metric("Peor día",
                   (f"Día {worst_date_c.split('-')[2]} (${worst_pnl_c:+,.0f})"
                    if worst_date_c else "—"))

        lim_col1, lim_col2 = st.columns([1, 3])
        with lim_col1:
            use_limit = st.checkbox("🎯 Filtro límite diario ±$", value=False,
                                     key="use_limit_filter")
        with lim_col2:
            daily_limit = st.number_input(
                "Límite ±$", value=500.0, step=100.0, min_value=1.0,
                key="daily_limit_val",
                label_visibility="collapsed" if not use_limit else "visible",
                disabled=not use_limit)

        limit_results: dict = {}
        if use_limit and selected_tiers:
            for ds, grp in filt_df_cal.groupby("date"):
                day_sorted = grp.sort_values("time").reset_index(drop=True)
                limit_results[ds] = find_daily_limit_hit(day_sorted, stake, daily_limit)

            hit_up   = sum(1 for v in limit_results.values() if v["hit"] == "UP")
            hit_down = sum(1 for v in limit_results.values() if v["hit"] == "DOWN")
            hit_none = sum(1 for v in limit_results.values() if v["hit"] is None)
            st.markdown(
                f"<div style='background:#1e2236;border-radius:8px;padding:10px 16px;"
                f"margin:8px 0;font-size:13px;'>"
                f"<span style='color:#00d68f;font-weight:bold;'>🟢 +${daily_limit:,.0f} primero: {hit_up} días</span>"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;"
                f"<span style='color:#ff4757;font-weight:bold;'>🔴 -${daily_limit:,.0f} primero: {hit_down} días</span>"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;"
                f"<span style='color:#6e7191;'>⚪ Sin límite alcanzado: {hit_none} días</span>"
                f"</div>", unsafe_allow_html=True)

        day_names_es = ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"]
        month_weeks  = cal_lib.monthcalendar(yr_cal, mo_cal)
        cal_html = ("<div style='display:grid;grid-template-columns:repeat(7,1fr);"
                    "gap:6px;margin-top:10px;'>")
        for dn in day_names_es:
            cal_html += (f"<div style='text-align:center;color:#6e7191;"
                         f"font-size:12px;font-weight:bold;padding:4px;'>{dn}</div>")
        for week in month_weeks:
            for day in week:
                if day == 0:
                    cal_html += "<div></div>"; continue
                ds   = f"{yr_cal:04d}-{mo_cal:02d}-{day:02d}"
                info = by_date_cal.get(ds)
                bdr  = ("#00d68f" if ds == best_date_c else
                        ("#ff4757" if ds == worst_date_c else "#1e2236"))
                if info:
                    pv  = info["pnl"]
                    pc  = "#00d68f" if pv > 0 else ("#ff4757" if pv < 0 else "#a0a3b1")
                    lim_html = ""
                    if use_limit and ds in limit_results:
                        lr = limit_results[ds]
                        if lr["hit"] == "UP":
                            lim_html = (f"<div style='background:#0a2e1e;border-radius:4px;padding:3px 4px;margin:3px 0;'>"
                                        f"<div style='color:#00d68f;font-size:11px;font-weight:bold;'>🟢 +${daily_limit:,.0f}</div>"
                                        f"<div style='color:#6e7191;font-size:9px;'>op #{lr['op']} · {lr['time']}</div></div>")
                            bdr = "#00d68f"
                        elif lr["hit"] == "DOWN":
                            lim_html = (f"<div style='background:#2e0a0a;border-radius:4px;padding:3px 4px;margin:3px 0;'>"
                                        f"<div style='color:#ff4757;font-size:11px;font-weight:bold;'>🔴 -${daily_limit:,.0f}</div>"
                                        f"<div style='color:#6e7191;font-size:9px;'>op #{lr['op']} · {lr['time']}</div></div>")
                            bdr = "#ff4757"
                        else:
                            lim_html = ("<div style='background:#1e2236;border-radius:4px;padding:3px 4px;margin:3px 0;'>"
                                        "<div style='color:#6e7191;font-size:10px;'>⚪ Sin límite</div></div>")
                    cal_html += (
                        f"<div style='background:#161929;border:2px solid {bdr};"
                        f"border-radius:8px;padding:8px 4px;text-align:center;"
                        f"min-height:{'105' if use_limit else '78'}px;'>"
                        f"<div style='color:#6e7191;font-size:10px;text-align:right;'>{day:02d}</div>"
                        f"<div style='color:{pc};font-size:14px;font-weight:bold;'>${pv:+,.0f}</div>"
                        f"<div style='color:#6e7191;font-size:9px;'>{info['wins']}W/{info['losses']}L·{info['total']}tr</div>"
                        f"{lim_html}</div>")
                else:
                    cal_html += (
                        f"<div style='background:#0d0f1a;border:1px solid #1e2236;"
                        f"border-radius:8px;padding:8px 4px;text-align:center;"
                        f"min-height:78px;opacity:0.35;'>"
                        f"<div style='color:#6e7191;font-size:10px;text-align:right;'>{day:02d}</div>"
                        f"<div style='color:#6e7191;font-size:13px;'>—</div></div>")
        cal_html += "</div>"
        st.markdown(cal_html, unsafe_allow_html=True)

        st.markdown("<div class='section-hdr'>🔍 Detalle de Día</div>", unsafe_allow_html=True)
        available_days = sorted(by_date_cal.keys())
        if available_days:
            sel_day = st.selectbox(
                "Seleccionar día:", ["— elige un día —"] + available_days,
                key="day_detail_sel")
            if sel_day != "— elige un día —":
                render_day_detail(df_main, sel_day, selected_tiers, stake)

    # ── Calendario anual ────────────────────────────────────────────────────
    if mode_r == "year":
        MONTH_NAMES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                       "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        MONTH_SHORT = ["Ene","Feb","Mar","Abr","May","Jun",
                       "Jul","Ago","Sep","Oct","Nov","Dic"]
        yr_y = result["label"]

        filt_df_yr = (df_main[df_main["tier"].isin(selected_tiers)].copy()
                      if selected_tiers else df_main.iloc[0:0].copy())
        by_month: dict = {}
        for mo_key, grp in filt_df_yr.groupby(filt_df_yr["date"].str[:7]):
            w = int(grp["correct"].sum()); t = len(grp)
            by_month[mo_key] = {"wins": w, "losses": t-w, "total": t,
                                 "pnl": (w-(t-w))*stake, "acc": w/t*100 if t>0 else 0}

        if by_month:
            total_pnl_y = sum(v["pnl"] for v in by_month.values())
            mo_gain_y   = sum(1 for v in by_month.values() if v["pnl"] > 0)
            mo_loss_y   = sum(1 for v in by_month.values() if v["pnl"] < 0)
            best_mo_y   = max(by_month, key=lambda k: by_month[k]["pnl"])
            worst_mo_y  = min(by_month, key=lambda k: by_month[k]["pnl"])
        else:
            total_pnl_y = mo_gain_y = mo_loss_y = 0
            best_mo_y = worst_mo_y = None

        st.markdown(
            f"<div class='section-hdr'>📅 Calendario Anual {yr_y} — "
            f"${stake:.0f}/trade · Tiers: {tier_label}</div>",
            unsafe_allow_html=True)
        ya1,ya2,ya3,ya4,ya5 = st.columns(5)
        ya1.metric("P&L Total Año",  f"${total_pnl_y:+,.0f}")
        ya2.metric("Meses ganancia", mo_gain_y)
        ya3.metric("Meses pérdida",  mo_loss_y)
        ya4.metric("Mejor mes",
                   (f"{MONTH_SHORT[int(best_mo_y.split('-')[1])-1]} "
                    f"(${by_month[best_mo_y]['pnl']:+,.0f})") if best_mo_y else "—")
        ya5.metric("Peor mes",
                   (f"{MONTH_SHORT[int(worst_mo_y.split('-')[1])-1]} "
                    f"(${by_month[worst_mo_y]['pnl']:+,.0f})") if worst_mo_y else "—")

        for row_i in range(4):
            cols_y = st.columns(3)
            for col_i in range(3):
                m      = row_i * 3 + col_i + 1
                mo_key = f"{yr_y}-{m:02d}"
                info   = by_month.get(mo_key)
                with cols_y[col_i]:
                    if info:
                        pv  = info["pnl"]
                        pc  = "#00d68f" if pv > 0 else "#ff4757"
                        acc = info["acc"]
                        ac  = "#00d68f" if acc >= 55 else ("#ff4757" if acc < 50 else "#a0a3b1")
                        st.markdown(f"""
                        <div style='background:#161929;border:2px solid {pc};
                                    border-radius:10px;padding:14px;text-align:center;
                                    margin-bottom:8px;'>
                          <div style='color:#e8eaf6;font-size:15px;font-weight:bold;'>{MONTH_NAMES[m-1]}</div>
                          <div style='color:{pc};font-size:26px;font-weight:bold;'>${pv:+,.0f}</div>
                          <div style='color:{ac};font-size:13px;'>{acc:.1f}%</div>
                          <div style='color:#6e7191;font-size:11px;'>{info["wins"]}W / {info["losses"]}L · {info["total"]} trades</div>
                        </div>""", unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div style='background:#0d0f1a;border:1px solid #1e2236;
                                    border-radius:10px;padding:14px;text-align:center;
                                    opacity:0.4;margin-bottom:8px;'>
                          <div style='color:#6e7191;font-size:15px;'>{MONTH_NAMES[m-1]}</div>
                          <div style='color:#6e7191;font-size:22px;'>—</div>
                        </div>""", unsafe_allow_html=True)

        if by_month:
            st.markdown("<div class='section-hdr'>🔍 Detalle de Mes</div>",
                        unsafe_allow_html=True)
            month_options = sorted(by_month.keys())
            sel_mo = st.selectbox("Seleccionar mes para detalle:",
                                  ["— elige un mes —"] + month_options,
                                  key="year_month_sel")
            if sel_mo != "— elige un mes —":
                mo_df    = df_main[df_main["date"].str[:7] == sel_mo]
                filt_mo  = (mo_df[mo_df["tier"].isin(selected_tiers)]
                            if selected_tiers else mo_df.iloc[0:0])
                by_date_mo: dict = {}
                for ds, grp in filt_mo.groupby("date"):
                    w = int(grp["correct"].sum()); t = len(grp)
                    by_date_mo[ds] = {"wins": w, "losses": t-w, "total": t,
                                      "pnl": (w-(t-w))*stake}

                yr_mo, mo_mo  = int(sel_mo.split("-")[0]), int(sel_mo.split("-")[1])
                month_weeks_d = cal_lib.monthcalendar(yr_mo, mo_mo)
                day_names_s   = ["L","M","X","J","V","S","D"]
                cal_mo_html   = ("<div style='display:grid;grid-template-columns:repeat(7,1fr);"
                                 "gap:4px;margin-top:8px;'>")
                for dn in day_names_s:
                    cal_mo_html += (f"<div style='text-align:center;color:#6e7191;"
                                    f"font-size:11px;font-weight:bold;padding:2px;'>{dn}</div>")
                for week in month_weeks_d:
                    for day in week:
                        if day == 0:
                            cal_mo_html += "<div></div>"; continue
                        ds   = f"{yr_mo:04d}-{mo_mo:02d}-{day:02d}"
                        info = by_date_mo.get(ds)
                        if info:
                            pv = info["pnl"]
                            pc = "#00d68f" if pv > 0 else "#ff4757"
                            cal_mo_html += (
                                f"<div style='background:#161929;border:1px solid {pc};"
                                f"border-radius:6px;padding:6px 2px;text-align:center;'>"
                                f"<div style='color:#6e7191;font-size:9px;text-align:right;'>{day}</div>"
                                f"<div style='color:{pc};font-size:12px;font-weight:bold;'>${pv:+,.0f}</div>"
                                f"<div style='color:#6e7191;font-size:8px;'>{info['wins']}W/{info['losses']}L</div></div>")
                        else:
                            cal_mo_html += (
                                f"<div style='background:#0d0f1a;border:1px solid #1e2236;"
                                f"border-radius:6px;padding:6px 2px;text-align:center;opacity:0.3;'>"
                                f"<div style='color:#6e7191;font-size:9px;text-align:right;'>{day}</div>"
                                f"<div style='color:#6e7191;font-size:11px;'>—</div></div>")
                cal_mo_html += "</div>"
                st.markdown(cal_mo_html, unsafe_allow_html=True)

    # ── Detalle automático (modo día) ─────────────────────────────────────────
    if mode_r == "day":
        render_day_detail(df_main, result["label"], selected_tiers, stake)

    # ── Señales ──────────────────────────────────────────────────────────────
    if s["signal_bias"]:
        st.markdown("<div class='section-hdr'>📡 Distribución de Señales</div>",
                    unsafe_allow_html=True)
        sig_rows = [
            {"Señal": sn, "UP": v["UP"], "DOWN": v["DOWN"],
             "NEUTRAL": v["NEUTRAL"], "Total": v["total"]}
            for sn, v in sorted(s["signal_bias"].items())
        ]
        st.dataframe(pd.DataFrame(sig_rows), use_container_width=True, hide_index=True)

    with st.expander("📋 Ver todas las predicciones"):
        show_cols = ["date","time","tier","prediction","actual","correct",
                     "confidence","signal_volume","signal_move","pct_move"]
        st.dataframe(df_main[show_cols], use_container_width=True, hide_index=True)

    st.markdown("<div class='section-hdr'>⬇ Exportar resultados</div>",
                unsafe_allow_html=True)
    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button(
            "📂 Descargar Excel Completo (12 hojas)",
            data=save_excel_bytes(result, stake),
            file_name=f"backtest_v2_{result['label']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with dl2:
        st.download_button(
            "📋 Descargar Excel Detalle (CST)",
            data=save_excel_detail_bytes(result),
            file_name=f"detalle_v2_{result['label']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
