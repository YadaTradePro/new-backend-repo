# services/golden_key_service.py
from extensions import db
from models import (
    ComprehensiveSymbolData,
    HistoricalData,
    TechnicalIndicatorData,
    GoldenKeyResult,
    AggregatedPerformance,
)
# Try to import SignalsPerformance if exists (used in some win-rate flows)
try:
    from models import SignalsPerformance
except Exception:
    SignalsPerformance = None

from flask import current_app
import pandas as pd
import logging
from datetime import datetime, timedelta, date
import jdatetime  # Ensure jdatetime is installed for Jalali<->Gregorian conversion
import numpy as np
import json
from sqlalchemy import func

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Import utility functions that we still want to use (but NOT the indicator functions
# that caused lru_cache issues). If missing, we will handle gracefully.
from services.utils import (
    get_today_jdate_str,
    normalize_value,
)
# optional utilities
try:
    from services.utils import calculate_smart_money_flow, check_candlestick_patterns
except Exception:
    calculate_smart_money_flow = None
    check_candlestick_patterns = None
    logger.debug("Optional utility functions calculate_smart_money_flow or check_candlestick_patterns not found in services.utils")

# ---------------------------------------------------------
# Local indicator implementations (to avoid lru_cache / hash issues)
# These functions return pandas.Series aligned with the input index
# ---------------------------------------------------------

def _to_series(x):
    """Helper: ensure x is a pandas Series."""
    if isinstance(x, pd.Series):
        return x.astype(float)
    return pd.Series(x).astype(float)


def compute_sma(series, window=20):
    s = _to_series(series)
    if s.empty or len(s) < window:
        return pd.Series([np.nan] * len(s), index=s.index)
    return s.rolling(window=window, min_periods=1).mean().reindex(s.index)


def compute_volume_ma(series, window=5):
    return compute_sma(series, window=window)


def compute_rsi(series, window=14):
    s = _to_series(series)
    if s.empty:
        return pd.Series([], dtype=float, index=s.index)
    # Standard Wilder's RSI (EMA of gain/loss)
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    # Use exponential weighted mean (alpha = 1/window)
    roll_up = up.ewm(span=window, adjust=False).mean()
    roll_down = down.ewm(span=window, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.fillna(0)  # fill temporarily; we'll replace with NaN where appropriate
    # For initial periods where roll_down==0 (no losses), RSI is 100; but zeroing is acceptable baseline
    rsi[(roll_up.isna()) & (roll_down.isna())] = np.nan
    return rsi.reindex(s.index)


def compute_macd(series, fast=12, slow=26, signal=9):
    s = _to_series(series)
    if s.empty:
        return (pd.Series([], dtype=float, index=s.index),
                pd.Series([], dtype=float, index=s.index),
                pd.Series([], dtype=float, index=s.index))
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line.reindex(s.index), signal_line.reindex(s.index), histogram.reindex(s.index)


def compute_atr(high, low, close, window=14):
    h = _to_series(high)
    l = _to_series(low)
    c = _to_series(close)
    if h.empty or l.empty or c.empty:
        return pd.Series([], dtype=float, index=c.index)
    tr1 = h - l
    tr2 = (h - c.shift()).abs()
    tr3 = (l - c.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(span=window, adjust=False).mean()
    return atr.reindex(c.index)


# ---------------------------------------------------------
# Helper filter functions (unchanged logic but defensive)
# ---------------------------------------------------------

def is_resistance_breakout(df_high, current_close, days_window=20):
    try:
        if len(df_high) < days_window:
            logger.debug(f"is_resistance_breakout: Not enough data ({len(df_high)} < {days_window})")
            return False
        recent_highs = df_high.iloc[-days_window-1:-1].max() if days_window + 1 <= len(df_high) else df_high.iloc[:-1].max()
        result = current_close > recent_highs if pd.notna(current_close) and pd.notna(recent_highs) else False
        logger.debug(f"Resistance Breakout: current_close={current_close}, recent_highs={recent_highs}, Result={result}")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_resistance_breakout error: {e}", exc_info=True)
        return False


def is_support_breakdown(df_low, current_close, days_window=20):
    try:
        if len(df_low) < days_window:
            logger.debug(f"is_support_breakdown: Not enough data ({len(df_low)} < {days_window})")
            return False
        recent_lows = df_low.iloc[-days_window-1:-1].min() if days_window + 1 <= len(df_low) else df_low.iloc[:-1].min()
        result = current_close < recent_lows if pd.notna(current_close) and pd.notna(recent_lows) else False
        logger.debug(f"Support Breakdown: current_close={current_close}, recent_lows={recent_lows}, Result={result}")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_support_breakdown error: {e}", exc_info=True)
        return False


def is_high_volume(current_volume, avg_volume, multiplier=1.5):
    try:
        result = current_volume > (avg_volume * multiplier) if pd.notna(current_volume) and pd.notna(avg_volume) else False
        logger.debug(f"High Volume: current_volume={current_volume}, avg_volume={avg_volume}, Result={result}")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_high_volume error: {e}", exc_info=True)
        return False


def is_rsi_oversold(rsi_value, threshold=30):
    try:
        result = rsi_value < threshold if pd.notna(rsi_value) else False
        logger.debug(f"RSI Oversold: rsi_value={rsi_value}, Result={result}")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_rsi_oversold error: {e}", exc_info=True)
        return False


def is_rsi_overbought(rsi_value, threshold=70):
    try:
        result = rsi_value > threshold if pd.notna(rsi_value) else False
        logger.debug(f"RSI Overbought: rsi_value={rsi_value}, Result={result}")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_rsi_overbought error: {e}", exc_info=True)
        return False


def is_macd_buy_signal(macd_line, signal_line):
    try:
        if macd_line.empty or signal_line.empty or len(macd_line) < 2 or len(signal_line) < 2:
            logger.debug("MACD Buy Signal: Not enough data for cross-over check.")
            return False
        current_macd = macd_line.iloc[-1]
        current_signal = signal_line.iloc[-1]
        prev_macd = macd_line.iloc[-2]
        prev_signal = signal_line.iloc[-2]
        if pd.isna(current_macd) or pd.isna(current_signal) or pd.isna(prev_macd) or pd.isna(prev_signal):
            logger.debug("MACD Buy Signal: NaN values encountered.")
            return False
        result = (current_macd > current_signal) and (prev_macd <= prev_signal)
        logger.debug(f"MACD Buy Signal: {result} (curr_macd={current_macd}, curr_sig={current_signal})")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_macd_buy_signal error: {e}", exc_info=True)
        return False


def is_macd_sell_signal(macd_line, signal_line):
    try:
        if macd_line.empty or signal_line.empty or len(macd_line) < 2 or len(signal_line) < 2:
            logger.debug("MACD Sell Signal: Not enough data for cross-over check.")
            return False
        current_macd = macd_line.iloc[-1]
        current_signal = signal_line.iloc[-1]
        prev_macd = macd_line.iloc[-2]
        prev_signal = signal_line.iloc[-2]
        if pd.isna(current_macd) or pd.isna(current_signal) or pd.isna(prev_macd) or pd.isna(prev_signal):
            logger.debug("MACD Sell Signal: NaN values encountered.")
            return False
        result = (current_macd < current_signal) and (prev_macd >= prev_signal)
        logger.debug(f"MACD Sell Signal: {result} (curr_macd={current_macd}, curr_sig={current_signal})")
        return bool(result)
    except Exception as e:
        logger.warning(f"is_macd_sell_signal error: {e}", exc_info=True)
        return False


# ---------------------------------------------------------
# Complex pattern checkers (use pandas series internally)
# ---------------------------------------------------------
def _check_double_bottom_pattern(close_prices_array, high_prices_array, volume_array):
    logger.debug("Checking for Double Bottom pattern (simplified internal implementation).")
    try:
        close_series = pd.Series(close_prices_array).dropna()
        high_series = pd.Series(high_prices_array).dropna()
        volume_series = pd.Series(volume_array).dropna()

        if len(close_series) < 40 or close_series.empty or high_series.empty or volume_series.empty:
            logger.debug("Double Bottom: insufficient data")
            return False

        recent_closes = close_series.iloc[-40:]
        first_half = recent_closes.iloc[:20]
        second_half = recent_closes.iloc[20:]

        if first_half.empty or second_half.empty:
            return False

        bottom1_price = first_half.min()
        bottom2_price = second_half.min()

        if not (0.95 * bottom1_price <= bottom2_price <= 1.05 * bottom1_price):
            logger.debug("Double Bottom: bottoms not similar")
            return False

        # get neckline as max between bottoms
        bottom1_idx = first_half.idxmin()
        bottom2_idx = second_half.idxmin()
        if bottom1_idx > bottom2_idx:
            bottom1_idx, bottom2_idx = bottom2_idx, bottom1_idx

        neckline_segment = close_series.loc[bottom1_idx:bottom2_idx]
        if neckline_segment.empty:
            return False
        neckline_price = neckline_segment.max()

        current_close = close_series.iloc[-1]
        current_volume = volume_series.iloc[-1]
        if len(volume_series) < 10:
            return False
        avg_volume_recent = volume_series.iloc[-10:].mean()

        if pd.isna(current_close) or pd.isna(neckline_price) or pd.isna(current_volume) or pd.isna(avg_volume_recent):
            return False

        if current_close > neckline_price and current_volume > (avg_volume_recent * 1.5):
            logger.debug(f"Double Bottom detected (neckline={neckline_price:.2f})")
            return True
        return False
    except Exception as e:
        logger.warning(f"_check_double_bottom_pattern error: {e}", exc_info=True)
        return False


def _check_descending_trendline_breakout(close_prices_array, high_prices_array, low_prices_array, volume_array):
    logger.debug("Checking for Descending Trendline Breakout (simplified internal implementation).")
    try:
        close_series = pd.Series(close_prices_array).dropna()
        high_series = pd.Series(high_prices_array).dropna()
        low_series = pd.Series(low_prices_array).dropna()
        volume_series = pd.Series(volume_array).dropna()

        if len(close_series) < 30 or close_series.empty or high_series.empty or low_series.empty or volume_series.empty:
            return False

        recent_highs = high_series.iloc[-30:]
        peaks = recent_highs[recent_highs == recent_highs.rolling(window=3, center=True).max()].dropna()

        descending_peaks = []
        if len(peaks) >= 2:
            # collect descending peaks (index, price)
            for i in range(len(peaks) - 1):
                if peaks.iloc[i] > peaks.iloc[i + 1]:
                    descending_peaks.append((peaks.index[i], peaks.iloc[i]))
                descending_peaks.append((peaks.index[i + 1], peaks.iloc[i + 1]))

            descending_peaks = sorted(list(set(descending_peaks)), key=lambda x: x[0])

            if len(descending_peaks) >= 2:
                peak1_idx, peak1_price = descending_peaks[-2]
                peak2_idx, peak2_price = descending_peaks[-1]
                if peak2_idx <= peak1_idx:
                    return False

                slope = (peak2_price - peak1_price) / (peak2_idx - peak1_idx)
                if slope >= 0:
                    return False

                current_idx = close_series.index[-1]
                projected_trendline_value = peak2_price + slope * (current_idx - peak2_idx)

                current_close = close_series.iloc[-1]
                current_open = close_series.iloc[-1]
                current_volume = volume_series.iloc[-1]

                if len(volume_series) < 10:
                    return False
                avg_volume_recent = volume_series.iloc[-10:].mean()

                if pd.isna(current_close) or pd.isna(projected_trendline_value) or pd.isna(current_open) or pd.isna(current_volume) or pd.isna(avg_volume_recent):
                    return False

                if current_close > projected_trendline_value and current_close > current_open and abs(current_close - current_open) > (high_series.iloc[-1] - low_series.iloc[-1]) * 0.5 and current_volume > (avg_volume_recent * 1.5):
                    logger.debug("Descending trendline breakout detected")
                    return True
        return False
    except Exception as e:
        logger.warning(f"_check_descending_trendline_breakout error: {e}", exc_info=True)
        return False


def _check_monthly_volume_vs_six_month_avg(volume_array, today_candle_data):
    logger.debug("Checking for Monthly Volume vs. Six Month Avg (simplified internal implementation).")
    try:
        volume_series = pd.Series(volume_array).dropna()
        if len(volume_series) < 120 or volume_series.empty:
            return False
        avg_volume_1_month = volume_series.iloc[-20:].mean()
        avg_volume_6_month = volume_series.iloc[-120:].mean()

        open_t = today_candle_data.get('open')
        close_t = today_candle_data.get('close')
        high_t = today_candle_data.get('high')
        low_t = today_candle_data.get('low')

        is_strong_bullish_candle = False
        if pd.notna(open_t) and pd.notna(close_t) and pd.notna(high_t) and pd.notna(low_t):
            body_size = close_t - open_t
            total_range = high_t - low_t
            if total_range > 0 and body_size > 0.5 * total_range and close_t > open_t:
                is_strong_bullish_candle = True

        if pd.isna(avg_volume_1_month) or pd.isna(avg_volume_6_month):
            return False

        if avg_volume_1_month > avg_volume_6_month * 1.2 and is_strong_bullish_candle:
            logger.debug("Monthly volume higher than 6-month avg and strong bullish candle")
            return True
        return False
    except Exception as e:
        logger.warning(f"_check_monthly_volume_vs_six_month_avg error: {e}", exc_info=True)
        return False


# ---------------------------------------------------------
# Main Golden Key processing
# ---------------------------------------------------------

def run_golden_key_analysis_and_save(top_n_symbols=8):
    logger.info("Starting Golden Key analysis and saving process.")
    today_jdate_str = get_today_jdate_str()
    all_symbols = ComprehensiveSymbolData.query.all()

    if not all_symbols:
        logger.warning("No symbols found in ComprehensiveSymbolData. Cannot run Golden Key analysis.")
        return False, "No symbols found to analyze."

    # fund keywords to skip
    fund_keywords = [
        "صندوق", "سرمایه گذاری", "اعتبار", "آتیه", "یکتا", "بورس", "دارایی",
        "گیلان", "اختصاصی", "تدبیر", "دماوند", "سپهر", "سودمند", "کامیاب",
        "آشنا", "ماهور", "ح", "پ", "ت"
    ]

    # Optionally delete previous fund/rights results for latest date
    fund_symbol_ids_to_delete = []
    for symbol_data in all_symbols:
        symbol_name = symbol_data.symbol_name
        if any(keyword in symbol_name for keyword in fund_keywords):
            fund_symbol_ids_to_delete.append(symbol_data.symbol_id)

    if fund_symbol_ids_to_delete:
        try:
            latest_date_result = db.session.query(func.max(GoldenKeyResult.jdate)).scalar()
            if latest_date_result:
                deleted_count = GoldenKeyResult.query.filter(
                    GoldenKeyResult.symbol_id.in_(fund_symbol_ids_to_delete),
                    GoldenKeyResult.jdate == latest_date_result
                ).delete(synchronize_session=False)
                db.session.commit()
                logger.info(f"Deleted {deleted_count} Golden Key results for funds/rights on {latest_date_result}.")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error deleting previous fund results: {e}", exc_info=True)

    current_day_results = []

    for symbol_data in all_symbols:
        symbol_id = symbol_data.symbol_id
        symbol_name = symbol_data.symbol_name

        if any(keyword in symbol_name for keyword in fund_keywords):
            logger.debug(f"Skipping {symbol_name}: identified as fund / right.")
            continue

        logger.debug(f"Analyzing symbol: {symbol_name} ({symbol_id})")

        historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.jdate.asc()).all()
        if not historical_records or len(historical_records) < 120:
            logger.debug(f"Skipping {symbol_name}: insufficient historical data ({len(historical_records) if historical_records else 0}).")
            continue

        df = pd.DataFrame([r.__dict__ for r in historical_records])
        if df.empty or 'jdate' not in df.columns:
            logger.debug(f"Skipping {symbol_name}: dataframe empty or missing jdate.")
            continue

        # convert jdate (assumed 'YYYY-MM-DD') to gregorian datetime index
        try:
            df['gregorian_date'] = df['jdate'].apply(lambda x: jdatetime.datetime.strptime(x, '%Y-%m-%d').togregorian())
            df = df.set_index(pd.to_datetime(df['gregorian_date']))
            df = df.sort_index()
        except Exception as e:
            logger.error(f"Date conversion error for {symbol_name}: {e}", exc_info=True)
            continue

        # convert numeric columns safely
        columns_to_convert = [
            'open', 'high', 'low', 'close', 'final', 'yesterday_price', 'volume', 'value', 'num_trades',
            'plc', 'plp', 'pcc', 'pcp', 'mv',
            'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n',
            'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume',
            'zd1', 'qd1', 'pd1', 'zo1', 'qo1', 'po1',
            'zd2', 'qd2', 'pd2', 'zo2', 'qo2', 'po2',
            'zd3', 'qd3', 'pd3', 'zo3', 'qo3', 'po3',
            'zd4', 'qd4', 'pd4', 'zo4', 'qo4', 'po4',
            'zd5', 'qd5', 'pd5', 'zo5', 'qo5', 'po5'
        ]
        for col in columns_to_convert:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                # not fatal, just warn
                logger.debug(f"Column {col} not present for {symbol_name} (may be optional).")

        critical_ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
        if any(col not in df.columns for col in critical_ohlcv_cols):
            missing = [c for c in critical_ohlcv_cols if c not in df.columns]
            logger.error(f"Missing critical OHLCV columns for {symbol_name}: {missing}")
            continue

        # drop rows with NaN in critical columns
        df.dropna(subset=critical_ohlcv_cols, inplace=True)
        if df.empty or len(df) < 120:
            logger.debug(f"After cleaning NaNs {symbol_name} has insufficient rows ({len(df)}). Skipping.")
            continue

        current_close = df['close'].iloc[-1]
        current_volume = df['volume'].iloc[-1]

        if len(df) < 2:
            today_candle_data = {}
            yesterday_candle_data = {}
        else:
            today_candle_data = {
                'open': df['open'].iloc[-1],
                'high': df['high'].iloc[-1],
                'low': df['low'].iloc[-1],
                'close': df['close'].iloc[-1],
                'volume': df['volume'].iloc[-1]
            }
            yesterday_candle_data = {
                'open': df['open'].iloc[-2],
                'high': df['high'].iloc[-2],
                'low': df['low'].iloc[-2],
                'close': df['close'].iloc[-2],
                'volume': df['volume'].iloc[-2]
            }


        hist_df_for_patterns = pd.DataFrame({
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["close"],
            "volume": df["volume"],
        })    

        pe_ratio_val = getattr(symbol_data, 'pe_ratio', np.nan)
        eps_val = getattr(symbol_data, 'eps', np.nan)

        # ---------------------------
        # Calculate indicators (local implementations)
        # ---------------------------
        try:
            rsi_series = compute_rsi(df['close'])
            macd_line, signal_line, macd_hist = compute_macd(df['close'])
            sma_20_series = compute_sma(df['close'], window=20)
            sma_50_series = compute_sma(df['close'], window=50)
            volume_ma_5_series = compute_volume_ma(df['volume'], window=5)
            volume_ma_1_month_series = compute_volume_ma(df['volume'], window=20)
            volume_ma_6_month_series = compute_volume_ma(df['volume'], window=120)
            atr_series = compute_atr(df['high'], df['low'], df['close'], window=14)

            # align indexes if necessary (ensure same length)
            for s in [rsi_series, macd_line, signal_line, sma_20_series, sma_50_series,
                      volume_ma_5_series, volume_ma_1_month_series, volume_ma_6_month_series, atr_series]:
                if len(s) != len(df):
                    # reindex to df.index with NaNs where missing
                    s.index = s.index if len(s.index) == len(df.index) else pd.RangeIndex(start=0, stop=len(s))
                    s = s.reindex(df.index)
            # (we keep s as local; we will reference our named variables above)
        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol_name}: {e}", exc_info=True)
            continue

        # Smart-money flow calculation (optional)
        latest_individual_buy_power = np.nan
        if calculate_smart_money_flow:
            try:
                smf_df = calculate_smart_money_flow(df)
                if not smf_df.empty and 'individual_buy_power' in smf_df.columns:
                    latest_individual_buy_power = smf_df['individual_buy_power'].iloc[-1]
                else:
                    logger.debug(f"calculate_smart_money_flow returned empty or missing column for {symbol_name}.")
            except Exception as e:
                logger.warning(f"calculate_smart_money_flow error for {symbol_name}: {e}", exc_info=True)
        else:
            logger.debug("calculate_smart_money_flow not available in utils; skipping smart money flow.")

        # extract latest indicator values (safe)
        latest_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else np.nan
        latest_macd = macd_line.iloc[-1] if len(macd_line) > 0 else np.nan
        latest_signal = signal_line.iloc[-1] if len(signal_line) > 0 else np.nan
        latest_sma_20 = sma_20_series.iloc[-1] if len(sma_20_series) > 0 else np.nan
        latest_sma_50 = sma_50_series.iloc[-1] if len(sma_50_series) > 0 else np.nan
        latest_vol_ma_5 = volume_ma_5_series.iloc[-1] if len(volume_ma_5_series) > 0 else np.nan
        latest_vol_ma_1m = volume_ma_1_month_series.iloc[-1] if len(volume_ma_1_month_series) > 0 else np.nan
        latest_vol_ma_6m = volume_ma_6_month_series.iloc[-1] if len(volume_ma_6_month_series) > 0 else np.nan
        latest_atr = atr_series.iloc[-1] if len(atr_series) > 0 else np.nan

        # ---------------------------
        # Apply filters and scoring
        # ---------------------------
        satisfied_filters = []
        total_score = 0
        reason_phrases = []

        # Define filters: function and args
        filter_definitions = {
            "فیلتر شکست مقاومت + عبور از MA50": {
                "func": lambda: is_resistance_breakout(df['high'], current_close) and pd.notna(latest_sma_50) and current_close > latest_sma_50,
                "score": 10, "category": "روند قیمت",
                "reason": "شکست مقاومت مهم و عبور از میانگین متحرک ۵۰ روزه"
            },
            "واگرایی مثبت RSI + افزایش حجم": {
                "func": lambda: pd.notna(latest_rsi) and is_rsi_oversold(latest_rsi, 30) and is_high_volume(current_volume, latest_vol_ma_5, multiplier=2.0),
                "score": 12, "category": "واگرایی",
                "reason": "واگرایی مثبت RSI و افزایش حجم چشمگیر"
            },
            "تقاطع طلایی MA20/MA50": {
                "func": lambda: pd.notna(latest_sma_20) and pd.notna(latest_sma_50) and latest_sma_20 > latest_sma_50 and (sma_20_series.iloc[-2] <= sma_50_series.iloc[-2] if len(sma_20_series) >= 2 and len(sma_50_series) >= 2 else False),
                "score": 15, "category": "میانگین‌ها", "reason": "تقاطع طلایی میانگین‌های متحرک ۲۰ و ۵۰ روزه"
            },
            "کندل چکشی یا دوجی با حجم بالا در کف": {
                "func": lambda: (check_candlestick_patterns(today_candle_data, yesterday_candle_data, hist_df_for_patterns) is not None and ("Hammer" in check_candlestick_patterns(today_candle_data, yesterday_candle_data, hist_df_for_patterns) or "Doji" in check_candlestick_patterns(today_candle_data, yesterday_candle_data, hist_df_for_patterns)) and is_high_volume(current_volume, latest_vol_ma_5, 1.5)) if check_candlestick_patterns else False,
                "score": 10, "category": "الگوهای کلاسیک", "reason": "تشکیل کندل چکشی یا دوجی با حجم بالا در کف"
            },


            "افزایش قدرت خریدار حقیقی + ورود پول": {
                "func": lambda: pd.notna(latest_individual_buy_power) and latest_individual_buy_power > 2.0,
                "score": 18, "category": "جریان وجوه", "reason": "افزایش قدرت خریدار حقیقی و ورود پول هوشمند"
            },
            "الگوی کف دوقلو + شکست گردن": {
                "func": lambda: _check_double_bottom_pattern(df['close'].values, df['high'].values, df['volume'].values),
                "score": 15, "category": "الگوهای کلاسیک", "reason": "تشکیل الگوی کف دوقلو و شکست خط گردن"
            },
            "شکست خط روند نزولی با کندل تایید": {
                "func": lambda: _check_descending_trendline_breakout(df['close'].values, df['high'].values, df['low'].values, df['volume'].values),
                "score": 13, "category": "روند قیمت", "reason": "شکست خط روند نزولی با کندل تأییدکننده"
            },
            "واگرایی مکدی + تقاطع صعودی": {
                "func": lambda: is_macd_buy_signal(macd_line, signal_line) and (df['close'].iloc[-1] < df['close'].iloc[-2] and macd_line.iloc[-1] > macd_line.iloc[-2]) if len(df) >= 2 else False,
                "score": 14, "category": "واگرایی", "reason": "واگرایی مثبت MACD و تقاطع صعودی خط سیگنال"
            },
            "عبور RSI از ناحیه اشباع فروش": {
                "func": lambda: (len(rsi_series) >= 2 and rsi_series.iloc[-1] > 30 and rsi_series.iloc[-2] <= 30 and current_close > (df['close'].iloc[-2] if len(df) >= 2 else np.nan)),
                "score": 11, "category": "روند قیمت", "reason": "عبور RSI از ناحیه اشباع فروش با افزایش قیمت"
            },
            "میانگین حجم ماه بالاتر از میانگین ۶ماهه + کندل صعودی": {
                "func": lambda: _check_monthly_volume_vs_six_month_avg(df['volume'].values, today_candle_data),
                "score": 9, "category": "حجم", "reason": "میانگین حجم ماه جاری بالاتر از میانگین ۶ ماهه با کندل صعودی قوی"
            },
            "حمایت شکسته": {
                "func": lambda: is_support_breakdown(df['low'], current_close),
                "score": -8, "category": "روند قیمت", "reason": "شکست حمایت مهم"
            },
            "RSI اشباع خرید": {
                "func": lambda: is_rsi_overbought(latest_rsi),
                "score": -10, "category": "روند قیمت", "reason": "RSI بالاتر از 70"
            },



            "تقاطع MACD نزولی": {
                "func": lambda: is_macd_sell_signal(macd_line, signal_line),
                "score": -12, "category": "واگرایی", "reason": "تقاطع نزولی MACD"
            },
        }

        for fname, finfo in filter_definitions.items():
            try:
                passed = finfo['func']()
                logger.debug(f"Filter '{fname}' for {symbol_name}: {passed}")
                if passed:
                    satisfied_filters.append(fname)
                    total_score += finfo['score']
                    reason_phrases.append(finfo['reason'])
            except Exception as e:
                logger.warning(f"Error evaluating filter '{fname}' for {symbol_name}: {e}", exc_info=True)

        # Additional ad-hoc scoring
        if pd.notna(latest_individual_buy_power) and latest_individual_buy_power > 1.2:
            total_score += 10
            reason_phrases.append(f"ورود پول هوشمند (IBP: {latest_individual_buy_power:.2f})")

        if pd.notna(pe_ratio_val) and pe_ratio_val > 30:
            reason_phrases.append(f"P/E بالا ({pe_ratio_val:.2f})")

        reason_str = ", ".join(reason_phrases) if reason_phrases else "بدون سیگنال"

        # Prepare result object for DB (but do not commit yet)
        result_payload = {
            "symbol_id": symbol_id,
            "symbol_name": symbol_name,
            "jdate": today_jdate_str,
            "score": int(total_score),
            "satisfied_filters": json.dumps(satisfied_filters),
            "reason": reason_str,
            "profit_loss_percentage": 0.0,
            "recommendation_price": current_close,
            "recommendation_jdate": today_jdate_str,
            "final_price": current_close,
            "status": "N/A",
            "probability_percent": 0.0,
            "timestamp": datetime.now(),
        }
        current_day_results.append(result_payload)
        logger.debug(f"Analyzed {symbol_name}: Score={total_score}, Filters={satisfied_filters}")

    # sort by score descending
    current_day_results.sort(key=lambda x: x['score'], reverse=True)

    # determine top N golden keys
    for idx, r in enumerate(current_day_results):
        r['is_golden_key'] = (idx < top_n_symbols and r['score'] > 0)
        if r['is_golden_key']:
            if r['score'] >= 50:
                r['signal_status'] = "📈 سیگنال قوی خرید"
            elif r['score'] >= 30:
                r['signal_status'] = "⚠️ احتمال رشد"
            else:
                r['signal_status'] = "❌ سیگنال ضعیف یا بی‌اثر"
        else:
            r['signal_status'] = "❌ سیگنال ضعیف یا بی‌اثر"

    # Upsert to DB
    new_count = 0
    updated_count = 0
    for r in current_day_results:
        try:
            existing = GoldenKeyResult.query.filter_by(symbol_id=r['symbol_id'], jdate=r['jdate']).first()
            final_reason_parts = []
            if r['reason'] and r['reason'] != "بدون سیگنال":
                final_reason_parts = [p.strip() for p in r['reason'].split(',') if p.strip()]
            final_reason_parts.insert(0, f"وضعیت سیگنال: {r['signal_status']}")
            final_reason_str = ", ".join(final_reason_parts)

            if existing:
                existing.score = r['score']
                existing.satisfied_filters = r['satisfied_filters']
                existing.reason = final_reason_str
                #existing.profit_loss_percentage = r['profit_loss_percentage']
                existing.recommendation_price = r['recommendation_price']
                existing.recommendation_jdate = r['recommendation_jdate']
                #existing.final_price = r['final_price']
                #existing.status = r['signal_status']
                existing.probability_percent = r['probability_percent']
                existing.is_golden_key = r['is_golden_key']
                existing.timestamp = datetime.now()
                db.session.add(existing)
                updated_count += 1
                logger.debug(f"Updated GoldenKeyResult for {r['symbol_name']}")
            else:
                new_obj = GoldenKeyResult(
                    symbol_id=r['symbol_id'],
                    symbol_name=r['symbol_name'],
                    jdate=r['jdate'],
                    score=r['score'],
                    satisfied_filters=r['satisfied_filters'],
                    reason=final_reason_str,
                    profit_loss_percentage=r['profit_loss_percentage'],
                    recommendation_price=r['recommendation_price'],
                    recommendation_jdate=r['recommendation_jdate'],
                    final_price=r['final_price'],
                    status=r['signal_status'],
                    probability_percent=r['probability_percent'],
                    is_golden_key=r['is_golden_key'],
                    timestamp=datetime.now()
                )
                db.session.add(new_obj)
                new_count += 1
                logger.debug(f"Inserted GoldenKeyResult for {r['symbol_name']}")
        except Exception as e:
            db.session.rollback()
            logger.error(f"DB upsert error for {r.get('symbol_name')}: {e}", exc_info=True)

    try:
        db.session.commit()
        logger.info(f"Golden Key analysis completed. New: {new_count}, Updated: {updated_count}. Top {top_n_symbols} flagged.")
        # extra debug: list top few
        logger.info("--- Top results ---")
        for i, r in enumerate(current_day_results[:min(10, len(current_day_results))]):
            logger.info(f"Rank {i+1}: {r['symbol_name']}, Score: {r['score']}, GoldenKey: {r['is_golden_key']}")
        return True, f"Golden Key analysis completed. New: {new_count}, Updated: {updated_count}."
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error committing Golden Key results: {e}", exc_info=True)
        return False, f"DB commit failed: {str(e)}"


# ---------------------------------------------------------
# API to retrieve results
# ---------------------------------------------------------
def get_golden_key_results(filters=None, top_n=8):
    """
    Returns a dict with top_stocks, technical_filters and last_updated.
    If filters provided (comma-separated filter names), returns only results that match ALL requested filters.
    """
    latest_date_result = db.session.query(func.max(GoldenKeyResult.jdate)).scalar()
    if not latest_date_result:
        logger.warning("No GoldenKeyResult rows found.")
        return {"top_stocks": [], "technical_filters": get_golden_key_filter_definitions(), "last_updated": "نامشخص"}

    query = GoldenKeyResult.query.filter_by(jdate=latest_date_result)
    if filters:
        wanted = [f.strip() for f in filters.split(',') if f.strip()]
        all_rows = query.all()
        matched = []
        for r in all_rows:
            satisfied = json.loads(r.satisfied_filters) if r.satisfied_filters else []
            if all(w in satisfied for w in wanted):
                matched.append(r)
        rows = matched
    else:
        # return explicitly flagged golden keys; fallback to top N by score
        rows = query.filter_by(is_golden_key=True).order_by(GoldenKeyResult.score.desc()).all()
        if not rows:
            rows = query.order_by(GoldenKeyResult.score.desc()).limit(top_n).all()

    output = []
    for r in rows:
        sf_list = json.loads(r.satisfied_filters) if r.satisfied_filters else []
        output.append({
            "symbol": r.symbol_name,
            "symbol_id": r.symbol_id,
            "jdate": r.jdate,
            "satisfied_filters": r.satisfied_filters,
            "satisfied_filters_list": sf_list,
            "total_score": r.score,
            "reason": r.reason,
            "entry_price": r.recommendation_price,
            "jentry_date": r.recommendation_jdate,
            "exit_price": r.final_price if getattr(r, 'status', '').lower().startswith('closed') else None,
            "jexit_date": getattr(r, 'jexit_date', None) if getattr(r, 'status', '').lower().startswith('closed') else None,
            "profit_loss_percentage": r.profit_loss_percentage if getattr(r, 'status', '').lower().startswith('closed') else None,
            "is_golden_key": r.is_golden_key,
            "status": r.status,
            "probability_percent": r.probability_percent,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None
        })
    logger.info(f"Returning {len(output)} Golden Key results for {latest_date_result}")
    return {"top_stocks": output, "technical_filters": get_golden_key_filter_definitions(), "last_updated": latest_date_result}


def get_golden_key_filter_definitions():
    return [
        {"name": "فیلتر شکست مقاومت + عبور از MA50", "category": "روند قیمت", "description": "قیمت پایانی بالاتر از بیشینه ۲۰ روزه و عبور از میانگین متحرک ۵۰ روزه."},
        {"name": "واگرایی مثبت RSI + افزایش حجم", "category": "واگرایی", "description": "قیمت کف پایین‌تر ساخته ولی RSI کف پایین‌تر نساخته و حجم معاملات امروز > ۲ برابر میانگین ۵ روزه."},
        {"name": "تقاطع طلایی MA20/MA50", "category": "میانگین‌ها", "description": "MA20 از پایین MA50 را قطع کند و قیمت بالاتر از هر دو میانگین باشد."},
        {"name": "کندل چکشی یا دوجی با حجم بالا در کف", "category": "الگوهای کلاسیک", "description": "تشکیل کندل چکشی یا دوجی در انتهای روند نزولی با حجم زیاد."},
        {"name": "افزایش قدرت خریدار حقیقی + ورود پول", "category": "جریان وجوه", "description": "سرانه خرید حقیقی > ۲ برابر سرانه فروش و مجموع خرید حقیقی امروز > میانگین ماهانه."},
        {"name": "الگوی کف دوقلو + شکست گردن", "category": "الگوهای کلاسیک", "description": "دو کف مشابه طی ۲ هفته اخیر و شکست مقاومت بین دو کف با حجم بالا."},
        {"name": "شکست خط روند نزولی با کندل تایید", "category": "روند قیمت", "description": "عبور قیمت از خط روند نزولی و کندل تأییدکننده با حجم بالا."},
        {"name": "واگرایی مکدی + تقاطع صعودی", "category": "واگرایی", "description": "MACD خط سیگنال را از پایین به بالا قطع کند و مکدی واگرایی مثبت با قیمت نشان دهد."},
        {"name": "عبور RSI از ناحیه اشباع فروش", "category": "روند قیمت", "description": "RSI دیروز < ۳۰ و امروز > ۳۰ و قیمت آخرین > قیمت دیروز."},
        {"name": "میانگین حجم ماه بالاتر از میانگین ۶ماهه + کندل صعودی", "category": "حجم", "description": "میانگین حجم ماه جاری بالاتر از میانگین ۶ ماهه با کندل صعودی قوی."},
        {"name": "حمایت شکسته", "category": "روند قیمت", "description": "شکست حمایت مهم"},
        {"name": "RSI اشباع خرید", "category": "روند قیمت", "description": "شاخص قدرت نسبی (RSI) بالای ۷۰ است."},
        {"name": "تقاطع MACD نزولی", "category": "واگرایی", "description": "خط MACD از خط سیگنال به سمت پایین عبور کرده است."},
    ]





# ---------------------------------------------------------
# Performance calculation
# ---------------------------------------------------------

def _update_golden_key_performance(all_results, days_to_evaluate=5):
    """
    Updates the final_price and profit_loss_percentage for GoldenKey signals 
    by fetching the closing price on the LAST AVAILABLE day, provided at least 3 records are found.
    Uses symbol_id lookup for robust data retrieval.
    """
    evaluated_results = []
    updated_count = 0
    
    MAX_RECORDS_TO_FETCH = days_to_evaluate + 1 # هدف: 6 رکورد (روز 0 تا روز 5)
    MIN_RECORDS_REQUIRED = 3  # حداقل 3 رکورد برای ارزیابی (روز 0 + دو روز بعد)
    
    for res in all_results:
        # فقط سیگنال‌هایی که هنوز نهایی نشده‌اند (profit_loss_percentage صفر است) و جزو GoldenKey بوده‌اند، ارزیابی شوند.
        if res.is_golden_key and (res.profit_loss_percentage is None or res.profit_loss_percentage == 0.0):
            try:
                symbol_name_to_search = res.symbol_name.strip()
                
                # 💡 گام ۱: پیدا کردن symbol_id از جدول اصلی ComprehensiveSymbolData
                # استفاده از strip() برای حذف فضای خالی احتمالی در نام نماد
                symbol_record = db.session.query(ComprehensiveSymbolData.id).filter(
                    ComprehensiveSymbolData.symbol_name == symbol_name_to_search
                ).first()

                if not symbol_record:
                    logger.warning(f"❌ نماد {res.symbol_name} در ComprehensiveSymbolData یافت نشد. Skipping.")
                    continue
                    
                target_symbol_id = symbol_record[0] # استخراج ID
                
                # 2. جستجو برای داده‌های تاریخی بر اساس symbol_id (کلید یکتا)
                historical_data = HistoricalData.query.filter(
                    # 💡 CHANGE: فیلتر بر اساس کلید عددی یکتا (symbol_id) به جای symbol_name
                    HistoricalData.symbol_id == target_symbol_id,
                    HistoricalData.jdate >= res.jdate # فیلتر بر اساس تاریخ شمسی موجود
                ).order_by(HistoricalData.jdate.asc()).limit(MAX_RECORDS_TO_FETCH).all() # محدودیت 6 رکورد
                
                
                # 3. چک کردن حداقل داده (حداقل 3 رکورد موجود باشد)
                if len(historical_data) >= MIN_RECORDS_REQUIRED: 
                    
                    # 💡 انتخاب قیمت نهایی: از آخرین رکورد موجود استفاده می‌شود
                    exit_data = historical_data[-1] 

                    # 💡 استخراج قیمت با اولویت 'close' و Fallback به 'final'
                    exit_price = getattr(exit_data, 'close', getattr(exit_data, 'final', None))
                    
                    if exit_price is None:
                         logger.error(f"Could not find a valid exit price ('close' or 'final') for {res.symbol_name} on {exit_data.jdate}.")
                         continue
                         
                    entry_price = res.recommendation_price # قیمت ورودی
                    
                    # 4. محاسبه درصد سود/زیان
                    profit_loss_percent = ((exit_price - entry_price) / entry_price) * 100
                    
                    # 5. به‌روزرسانی رکورد در دیتابیس
                    num_days_evaluated = len(historical_data) - 1 # تعداد روزهای کاری پس از سیگنال
                    res.final_price = exit_price
                    res.profit_loss_percentage = profit_loss_percent
                    # 💡 به‌روزرسانی وضعیت برای نشان دادن تعداد روز ارزیابی شده
                    res.status = f"✅ ارزیابی ({num_days_evaluated} روز) (سود)" if profit_loss_percent > 0 else f"❌ ارزیابی ({num_days_evaluated} روز) (زیان)"
                    res.timestamp = datetime.now()
                    db.session.add(res)
                    updated_count += 1
                else:
                    logger.debug(f"Insufficient data (found {len(historical_data)} records, less than {MIN_RECORDS_REQUIRED} required) for {res.symbol_name} ({res.jdate}). Skipping final price update.")


            except Exception as e:
                # خطا اینجا log می‌شود، اما به commit کردن در انتها اجازه می‌دهد
                logger.error(f"Error updating performance for {res.symbol_name} ({res.jdate}): {e}", exc_info=True)
                
        evaluated_results.append(res) 

    # commit کردن تغییرات
    try:
        db.session.commit()
        logger.info(f"Successfully updated final_price for {updated_count} GoldenKey results.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"DB commit error during performance update: {e}", exc_info=True)
        
    return evaluated_results




# ---------------------------------------------------------
# Performance & Win-rate helpers (simplified / safe)
# ---------------------------------------------------------

def _calculate_performance_metrics(all_results, start_date):
    successful = 0
    total_profit = 0.0 # جمع درصد سودهای واقعی
    total_loss = 0.0   # جمع درصد زیان‌های واقعی
    total = 0

    try:
        start_gregorian = jdatetime.datetime.strptime(start_date, '%Y-%m-%d').togregorian().date()
    except Exception:
        start_gregorian = None

    for res in all_results:
        try:
            # از jdate استفاده می‌کنیم که تاریخ صدور سیگنال است
            res_date = jdatetime.datetime.strptime(res.jdate, '%Y-%m-%d').togregorian().date()
        except Exception:
            continue
            
        # فقط سیگنال‌های Golden Key و در بازه زمانی مورد نظر
        if res.is_golden_key and (start_gregorian is None or res_date >= start_gregorian):
            
            # ✅ شرط کلیدی: فقط سیگنال‌هایی که final_price در آن‌ها محاسبه شده را در ارزیابی دخیل کن
            if res.profit_loss_percentage is not None and res.profit_loss_percentage != 0.0:
                total += 1
                
                profit_percent = res.profit_loss_percentage
                
                if profit_percent > 0:
                    successful += 1
                    total_profit += profit_percent # جمع درصد سودها
                else:
                    total_loss += profit_percent * -1 # جمع درصد زیان‌ها (به صورت مثبت)
            # else:
                # سیگنال‌هایی که هنوز ارزیابی نهایی نشده‌اند (کمتر از 5 روز گذشته است) نادیده گرفته می‌شوند.


    win_rate = (successful / total * 100) if total > 0 else 0.0
    # درصد سود و زیان تجمعی باید به عنوان جمع درصدها برگردانده شود.
    return total, successful, win_rate, total_profit, total_loss


def _save_performance_metrics(today_jdate_str, period_type, signal_source, total_signals, successful_signals, win_rate, total_profit_percent, total_loss_percent):
    existing = AggregatedPerformance.query.filter_by(report_date=today_jdate_str, period_type=period_type, signal_source=signal_source).first()
    if existing:
        existing.total_signals = total_signals
        existing.successful_signals = successful_signals
        existing.win_rate = win_rate
        existing.total_profit_percent = total_profit_percent
        existing.total_loss_percent = total_loss_percent
        existing.updated_at = datetime.now()
        db.session.add(existing)
        logger.info(f"Updated aggregated performance for {signal_source} ({period_type})")
    else:
        newp = AggregatedPerformance(
            report_date=today_jdate_str,
            period_type=period_type,
            signal_source=signal_source,
            total_signals=total_signals,
            successful_signals=successful_signals,
            win_rate=win_rate,
            total_profit_percent=total_profit_percent,
            total_loss_percent=total_loss_percent,
            created_at=datetime.now()
        )
        db.session.add(newp)
        logger.info(f"Saved new aggregated performance for {signal_source} ({period_type})")
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving aggregated performance: {e}", exc_info=True)


def calculate_golden_key_win_rate():
    logger.info("Starting performance metrics update and save process.")
    today_jdate_str = get_today_jdate_str()
    # 1. بازیابی تمام نتایج برای ارزیابی
    all_results = GoldenKeyResult.query.all()
    
    if not all_results:
        logger.warning("No GoldenKeyResult rows found for performance computation.")
        return False, "Performance computation skipped: No Golden Key results found." 
        
    # 2. ✅ گام جدید: به‌روزرسانی final_price و profit_loss_percentage در دیتابیس
    # این گام باید انجام شود تا _calculate_performance_metrics داده‌ی ارزیابی شده داشته باشد.
    evaluated_results = _update_golden_key_performance(all_results, days_to_evaluate=5)

    # 3. تعریف بازه‌های زمانی
    # ✅ اصلاح بازه‌ی هفتگی به ۶ روز
    week_ago = (jdatetime.datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    month_ago = (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    # 4. محاسبه و ذخیره هفتگی (از evaluated_results استفاده می‌کند)
    w_total, w_success, w_win, w_profit, w_loss = _calculate_performance_metrics(evaluated_results, week_ago)
    _save_performance_metrics(today_jdate_str, 'weekly', 'GoldenKeyService', w_total, w_success, w_win, w_profit, w_loss)

    # 5. محاسبه و ذخیره ماهانه (از evaluated_results استفاده می‌کند)
    m_total, m_success, m_win, m_profit, m_loss = _calculate_performance_metrics(evaluated_results, month_ago)
    _save_performance_metrics(today_jdate_str, 'monthly', 'GoldenKeyService', m_total, m_success, m_win, m_profit, m_loss)

    logger.info("Performance metrics update done.")
    # ✅ بازگرداندن مقادیر موفقیت‌آمیز برای کنترلر
    return True, "Golden Key Win-Rate calculation completed successfully after performance update."


# End of file
