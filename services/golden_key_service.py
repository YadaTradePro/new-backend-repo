# services/golden_key_service.py
from extensions import db
from models import ComprehensiveSymbolData, HistoricalData, TechnicalIndicatorData, GoldenKeyResult, AggregatedPerformance
from flask import current_app
import pandas as pd
import logging
from datetime import datetime, timedelta, date
import jdatetime # Ensure jdatetime is imported for Jalali to Gregorian conversion
import numpy as np
import json 
from sqlalchemy import func 

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# Import utility functions - ONLY import functions that are actually present in services/utils.py
from services.utils import (
    get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, 
    calculate_sma, calculate_bollinger_bands, calculate_volume_ma, get_symbol_id,
    calculate_atr, 
    calculate_smart_money_flow, # This function is in your provided utils.py
    check_candlestick_patterns # This function is in your provided utils.py
)

# --- Helper Functions for Filters ---
def is_resistance_breakout(df_high, current_close, days_window=20):
    """
    Checks for resistance breakout: current close > max high of last `days_window` days.
    """
    if len(df_high) < days_window:
        logger.debug(f"is_resistance_breakout: Not enough data ({len(df_high)} < {days_window})")
        return False
    # Get high prices for the last 'days_window' excluding current day
    recent_highs = df_high.iloc[-days_window-1:-1].max() if days_window + 1 <= len(df_high) else df_high.iloc[:-1].max()
    result = current_close > recent_highs if pd.notna(current_close) and pd.notna(recent_highs) else False
    logger.debug(f"Resistance Breakout: current_close={current_close}, recent_highs={recent_highs}, Result={result}")
    return result

def is_support_breakdown(df_low, current_close, days_window=20):
    """
    Checks for support breakdown: current close < min low of last `days_window` days.
    """
    if len(df_low) < days_window:
        logger.debug(f"is_support_breakdown: Not enough data ({len(df_low)} < {days_window})")
        return False
    # Get low prices for the last 'days_window' excluding current day
    recent_lows = df_low.iloc[-days_window-1:-1].min() if days_window + 1 <= len(df_low) else df_low.iloc[:-1].min()
    result = current_close < recent_lows if pd.notna(current_close) and pd.notna(recent_lows) else False
    logger.debug(f"Support Breakdown: current_close={current_close}, recent_lows={recent_lows}, Result={result}")
    return result

def is_high_volume(current_volume, avg_volume, multiplier=1.5):
    """
    Checks if current volume is significantly higher than average volume.
    """
    result = current_volume > (avg_volume * multiplier) if pd.notna(current_volume) and pd.notna(avg_volume) else False
    logger.debug(f"High Volume: current_volume={current_volume}, avg_volume={avg_volume}, Result={result}")
    return result

def is_rsi_oversold(rsi_value, threshold=30):
    """
    Checks if RSI indicates oversold conditions.
    """
    result = rsi_value < threshold if pd.notna(rsi_value) else False
    logger.debug(f"RSI Oversold: rsi_value={rsi_value}, Result={result}")
    return result

def is_rsi_overbought(rsi_value, threshold=70):
    """
    Checks if RSI indicates overbought conditions.
    """
    result = rsi_value > threshold if pd.notna(rsi_value) else False
    logger.debug(f"RSI Overbought: rsi_value={rsi_value}, Result={result}")
    return result

def is_macd_buy_signal(macd_line, signal_line):
    """
    Checks for MACD buy signal (MACD crosses above Signal Line).
    """
    if macd_line.empty or signal_line.empty or len(macd_line) < 2 or len(signal_line) < 2:
        logger.debug("MACD Buy Signal: Not enough data for cross-over check.")
        return False
    
    current_macd = macd_line.iloc[-1]
    current_signal = signal_line.iloc[-1]
    prev_macd = macd_line.iloc[-2]
    prev_signal = signal_line.iloc[-2]

    if pd.isna(current_macd) or pd.isna(current_signal) or \
       pd.isna(prev_macd) or pd.isna(prev_signal):
        logger.debug("MACD Buy Signal: NaN values encountered.")
        return False

    result = (current_macd > current_signal) and (prev_macd <= prev_signal)
    logger.debug(f"MACD Buy Signal: current_macd={current_macd}, current_signal={current_signal}, prev_macd={prev_macd}, prev_signal={prev_signal}, Result={result}")
    return result

def is_macd_sell_signal(macd_line, signal_line):
    """
    Checks for MACD sell signal (MACD crosses below Signal Line).
    """
    if macd_line.empty or signal_line.empty or len(macd_line) < 2 or len(signal_line) < 2:
        logger.debug("MACD Sell Signal: Not enough data for cross-over check.")
        return False

    current_macd = macd_line.iloc[-1]
    current_signal = signal_line.iloc[-1]
    prev_macd = macd_line.iloc[-2]
    prev_signal = signal_line.iloc[-2]

    if pd.isna(current_macd) or pd.isna(current_signal) or \
       pd.isna(prev_macd) or pd.isna(prev_signal):
        logger.debug("MACD Sell Signal: NaN values encountered.")
        return False

    result = (current_macd < current_signal) and (prev_macd >= prev_signal)
    logger.debug(f"MACD Sell Signal: current_macd={current_macd}, current_signal={current_signal}, prev_macd={prev_macd}, prev_signal={prev_signal}, Result={result}")
    return result

# --- Internal Implementations for Complex Filters ---

def _check_double_bottom_pattern(close_prices_array, high_prices_array, volume_array):
    """
    Checks for a simplified Double Bottom pattern + Neckline Breakout.
    Converts input arrays to pandas Series for easier processing.
    """
    logger.debug("Checking for Double Bottom pattern (simplified internal implementation).")
    
    # Convert numpy arrays to pandas Series
    close_series = pd.Series(close_prices_array).dropna()
    high_series = pd.Series(high_prices_array).dropna()
    volume_series = pd.Series(volume_array).dropna()

    if len(close_series) < 40 or close_series.empty or high_series.empty or volume_series.empty: 
        logger.debug("Double Bottom: Insufficient or invalid data after conversion/dropna.")
        return False

    recent_closes = close_series.iloc[-40:]
    
    first_half = recent_closes.iloc[:20]
    if first_half.empty: 
        logger.debug("Double Bottom: First half of recent closes is empty.")
        return False
    bottom1_idx = first_half.idxmin()
    bottom1_price = first_half.min()

    second_half = recent_closes.iloc[20:]
    if second_half.empty: 
        logger.debug("Double Bottom: Second half of recent closes is empty.")
        return False
    bottom2_idx = second_half.idxmin()
    bottom2_price = second_half.min()

    if not (0.95 * bottom1_price <= bottom2_price <= 1.05 * bottom1_price):
        logger.debug("Double Bottom: Bottoms are not at similar levels.")
        return False

    # Ensure indices are valid for slicing
    if bottom1_idx > bottom2_idx: # Ensure bottom1 is before bottom2
        bottom1_idx, bottom2_idx = bottom2_idx, bottom1_idx # Swap if out of order

    neckline_segment = close_series.loc[bottom1_idx:bottom2_idx]
    if neckline_segment.empty: 
        logger.debug("Double Bottom: Neckline segment is empty.")
        return False
    neckline_price = neckline_segment.max()

    if close_series.empty or volume_series.empty: # Re-check after potential empty slice
        return False

    current_close = close_series.iloc[-1]
    current_volume = volume_series.iloc[-1]
    
    if len(volume_series) < 10: # Ensure enough data for avg_volume_recent
        logger.debug("Double Bottom: Not enough volume data for recent average.")
        return False
    
    avg_volume_recent = volume_series.iloc[-10:].mean() 

    if pd.isna(current_close) or pd.isna(neckline_price) or pd.isna(current_volume) or pd.isna(avg_volume_recent):
        logger.debug("Double Bottom: NaN values in critical data points.")
        return False

    if current_close > neckline_price and current_volume > (avg_volume_recent * 1.5):
        logger.debug(f"Double Bottom detected: Bottom1={bottom1_price:.2f}, Bottom2={bottom2_price:.2f}, Neckline={neckline_price:.2f}, Current Close={current_close:.2f}, Current Volume={current_volume:.0f}")
        return True
    
    return False

def _check_descending_trendline_breakout(close_prices_array, high_prices_array, low_prices_array, volume_array):
    """
    Checks for a simplified Descending Trendline Breakout with confirmation candle.
    Converts input arrays to pandas Series for faster processing.
    """
    logger.debug("Checking for Descending Trendline Breakout (simplified internal implementation).")
    
    # Convert numpy arrays to pandas Series
    close_series = pd.Series(close_prices_array).dropna()
    high_series = pd.Series(high_prices_array).dropna()
    low_series = pd.Series(low_prices_array).dropna()
    volume_series = pd.Series(volume_array).dropna()

    if len(close_series) < 30 or close_series.empty or high_series.empty or low_series.empty or volume_series.empty: 
        logger.debug("Descending Trendline: Insufficient or invalid data after conversion/dropna.")
        return False

    recent_highs = high_series.iloc[-30:]
    
    peaks = recent_highs[recent_highs == recent_highs.rolling(window=3, center=True).max()]
    peaks = peaks.dropna() 
    
    descending_peaks = []
    if len(peaks) >= 2:
        # Filter for truly descending peaks
        for i in range(len(peaks) - 1):
            if peaks.iloc[i] > peaks.iloc[i+1]:
                descending_peaks.append((peaks.index[i], peaks.iloc[i]))
                descending_peaks.append((peaks.index[i+1], peaks.iloc[i+1]))
        
        descending_peaks = sorted(list(set(descending_peaks)), key=lambda x: x[0])
        
        if len(descending_peaks) >= 2:
            peak1_idx, peak1_price = descending_peaks[-2]
            peak2_idx, peak2_price = descending_peaks[-1]

            if peak2_idx <= peak1_idx: # Ensure peaks are chronologically ordered
                logger.debug("Descending Trendline: Peaks are not in chronological order.")
                return False

            # Calculate slope of the trendline (price change per index unit)
            slope = (peak2_price - peak1_price) / (peak2_idx - peak1_idx)
            
            # Trendline must be descending (negative slope)
            if slope >= 0:
                logger.debug(f"Descending Trendline: Slope is not negative ({slope:.2f}).")
                return False

            current_idx = close_series.index[-1]
            projected_trendline_value = peak2_price + slope * (current_idx - peak2_idx)

            current_close = close_series.iloc[-1]
            current_open = close_series.iloc[-1] 
            current_volume = volume_series.iloc[-1]
            
            if len(volume_series) < 10: # Ensure enough data for avg_volume_recent
                logger.debug("Descending Trendline: Not enough volume data for recent average.")
                return False
            
            avg_volume_recent = volume_series.iloc[-10:].mean()

            if pd.isna(current_close) or pd.isna(projected_trendline_value) or \
               pd.isna(current_open) or pd.isna(current_volume) or pd.isna(avg_volume_recent):
                logger.debug("Descending Trendline: NaN values in critical data points.")
                return False


            # Check for breakout and confirmation candle (strong bullish candle with high volume)
            if current_close > projected_trendline_value and \
               current_close > current_open and \
               abs(current_close - current_open) > (high_series.iloc[-1] - low_series.iloc[-1]) * 0.5 and \
               current_volume > (avg_volume_recent * 1.5):
                logger.debug(f"Descending Trendline Breakout detected: Trendline Value={projected_trendline_value:.2f}, Current Close={current_close:.2f}, Current Volume={current_volume:.0f}")
                return True
    return False

def _check_monthly_volume_vs_six_month_avg(volume_array, today_candle_data):
    """
    Checks if current month's average volume is higher than 6-month average volume
    combined with a strong bullish candle.
    Converts input array to pandas Series for faster processing.
    """
    logger.debug("Checking for Monthly Volume vs. Six Month Avg (simplified internal implementation).")
    
    # Convert numpy array to pandas Series
    volume_series = pd.Series(volume_array).dropna()

    if len(volume_series) < 120 or volume_series.empty: 
        logger.debug("Monthly Volume: Insufficient or invalid volume data after conversion/dropna.")
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
    else:
        logger.debug("Monthly Volume: Missing or NaN candle data for bullish candle check.")

    if pd.isna(avg_volume_1_month) or pd.isna(avg_volume_6_month):
        logger.debug("Monthly Volume: NaN average volume values.")
        return False

    if avg_volume_1_month > avg_volume_6_month * 1.2 and is_strong_bullish_candle: 
        logger.debug(f"Monthly Volume vs. Six Month Avg: 1M Avg={avg_volume_1_month:.0f}, 6M Avg={avg_volume_6_month:.0f}. Strong Bullish Candle: {is_strong_bullish_candle}")
        return True
    
    return False


# --- Main Golden Key Logic ---

def run_golden_key_analysis_and_save(top_n_symbols=8): 
    logger.info("Starting Golden Key analysis and saving process.")
    
    today_jdate_str = get_today_jdate_str()
    all_symbols = ComprehensiveSymbolData.query.all()
    
    if not all_symbols:
        logger.warning("No symbols found in ComprehensiveSymbolData. Cannot run Golden Key analysis.")
        return False, "No symbols found to analyze."

    fund_keywords = ["صندوق", "سرمایه گذاری", "اعتبار", "آتیه", "یکتا", "بورس", "دارایی", "گیلان", "اختصاصی", 
                     "تدبیر", "دماوند", "سپهر", "سودمند", "کامیاب", "آشنا", "ماهور", "ح", "پ", "ت"] 
    
    fund_symbol_ids_to_delete = []
    for symbol_data in all_symbols:
        symbol_name = symbol_data.symbol_name
        if any(keyword in symbol_name for keyword in fund_keywords):
            fund_symbol_ids_to_delete.append(symbol_data.symbol_id)
            
    if fund_symbol_ids_to_delete:
        latest_date_result = db.session.query(func.max(GoldenKeyResult.jdate)).scalar()
        if latest_date_result:
            try:
                deleted_count = GoldenKeyResult.query.filter(
                    GoldenKeyResult.symbol_id.in_(fund_symbol_ids_to_delete),
                    GoldenKeyResult.jdate == latest_date_result
                ).delete(synchronize_session=False)
                db.session.commit() 
                logger.info(f"Deleted {deleted_count} Golden Key results for investment funds/rights on {latest_date_result}.")
            except Exception as e:
                db.session.rollback()
                logger.error(f"Error deleting old fund/rights results: {e}", exc_info=True)
    
    current_day_results = []

    for symbol_data in all_symbols:
        symbol_id = symbol_data.symbol_id
        symbol_name = symbol_data.symbol_name 

        if any(keyword in symbol_name for keyword in fund_keywords):
            logger.debug(f"Skipping {symbol_name}: Identified as an investment fund or right based on keywords. (Symbol ID: {symbol_id})")
            continue
        
        logger.debug(f"Analyzing symbol: {symbol_name} ({symbol_id})")

        historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.jdate.asc()).all()
        
        if not historical_records or len(historical_records) < 120: 
            logger.debug(f"Skipping {symbol_name}: Insufficient historical data ({len(historical_records)} records). Minimum 120 required for complex filters.")
            continue

        df = pd.DataFrame([r.__dict__ for r in historical_records])
        
        if df.empty:
            logger.debug(f"Skipping {symbol_name}: DataFrame is empty after conversion from historical records.")
            continue

        if 'jdate' not in df.columns:
            logger.error(f"Column 'jdate' not found in DataFrame for {symbol_name}. Cannot process dates. Skipping.")
            continue
            
        df['gregorian_date'] = df['jdate'].apply(lambda x: jdatetime.datetime.strptime(x, '%Y-%m-%d').togregorian())
        df = df.set_index(pd.to_datetime(df['gregorian_date'])) 
        df = df.sort_index() 

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
                logger.warning(f"Column '{col}' not found in DataFrame for {symbol_name}. This might affect calculations.")
        
        critical_ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_critical_cols_after_conversion = [col for col in critical_ohlcv_cols if col not in df.columns]
        if missing_critical_cols_after_conversion:
            logger.error(f"Critical OHLCV columns missing from DataFrame for {symbol_name} after numeric conversion: {missing_critical_cols_after_conversion}. Skipping analysis for this symbol.")
            continue 
        
        df.dropna(subset=critical_ohlcv_cols, inplace=True)

        if df.empty:
            logger.debug(f"Skipping {symbol_name}: Historical data became empty after cleaning critical OHLCV NaNs.")
            continue
        
        # Re-check minimum length after dropping NaNs, especially for patterns needing longer history
        if len(df) < 120: 
            logger.debug(f"Skipping {symbol_name}: Insufficient historical data ({len(df)} records) after NaN removal for full indicator calculation. Minimum 120 required.")
            continue

        current_close = df['close'].iloc[-1]
        current_volume = df['volume'].iloc[-1]
        
        if len(df) < 2: 
            logger.debug(f"Skipping {symbol_name}: Not enough data for candlestick patterns (need at least 2 days).")
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
        
        pe_ratio_val = symbol_data.pe_ratio if hasattr(symbol_data, 'pe_ratio') else np.nan 
        eps_val = symbol_data.eps if hasattr(symbol_data, 'eps') else np.nan 

        rsi_val = calculate_rsi(df['close']) 
        macd_line, signal_line, _ = calculate_macd(df['close']) 
        sma_20 = calculate_sma(df['close'], window=20) 
        sma_50 = calculate_sma(df['close'], window=50) 
        volume_ma_5_day = calculate_volume_ma(df['volume'], window=5) 
        volume_ma_1_month = calculate_volume_ma(df['volume'], window=20) # Approx 1 month (20 trading days)
        volume_ma_6_month = calculate_volume_ma(df['volume'], window=120) # Approx 6 months (120 trading days)
        atr_val = calculate_atr(df['high'], df['low'], df['close']) 
        
        # --- Call smart money flow calculation (assuming utils.py expects non-_hist names) ---
        smart_money_flow_df_result = calculate_smart_money_flow(df) 
        
        latest_individual_buy_power = np.nan
        if not smart_money_flow_df_result.empty and 'individual_buy_power' in smart_money_flow_df_result.columns:
            latest_individual_buy_power = smart_money_flow_df_result['individual_buy_power'].iloc[-1]
        else:
            logger.warning(f"Could not calculate 'individual_buy_power' for {symbol_name}. Result DataFrame from calculate_smart_money_flow was empty or missing column. This might be due to missing or incorrectly named columns in your HistoricalData.")
        
        # NEW: Log the latest_individual_buy_power value
        logger.debug(f"  Individual Buy Power for {symbol_name}: {latest_individual_buy_power:.2f}")
        # --- End of smart money flow column handling ---

        latest_rsi = rsi_val.iloc[-1] if not rsi_val.empty else np.nan
        latest_macd = macd_line.iloc[-1] if not macd_line.empty else np.nan
        latest_signal_line = signal_line.iloc[-1] if not signal_line.empty else np.nan
        latest_sma_20 = sma_20.iloc[-1] if not sma_20.empty else np.nan 
        latest_sma_50 = sma_50.iloc[-1] if not sma_50.empty else np.nan 
        latest_volume_ma_5_day = volume_ma_5_day.iloc[-1] if not volume_ma_5_day.empty else np.nan 
        latest_volume_ma_1_month = volume_ma_1_month.iloc[-1] if not volume_ma_1_month.empty else np.nan 
        latest_volume_ma_6_month = volume_ma_6_month.iloc[-1] if not volume_ma_6_month.empty else np.nan 
        latest_atr = atr_val.iloc[-1] if not atr_val.empty else np.nan
        
        satisfied_filters = []
        total_score = 0
        reason_phrases = []

        # Filter definitions and their criteria (with updated scores and logic)
        filter_definitions = {
            "فیلتر شکست مقاومت + عبور از MA50": {
                "func": lambda high_df, current_close_val, sma_50_val: is_resistance_breakout(high_df, current_close_val) and pd.notna(sma_50_val) and current_close_val > sma_50_val, 
                "args": [df['high'], current_close, latest_sma_50], 
                "score": 10, "category": "روند قیمت", "reason": "شکست مقاومت مهم و عبور از میانگین متحرک ۵۰ روزه"
            }, 
            "واگرایی مثبت RSI + افزایش حجم": {
                "func": lambda rsi_val, vol, vol_ma_5: pd.notna(rsi_val) and is_rsi_oversold(rsi_val, threshold=30) and is_high_volume(vol, vol_ma_5, multiplier=2.0), 
                "args": [latest_rsi, current_volume, latest_volume_ma_5_day], 
                "score": 12, "category": "واگرایی", "reason": "واگرایی مثبت RSI و افزایش حجم چشمگیر"
            }, 
            "تقاطع طلایی MA20/MA50": {
                "func": lambda c, sma20, sma50: pd.notna(c) and pd.notna(sma20) and pd.notna(sma50) and \
                                            c > sma20 and c > sma50 and \
                                            sma20 > sma50 and \
                                            (sma_20.iloc[-2] <= sma_50.iloc[-2] if len(sma_20) >= 2 and len(sma_50) >= 2 else False), # Check for actual cross
                "args": [current_close, latest_sma_20, latest_sma_50], 
                "score": 15, "category": "میانگین‌ها", "reason": "تقاطع طلایی میانگین‌های متحرک ۲۰ و ۵۰ روزه"
            }, 
            "کندل چکشی یا دوجی با حجم بالا در کف": {
                "func": lambda tcd, ycd, cls_series_arr, vol, vol_ma: ("Hammer" in check_candlestick_patterns(tcd, ycd, cls_series_arr) or "Doji" in check_candlestick_patterns(tcd, ycd, cls_series_arr)) and is_high_volume(vol, vol_ma, multiplier=1.5), 
                "args": [today_candle_data, yesterday_candle_data, df['close'].values, current_volume, latest_volume_ma_5_day], 
                "score": 10, "category": "الگوهای کلاسیک", "reason": "تشکیل کندل چکشی یا دوجی با حجم بالا در کف روند نزولی"
            }, 
            "افزایش قدرت خریدار حقیقی + ورود پول": {
                "func": lambda val: pd.notna(val) and val > 2.0, 
                "args": [latest_individual_buy_power], 
                "score": 18, "category": "جریان وجوه", "reason": "افزایش قدرت خریدار حقیقی و ورود پول هوشمند به سهم"
            }, 
            "الگوی کف دوقلو + شکست گردن": {
                "func": _check_double_bottom_pattern, 
                "args": [df['close'].values, df['high'].values, df['volume'].values], 
                "score": 15, "category": "الگوهای کلاسیک", "reason": "تشکیل الگوی کف دوقلو و شکست خط گردن"
            }, 
            "شکست خط روند نزولی با کندل تایید": {
                "func": _check_descending_trendline_breakout, 
                "args": [df['close'].values, df['high'].values, df['low'].values, df['volume'].values], 
                "score": 13, "category": "روند قیمت", "reason": "شکست خط روند نزولی با کندل تأییدکننده"
            }, 
            "واگرایی مکدی + تقاطع صعودی": {
                "func": lambda macd, signal, close_series: is_macd_buy_signal(macd, signal) and \
                                                        (close_series.iloc[-1] < close_series.iloc[-2] and macd.iloc[-1] > macd.iloc[-2]), 
                "args": [macd_line, signal_line, df['close']], 
                "score": 14, "category": "واگرایی", "reason": "واگرایی مثبت MACD و تقاطع صعودی خط سیگنال"
            }, 
            "عبور RSI از ناحیه اشباع فروش": {
                "func": lambda rsi_val, current_close_val, prev_close_val: pd.notna(rsi_val) and rsi_val > 30 and \
                                                                    (calculate_rsi(df['close'].iloc[:-1]).iloc[-1] <= 30 if len(df['close']) >= 2 else False) and \
                                                                    current_close_val > prev_close_val, 
                "args": [latest_rsi, current_close, df['close'].iloc[-2] if len(df['close']) >= 2 else np.nan], 
                "score": 11, "category": "روند قیمت", "reason": "عبور RSI از ناحیه اشباع فروش با افزایش قیمت"
            }, 
            "میانگین حجم ماه بالاتر از میانگین ۶ماهه + کندل صعودی": {
                "func": _check_monthly_volume_vs_six_month_avg, 
                "args": [df['volume'].values, today_candle_data], 
                "score": 9, "category": "حجم", "reason": "میانگین حجم ماه جاری بالاتر از میانگین ۶ ماهه با کندل صعودی قوی"
            },

            "حمایت شکسته": {"func": is_support_breakdown, "args": [df['low'], current_close], "score": -8, "category": "روند قیمت", "reason": "شکست حمایت مهم"}, 
            "RSI اشباع خرید": {"func": is_rsi_overbought, "args": [latest_rsi], "score": -10, "category": "روند قیمت", "reason": "شاخص قدرت نسبی (RSI) بالای ۷۰ است."},
            "تقاطع MACD نزولی": {"func": is_macd_sell_signal, "args": [macd_line, signal_line], "score": -12, "category": "واگرایی", "reason": "تقاطع نزولی MACD"},
        }

        for filter_name, filter_info in filter_definitions.items():
            try:
                filter_passed = filter_info["func"](*filter_info["args"])
                logger.debug(f"    Filter '{filter_name}' evaluation for {symbol_name}: Result={filter_passed}")

                if filter_passed:
                    satisfied_filters.append(filter_name)
                    total_score += filter_info["score"]
                    reason_phrases.append(filter_info["reason"])
                    logger.debug(f"    Filter '{filter_name}' SATISFIED for {symbol_name}. Score added: {filter_info['score']}")
                else:
                    logger.debug(f"    Filter '{filter_name}' NOT SATISFIED for {symbol_name}.")

            except Exception as e:
                logger.warning(f"Error applying filter '{filter_name}' for {symbol_name}: {e}", exc_info=True)
        
        # Initial reason string without status
        initial_reason_str = ", ".join(reason_phrases) if reason_phrases else "بدون دلیل خاص"
        
        symbol_result_data = {
            "symbol_id": symbol_id,
            "symbol_name": symbol_name,
            "jdate": today_jdate_str,
            "score": total_score,
            "satisfied_filters": json.dumps(satisfied_filters),
            "reason": initial_reason_str, # Store the initial reason string
            "profit_loss_percentage": 0.0, 
            "recommendation_price": current_close,
            "recommendation_jdate": today_jdate_str,
            "final_price": current_close,
            "status": "active", 
            "probability_percent": 0.0, 
            "timestamp": datetime.now()
        }
        current_day_results.append(symbol_result_data)
        logger.debug(f"Analyzed {symbol_name}: Score={total_score}, Filters={satisfied_filters}")

    current_day_results.sort(key=lambda x: x['score'], reverse=True)

    # --- NEW DEBUG LOGGING (Before DB Save) ---
    logger.info("--- Top 10 Symbols after analysis and sorting (before DB save) ---")
    for idx, res in enumerate(current_day_results[:10]):
        logger.info(f"  Rank {idx+1}: Symbol: {res['symbol_name']}, Score: {res['score']}, Filters: {json.loads(res['satisfied_filters'])}, Proposed is_golden_key: {idx < top_n_symbols}")
    logger.info("-----------------------------------------------------------------")
    # --- END NEW DEBUG LOGGING ---

    new_results_count = 0
    updated_results_count = 0

    for i, result_data in enumerate(current_day_results):
        logger.debug(f"Processing for DB save: Symbol: {result_data['symbol_name']}, Index: {i}, Score: {result_data['score']}")
        existing_result = GoldenKeyResult.query.filter_by(
            symbol_id=result_data['symbol_id'],
            jdate=result_data['jdate']
        ).first()

        is_golden_key_flag = False
        signal_status = "❌ سیگنال ضعیف یا بی‌اثر"

        # A symbol is a "Golden Key" if it's in the top N, regardless of its score.
        if i < top_n_symbols: 
            is_golden_key_flag = True
            if result_data['score'] >= 50:
                signal_status = "📈 سیگنال قوی خرید"
            elif result_data['score'] >= 30: 
                signal_status = "⚠️ احتمال رشد"
            # If score is < 30, it will remain "❌ سیگنال ضعیف یا بی‌اثر" but still be is_golden_key=True
            logger.debug(f"  {result_data['symbol_name']} qualifies for is_golden_key=True. Score: {result_data['score']}, Index: {i}, Top N: {top_n_symbols}. Final status: {signal_status}")
        else:
            logger.debug(f"  {result_data['symbol_name']} does NOT qualify for is_golden_key=True. Score: {result_data['score']}, Index: {i}, Top N: {top_n_symbols}.")

        # Prepare the final reason string by prepending the status
        final_reason_parts = []
        if result_data['reason'] and result_data['reason'] != "بدون دلیل خاص":
            final_reason_parts = [r.strip() for r in result_data['reason'].split(',') if r.strip()]
        
        final_reason_parts.insert(0, f"وضعیت سیگنال: {signal_status}")
        final_reason_str = ", ".join(final_reason_parts)

        if existing_result:
            existing_result.score = result_data['score']
            existing_result.satisfied_filters = result_data['satisfied_filters']
            existing_result.reason = final_reason_str # Use the final reason string
            existing_result.is_golden_key = is_golden_key_flag # Explicitly set the flag
            existing_result.recommendation_price = result_data['recommendation_price']
            existing_result.recommendation_jdate = result_data['recommendation_jdate']
            existing_result.final_price = result_data['final_price']
            existing_result.status = signal_status # Explicitly set the status
            existing_result.timestamp = datetime.now()
            db.session.add(existing_result)
            updated_results_count += 1
            logger.debug(f"  Updated existing result for {result_data['symbol_name']}. is_golden_key={is_golden_key_flag}, status={signal_status}")
        else:
            new_result = GoldenKeyResult(
                symbol_id=result_data['symbol_id'],
                symbol_name=result_data['symbol_name'],
                jdate=result_data['jdate'],
                score=result_data['score'],
                satisfied_filters=result_data['satisfied_filters'],
                reason=final_reason_str, # Use the final reason string
                profit_loss_percentage=result_data['profit_loss_percentage'],
                recommendation_price=result_data['recommendation_price'],
                recommendation_jdate=result_data['recommendation_jdate'],
                final_price=result_data['final_price'],
                status=signal_status, # Explicitly set the status for new record
                probability_percent=result_data['probability_percent'],
                is_golden_key=is_golden_key_flag, # Explicitly set the flag for new record
                timestamp=datetime.now()
            )
            db.session.add(new_result)
            new_results_count += 1
            logger.debug(f"  Created new result for {result_data['symbol_name']}. is_golden_key={is_golden_key_flag}, status={signal_status}")
        
    try:
        db.session.commit()
        # --- NEW LOGGING AFTER COMMIT ---
        logger.info("--- Verifying is_golden_key status in DB after commit ---")
        committed_golden_keys = GoldenKeyResult.query.filter_by(jdate=today_jdate_str, is_golden_key=True).order_by(GoldenKeyResult.score.desc()).all()
        if committed_golden_keys:
            for idx, gk_res in enumerate(committed_golden_keys):
                logger.info(f"  Committed Golden Key {idx+1}: Symbol: {gk_res.symbol_name}, Score: {gk_res.score}, Is Golden Key: {gk_res.is_golden_key}, Status: {gk_res.status}, Reason: {gk_res.reason}")
        else:
            logger.info(f"  No Golden Key results found in DB for date {today_jdate_str} after commit. This is unexpected if top N symbols had positive scores or if top N was intended to always mark some as True.")
        logger.info("----------------------------------------------------------")
        # --- END NEW LOGGING AFTER COMMIT ---

        message = f"Golden Key analysis completed. New results: {new_results_count}, Updated: {updated_results_count}. Top {top_n_symbols} symbols flagged as Golden Key."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving Golden Key results: {e}", exc_info=True)
        return False, f"Error saving Golden Key results: {str(e)}"


def get_golden_key_results(filters=None):
    """
    Retrieves Golden Key results based on filters.
    If no filters are provided, it returns the top N (is_golden_key=True) symbols for the latest date.
    If filters are provided, it returns all symbols for the latest date that match the filters.
    """
    logger.info(f"Fetching Golden Key results with filters: {filters}")
    
    query = GoldenKeyResult.query

    latest_date_result = db.session.query(func.max(GoldenKeyResult.jdate)).scalar()
    
    if latest_date_result:
        query = query.filter_by(jdate=latest_date_result)
        last_updated_display = latest_date_result
        logger.info(f"Latest Golden Key results date: {latest_date_result}")
    else:
        logger.warning("No Golden Key results found in database for any date.")
        last_updated_display = "نامشخص"
        return {
            "top_stocks": [],
            "technical_filters": get_golden_key_filter_definitions(),
            "last_updated": last_updated_display
        }

    satisfied_filters_list_from_param = [] 
    if filters:
        satisfied_filters_list_from_param = [f.strip() for f in filters.split(',') if f.strip()]
        logger.info(f"Applying Golden Key filters: {satisfied_filters_list_from_param}")
        
        all_latest_results = query.all() 
        filtered_results = []
        for r in all_latest_results:
            r_satisfied_filters_from_db = json.loads(r.satisfied_filters) if r.satisfied_filters else []
            logger.debug(f"  Symbol: {r.symbol_name}, Stored Filters: {r_satisfied_filters_from_db}")
            
            # Check if ALL requested filters are present in the signal's satisfied_filters
            if all(f in r_satisfied_filters_from_db for f in satisfied_filters_list_from_param):
                filtered_results.append(r)
                logger.debug(f"    MATCH: {r.symbol_name} matches all requested filters.")
            else:
                logger.debug(f"    NO MATCH: {r.symbol_name} does not match all requested filters.")
        
        results = filtered_results
        results.sort(key=lambda x: x.score, reverse=True)

    else:
        # This part is crucial: if no filters, we explicitly look for is_golden_key=True
        # Now, top_n_symbols are always flagged as is_golden_key=True by run_golden_key_analysis_and_save
        results = query.filter_by(is_golden_key=True).order_by(GoldenKeyResult.score.desc()).all()
        if not results:
            logger.info(f"No symbols flagged as 'is_golden_key=True' for date {latest_date_result}. This indicates that no symbols met the 'top_n_symbols' criterion during analysis. Returning top {top_n_symbols} by score instead (these will still show is_golden_key=False if they weren't in the original top N).")
            # Fallback to top N by score if no is_golden_key=True records exist for the latest date
            results = query.order_by(GoldenKeyResult.score.desc()).limit(8).all() # Use a default limit like 8 or a configurable one
        else:
            logger.info(f"Found {len(results)} symbols flagged as 'is_golden_key=True' for date {latest_date_result}.")

    output = []
    for r in results:
        r_satisfied_filters_list = json.loads(r.satisfied_filters) if r.satisfied_filters else []
        logger.debug(f"Retrieving from DB: Symbol: {r.symbol_name}, Score: {r.score}, Is Golden Key: {r.is_golden_key}, Status: {r.status}")
        output.append({
            "symbol": r.symbol_name, 
            "name": r.symbol_name,   
            "symbol_id": r.symbol_id,
            "symbol_name": r.symbol_name,
            "jdate": r.jdate,  
            "satisfied_filters": r.satisfied_filters,  
            "satisfied_filters_list": r_satisfied_filters_list,  
            "total_score": r.score,
            "reason": r.reason,
            # Use profit_loss_percentage only if status is closed, otherwise None
            "weekly_growth": r.profit_loss_percentage if r.status and "closed" in r.status else None, 
            "entry_price": r.recommendation_price,
            "jentry_date": r.recommendation_jdate,
            "exit_price": r.final_price if r.status and "closed" in r.status else None,
            "jexit_date": r.jexit_date if r.status and "closed" in r.status else None, 
            "profit_loss_percentage": r.profit_loss_percentage if r.status and "closed" in r.status else None,
            "is_golden_key": r.is_golden_key,
            "status": r.status,
            "probability_percent": r.probability_percent,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None
        })
    
    logger.info(f"Returning {len(output)} Golden Key results for display.")
    all_filter_definitions = get_golden_key_filter_definitions()

    return {
        "top_stocks": output,
        "technical_filters": all_filter_definitions,
        "last_updated": last_updated_display
    }

def get_golden_key_filter_definitions():
    """
    Returns a static list of all defined Golden Key filters with their categories and descriptions.
    This list should match the filters used in run_golden_key_analysis and save.
    """
    return [
        {"name": "فیلتر شکست مقاومت + عبور از MA50", "category": "روند قیمت", "description": "قیمت پایانی بالاتر از بیشینه ۲۰ روزه و عبور از میانگین متحرک ۵۰ روزه."},
        {"name": "واگرایی مثبت RSI + افزایش حجم", "category": "واگرایی", "description": "قیمت کف پایین‌تر ساخته ولی RSI کف پایین‌تر نساخته و حجم معاملات امروز > ۲ برابر میانگین ۵ روزه."},
        {"name": "تقاطع طلایی MA20/MA50", "category": "میانگین‌ها", "description": "MA20 از پایین MA50 را قطع کند و قیمت بالاتر از هر دو میانگین باشد."},
        {"name": "کندل چکشی یا دوجی با حجم بالا در کف", "category": "الگوهای کلاسیک", "description": "تشکیل کندل چکشی یا دوجی در انتهای روند نزولی با حجم زیاد."},
        {"name": "افزایش قدرت خریدار حقیقی + ورود پول", "category": "جریان وجوه", "description": "سرانه خرید حقیقی > ۲ برابر سرانه فروش و مجموع خرید حقیقی امروز > میانگین ماهانه."},
        {"name": "الگوی کف دوقلو + شکست گردن", "category": "الگوهای کلاسیک", "description": "دو کف مشابه طی ۲ هفته اخیر و شکست مقاومت بین دو کف با حجم بالا."}, 
        {"name": "شکست خط روند نزولی با کندل تایید", "category": "روند قیمت", "description": "عبور قیمت از خط روند نزولی رسم‌شده و کندل صعودی تأییدکننده با حجم بالا."}, 
        {"name": "واگرایی مکدی + تقاطع صعودی", "category": "واگرایی", "description": "MACD خط سیگنال را از پایین به بالا قطع کند و مکدی واگرایی مثبت با قیمت نشان دهد."},
        {"name": "عبور RSI از ناحیه اشباع فروش", "category": "روند قیمت", "description": "RSI دیروز < ۳۰ و امروز > ۳۰ و قیمت آخرین > قیمت دیروز."},
        {"name": "میانگین حجم ماه بالاتر از میانگین ۶ماهه + کندل صعودی", "category": "حجم", "description": "میانگین حجم ماه جاری بالاتر از میانگین ۶ ماهه با کندل صعودی قوی."},
        
        {"name": "حمایت شکسته", "category": "روند قیمت", "description": "شکست حمایت مهم"}, 
        {"name": "RSI اشباع خرید", "category": "روند قیمت", "description": "شاخص قدرت نسبی (RSI) بالای ۷۰ است."},
        {"name": "تقاطع MACD نزولی", "category": "واگرایی", "description": "خط MACD از خط سیگنال به سمت پایین عبور کرده است."},
    ]

def calculate_golden_key_win_rate():
    """
    Calculates the win rate for Golden Key signals.
    Updates the status of signals older than 7 days to 'closed_profit' or 'closed_loss'
    based on their actual performance (final_price vs recommendation_price).
    This function is intended to run on Wednesday night.
    """
    logger.info("Starting Golden Key Win-Rate calculation and signal status update.")
    
    today_gregorian = datetime.now().date()
    today_jdate_obj = jdatetime.date.fromgregorian(date=today_gregorian)
    today_jdate_str = today_jdate_obj.strftime('%Y-%m-%d')

    # Fetch all active Golden Key signals that were recommended up to 7 days ago
    # We want to close signals that have been active for at least 7 days
    seven_days_ago_greg = today_gregorian - timedelta(days=7)
    seven_days_ago_jdate_obj = jdatetime.date.fromgregorian(date=seven_days_ago_greg)
    seven_days_ago_jdate_str = seven_days_ago_jdate_obj.strftime('%Y-%m-%d')


    active_golden_key_signals = GoldenKeyResult.query.filter(
        GoldenKeyResult.status == 'active',
        GoldenKeyResult.is_golden_key == True, # Only evaluate signals that were actually Golden Key
        GoldenKeyResult.recommendation_jdate <= seven_days_ago_jdate_str # Signals older than or equal to 7 days
    ).all()

    total_signals_evaluated = len(active_golden_key_signals)
    closed_signals_count = 0
    successful_closed_signals = 0
    total_profit_percent_closed = 0.0
    total_loss_percent_closed = 0.0

    if total_signals_evaluated == 0:
        logger.info("No active Golden Key signals older than 7 days found to evaluate win rate.")
        # We still need to update aggregated performance even if no signals were closed
        update_aggregated_performance_for_today(0, 0, 0.0, 0.0, 0.0)
        return True, "No active Golden Key signals found to evaluate win rate."

    for signal in active_golden_key_signals:
        try:
            rec_jdate_obj = jdatetime.date.strptime(signal.recommendation_jdate, '%Y-%m-%d')
            rec_gregorian_date = rec_jdate_obj.togregorian()
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing recommendation_jdate '{signal.recommendation_jdate}' for signal {signal.symbol_name}: {e}. Skipping signal evaluation.", exc_info=True)
            continue 

        # Fetch latest historical data for the symbol
        latest_historical_data = HistoricalData.query.filter_by(symbol_id=signal.symbol_id)\
                                        .order_by(HistoricalData.jdate.desc()).first()
            
        current_price = None
        if latest_historical_data:
            current_price = normalize_value(latest_historical_data.final)
            if current_price is None or current_price <= 0:
                current_price = normalize_value(latest_historical_data.close)

        if current_price is None or current_price <= 0:
            logger.warning(f"  Could not get valid current price for signal {signal.symbol_name} (ID: {signal.id}). Setting to 'closed_neutral'.")
            signal.status = "closed_neutral"
            signal.profit_loss_percentage = 0.0
            signal.final_price = signal.recommendation_price # Keep final price same as recommendation if no market data
            signal.jexit_date = today_jdate_str
            signal.exit_date = today_gregorian
            closed_signals_count += 1
            db.session.add(signal)
            
            # Add to SignalsPerformance
            existing_perf = SignalsPerformance.query.filter_by(signal_id=str(signal.id)).first() # Use signal.id as signal_id
            if not existing_perf:
                new_perf = SignalsPerformance(
                    signal_id=str(signal.id), # Use the ID from GoldenKeyResult
                    symbol_id=signal.symbol_id,
                    symbol_name=signal.symbol_name,
                    signal_source='Golden Key',
                    entry_date=rec_gregorian_date,
                    jentry_date=signal.recommendation_jdate,
                    entry_price=signal.recommendation_price,
                    outlook="نامشخص", # Default outlook for performance
                    reason=signal.reason,
                    probability_percent=signal.probability_percent,
                    exit_date=today_gregorian,
                    jexit_date=today_jdate_str,
                    exit_price=signal.recommendation_price,
                    profit_loss_percent=0.0,
                    status="closed_neutral",
                    created_at=datetime.now(),
                    evaluated_at=datetime.now()
                )
                db.session.add(new_perf)
                logger.info(f"  Created new SignalsPerformance record for Golden Key signal {signal.symbol_name} (ID: {signal.id}) as closed_neutral.")
            else:
                existing_perf.exit_date = today_gregorian
                existing_perf.jexit_date = today_jdate_str
                existing_perf.exit_price = signal.recommendation_price
                existing_perf.profit_loss_percent = 0.0
                existing_perf.status = "closed_neutral"
                existing_perf.evaluated_at = datetime.now()
                db.session.add(existing_perf)
                logger.info(f"  Updated existing SignalsPerformance record for Golden Key signal {signal.symbol_name} (ID: {signal.id}) to closed_neutral.")

            continue # Skip to next signal if price is invalid

        profit_loss = ((current_price - signal.recommendation_price) / signal.recommendation_price) * 100 if signal.recommendation_price and signal.recommendation_price != 0 else 0.0
        
        signal.profit_loss_percentage = profit_loss
        signal.final_price = current_price
        signal.jexit_date = today_jdate_str
        signal.exit_date = today_gregorian 

        if profit_loss >= 5.0: # Example profit target for closing
            signal.status = "closed_profit"
            successful_closed_signals += 1
            total_profit_percent_closed += profit_loss
        elif profit_loss <= -3.0: # Example stop loss for closing
            signal.status = "closed_loss"
            total_loss_percent_closed += profit_loss
        else: # If not hitting target/stop-loss, but older than 7 days, close as neutral
            signal.status = "closed_neutral"
        
        closed_signals_count += 1
        logger.info(f"  Signal {signal.symbol_name} (ID: {signal.id}) closed. Profit/Loss: {profit_loss:.2f}%, Status: {signal.status}")
        
        db.session.add(signal) 

        # Add or update to SignalsPerformance table
        existing_performance = SignalsPerformance.query.filter_by(signal_id=str(signal.id)).first() # Use signal.id
        if existing_performance:
            existing_performance.exit_date = signal.exit_date
            existing_performance.jexit_date = signal.jexit_date
            existing_performance.exit_price = signal.final_price
            existing_performance.profit_loss_percent = signal.profit_loss_percentage
            existing_performance.status = signal.status
            existing_performance.evaluated_at = datetime.now()
            db.session.add(existing_performance)
            logger.info(f"Updated SignalsPerformance record for Golden Key signal {signal.symbol_name} (ID: {signal.id}).")
        else:
            new_performance_record = SignalsPerformance(
                signal_id=str(signal.id), # Use the ID from GoldenKeyResult
                symbol_id=signal.symbol_id, 
                symbol_name=signal.symbol_name,
                signal_source='Golden Key', 
                entry_date=rec_gregorian_date,
                jentry_date=signal.recommendation_jdate,
                entry_price=signal.recommendation_price,
                outlook="نامشخص", # Default outlook for performance
                reason=signal.reason,
                probability_percent=signal.probability_percent,
                exit_date=signal.exit_date,
                jexit_date=signal.jexit_date,
                exit_price=signal.final_price,
                profit_loss_percent=signal.profit_loss_percentage,
                status=signal.status,
                created_at=datetime.now(),
                evaluated_at=datetime.now()
            )
            db.session.add(new_performance_record)
            logger.info(f"Created new SignalsPerformance record for Golden Key signal {signal.symbol_name} (ID: {signal.id}).")


    try:
        db.session.commit()
        message = f"Golden Key Win-Rate calculation and status update completed. Closed {closed_signals_count} signals."
        logger.info(message)
    except Exception as e:
        db.session.rollback()
        error_message = f"Error committing Golden Key Win-Rate updates: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

    win_rate_closed = (successful_closed_signals / closed_signals_count) * 100 if closed_signals_count > 0 else 0

    # Trigger aggregated performance calculation for Golden Key
    if hasattr(current_app, 'performance_service') and hasattr(current_app.performance_service, 'calculate_and_save_aggregated_performance'):
        success_agg, msg_agg = current_app.performance_service.calculate_and_save_aggregated_performance(
            period_type='weekly', 
            signal_source='Golden Key'
        )
        logger.info(f"Aggregated performance for Golden Key: {msg_agg}")
    else:
        logger.warning("performance_service.calculate_and_save_aggregated_performance not found. Aggregated performance for Golden Key not updated.")

    # Also trigger an overall aggregation for the app's performance
    if hasattr(current_app, 'performance_service') and hasattr(current_app.performance_service, 'calculate_and_save_aggregated_performance'):
        success_overall_agg, msg_overall_agg = current_app.performance_service.calculate_and_save_aggregated_performance(
            period_type='weekly', 
            signal_source='overall' # For overall app performance
        )
        logger.info(f"Aggregated overall performance (weekly): {msg_overall_agg}")
    
    return True, message 


def update_aggregated_performance_for_today(total_signals, successful_signals, win_rate, total_profit_percent, total_loss_percent):
    """
    Helper function to update or create aggregated performance record for today.
    """
    today_jdate_str = get_today_jdate_str()
    period_type = "weekly" 
    signal_source = "golden_key"

    existing_agg_perf = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type=period_type,
        signal_source=signal_source
    ).first()

    if existing_agg_perf:
        existing_agg_perf.total_signals = total_signals
        existing_agg_perf.successful_signals = successful_signals
        existing_agg_perf.win_rate = win_rate
        existing_agg_perf.total_profit_percent = total_profit_percent
        existing_agg_perf.total_loss_percent = total_loss_percent
        existing_agg_perf.updated_at = datetime.now()
        db.session.add(existing_agg_perf)
        logger.info(f"Updated aggregated performance for {signal_source} ({period_type}) on {today_jdate_str}.")
    else:
        new_agg_perf = AggregatedPerformance(
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
        db.session.add(new_agg_perf)
        logger.info(f"Created new aggregated performance record for {signal_source} ({period_type}) on {today_jdate_str}.")
    
    try:
        db.session.commit()
        logger.info("Aggregated performance record committed successfully.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error committing aggregated performance record: {e}", exc_info=True)