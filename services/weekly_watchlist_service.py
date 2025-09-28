# -*- coding: utf-8 -*-
# services/weekly_watchlist_service.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData, WeeklyWatchlistResult, SignalsPerformance, AggregatedPerformance, GoldenKeyResult
from flask import current_app
import pandas as pd
from datetime import datetime, timedelta, date
import jdatetime
import uuid
from sqlalchemy import func, text
import logging
import json
import numpy as np
from types import SimpleNamespace


# NEW: Import for market sentiment analysis
from services.iran_market_data import fetch_iran_market_indices

# Import utility functions
from services.utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns, check_tsetmc_filters, check_financial_ratios, convert_gregorian_to_jalali, calculate_z_score

# Import analysis_service for aggregated performance calculation
from services import analysis_service

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# IMPROVEMENT: Lookback period and minimum history days adjusted for better indicator quality
TECHNICAL_DATA_LOOKBACK_DAYS = 120
MIN_REQUIRED_HISTORY_DAYS = 50

# REVISED: New filter weights for a predictive scoring algorithm
FILTER_WEIGHTS = {
    # --- High-Impact Leading & Breakout Signals ---
    "RSI_Positive_Divergence": 5,
    "Resistance_Broken": 5,
    "Squeeze_Momentum_Fired_Long": 4,
    "Stochastic_Bullish_Cross_Oversold": 4,
    "Consolidation_Breakout_Candidate": 3,
    "Bollinger_Lower_Band_Touch": 2,

    # --- Trend Confirmation Signals ---
    "MACD_Bullish_Cross_Confirmed": 2,
    "HalfTrend_Buy_Signal": 2,
    "High_Volume_On_Up_Day": 2,
    "Positive_Real_Money_Flow_Trend_10D": 3,
    "Price_Above_SMA50": 1,

    # --- Fundamental Quality Filters ---
    "High_ROE": 2,
    "Reasonable_PE": 1,
    "Reasonable_PS": 1,
    "Reasonable_PB": 1,
    "Positive_EPS": 1,


    # --- candlestick filters Filters ---
    "Bullish_Engulfing_Detected": 3,
    "Hammer_Detected": 3,
    "Morning_Star_Detected": 4,
    "ML_Predicts_Uptrend": 4,

    # --- Penalties & Negative Scores (Crucial for avoiding peaks) ---
    "RSI_Is_Overbought": -4,
    "Price_Too_Stretched_From_SMA50": -3,
    "Negative_Real_Money_Flow_Trend_10D": -2,
}

# کمک‌کننده: بازگرداندن یک سری close قابل‌اعتماد از historical DF
def _get_close_series_from_hist_df(hist_df):
    """
    Accepts a historical dataframe and returns a numeric pandas Series of close prices.
    Tries common column names: 'close_price', 'close', 'final'
    """
    if hist_df is None or hist_df.empty:
        return pd.Series(dtype=float)

    for col in ['close_price', 'close', 'final']:
        if col in hist_df.columns:
            ser = pd.to_numeric(hist_df[col], errors='coerce').dropna()
            if not ser.empty:
                return ser
    return pd.Series(dtype=float)



# --- NEW: Market Sentiment Analysis Function ---
def _get_market_sentiment() -> str:
    """
    Determines short-term market sentiment based on the daily change of major indices.
    Returns: 'Bullish', 'Neutral', or 'Bearish'.
    """
    try:
        indices_data = fetch_iran_market_indices()
        total_index = indices_data.get("Total_Index", {})
        equal_weighted_index = indices_data.get("Equal_Weighted_Index", {})

        total_percent = total_index.get("percent", 0) or 0
        equal_percent = equal_weighted_index.get("percent", 0) or 0

        if total_percent > 0.3 and equal_percent > 0.3:
            logger.info(f"Market Sentiment: Bullish (Total: {total_percent}%, Equal: {equal_percent}%)")
            return "Bullish"
        elif total_percent < -0.3 and equal_percent < -0.3:
            logger.info(f"Market Sentiment: Bearish (Total: {total_percent}%, Equal: {equal_percent}%)")
            return "Bearish"
        else:
            logger.info(f"Market Sentiment: Neutral (Total: {total_percent}%, Equal: {equal_percent}%)")
            return "Neutral"
    except Exception as e:
        logger.error(f"Could not fetch market sentiment, defaulting to Neutral: {e}")
        return "Neutral"

# --- REVISED: Filter Functions ---

def _check_market_condition_filters(hist_df, tech_df):
    """
    Checks for individual stock conditions like overbought state or consolidation.
    """
    satisfied_filters, reason_parts = [], {"market_condition": []}
    if tech_df is None or tech_df.empty or hist_df is None or len(hist_df) < 50:
        return satisfied_filters, reason_parts

    last_tech = tech_df.iloc[-1]
    close_ser = _get_close_series_from_hist_df(hist_df)
    if close_ser.empty:
        return satisfied_filters, reason_parts
    last_close = close_ser.iloc[-1]
    last_hist = hist_df.iloc[-1]

    # --- Check 1: RSI Overbought Condition (Penalize only if weak) ---
    if hasattr(last_tech, 'RSI') and last_tech.RSI is not None and last_tech.RSI > 70:
        # Check for negative divergence (if available)
        is_negative_divergence = (
            hasattr(last_tech, 'RSI_Divergence') and last_tech.RSI_Divergence == "Negative"
        )

        # Compare last day volume with 10-day average
        historical_volume_series = hist_df.tail(10)['volume']
        average_volume = historical_volume_series.mean() if not historical_volume_series.empty else 0
        is_high_volume = last_hist['volume'] > average_volume * 1.5 if average_volume > 0 else False

        if is_negative_divergence or not is_high_volume:
            satisfied_filters.append("RSI_Is_Overbought")
            reason_parts["market_condition"].append(
                f"RSI ({last_tech.RSI:.2f}) overbought with weakness "
                "(negative divergence or low volume)."
            )
        else:
            reason_parts["market_condition"].append(
                f"RSI ({last_tech.RSI:.2f}) overbought but supported by strong volume (no penalty)."
            )

    # --- Check 2: Price is too far from its SMA50 (Penalize) ---
    if hasattr(last_tech, 'SMA_50') and last_tech.SMA_50 is not None and last_tech.SMA_50 > 0:
        stretch_percent = ((last_close - last_tech.SMA_50) / last_tech.SMA_50) * 100
        if stretch_percent > 20:
            satisfied_filters.append("Price_Too_Stretched_From_SMA50")
            reason_parts["market_condition"].append(
                f"Price is overextended ({stretch_percent:.1f}%) from SMA50."
            )

    # --- Check 3: Consolidation Pattern (Reward) ---
    if hasattr(last_tech, 'ATR'):
        atr_series = pd.to_numeric(tech_df['ATR'].dropna())
        if len(atr_series) > 30:
            recent_atr_avg = atr_series.tail(10).mean()
            historical_atr_avg = atr_series.tail(30).mean()
            if recent_atr_avg < (historical_atr_avg * 0.7):
                satisfied_filters.append("Consolidation_Breakout_Candidate")
                reason_parts["market_condition"].append(
                    "Stock is in a low-volatility consolidation phase."
                )

    return satisfied_filters, reason_parts







# تغییر در is_data_sufficient: انعطاف‌پذیرتر و مقاوم‌تر
def is_data_sufficient(data_df, min_len):
    """
    Checks if the provided DataFrame is not empty and has at least min_len records.
    
    Args:
        data_df (pd.DataFrame): The DataFrame of data records.
        min_len (int): The minimum required length for the data.
        
    Returns:
        bool: True if data is sufficient, False otherwise.
    """
    if data_df is None or data_df.empty:
        return False
    return len(data_df) >= min_len

def convert_jalali_to_gregorian_timestamp(jdate_str):
    """
    Converts a Jalali date string (YYYY-MM-DD) to a pandas Timestamp (Gregorian).
    Handles NaN/None values gracefully.
    """
    if pd.notna(jdate_str) and isinstance(jdate_str, str):
        try:
            jy, jm, jd = map(int, jdate_str.split('-'))
            gregorian_date = jdatetime.date(jy, jm, jd).togregorian()
            return pd.Timestamp(gregorian_date)
        except ValueError:
            return pd.NaT
    return pd.NaT




# تغییرات در _check_technical_filters: استفاده از سری close ایمن و محافظت در برابر KeyError
# کد بازبینی شده
# ----------------------------

def _check_technical_filters(hist_df, tech_df):
    """
    Checks technical indicators, including new leading indicators.
    """
    satisfied_filters, reason_parts = [], {"technical": []}
    if tech_df is None or tech_df.empty or len(tech_df) < 2:
        return satisfied_filters, reason_parts

    technical_rec = tech_df.iloc[-1]
    prev_tech_rec = tech_df.iloc[-2]
    
    close_ser = _get_close_series_from_hist_df(hist_df)
    last_close_val = close_ser.iloc[-1] if not close_ser.empty else None
    if last_close_val is None:
        return satisfied_filters, reason_parts

    # Helper function to safely get a value
    def get_attr_safe(rec, attr, default=None):
        val = getattr(rec, attr, default)
        if isinstance(val, (pd.Series, pd.DataFrame)):
            return val.iloc[0] if not val.empty else default
        return val

    # RSI Positive Divergence
    current_rsi = get_attr_safe(technical_rec, 'RSI')
    prev_rsi = get_attr_safe(prev_tech_rec, 'RSI')
    
    # اطمینان از وجود داده و مقایسه صحیح
    if current_rsi is not None and prev_rsi is not None and len(close_ser) > 1:
        # اینجا مقادیر به صورت عددی استخراج شده‌اند و مقایسه امن است
        if current_rsi > prev_rsi and close_ser.iloc[-1] < close_ser.iloc[-2]:
            satisfied_filters.append("RSI_Positive_Divergence")
            reason_parts["technical"].append(f"Positive divergence on RSI ({current_rsi:.2f}).")
            
    # MACD Cross
    current_macd = get_attr_safe(technical_rec, 'MACD')
    current_macd_signal = get_attr_safe(technical_rec, 'MACD_Signal')
    prev_macd = get_attr_safe(prev_tech_rec, 'MACD')
    prev_macd_signal = get_attr_safe(prev_tech_rec, 'MACD_Signal')
    if all(x is not None for x in [current_macd, current_macd_signal, prev_macd, prev_macd_signal]):
        if current_macd > current_macd_signal and prev_macd <= prev_macd_signal:
            satisfied_filters.append("MACD_Bullish_Cross_Confirmed")
            
    # Price vs SMA50
    sma50 = get_attr_safe(technical_rec, 'SMA_50')
    if sma50 is not None and last_close_val > sma50:
        satisfied_filters.append("Price_Above_SMA50")
        
    # Bollinger Lower Band
    bollinger_low = get_attr_safe(technical_rec, 'Bollinger_Low')
    if bollinger_low is not None and last_close_val < bollinger_low:
        satisfied_filters.append("Bollinger_Lower_Band_Touch")

    # IMPROVED: Volume Analysis
    if 'volume' in hist_df.columns and len(hist_df) >= 20 and len(close_ser) > 1:
        volume_z_score = calculate_z_score(pd.to_numeric(hist_df['volume'], errors='coerce').dropna().iloc[-20:])
        if volume_z_score is not None and volume_z_score > 1.5 and close_ser.iloc[-1] > close_ser.iloc[-2]:
            satisfied_filters.append("High_Volume_On_Up_Day")
            reason_parts["technical"].append(f"High volume (Z-Score: {volume_z_score:.2f}) on a positive day.")
            
    # NEW: Stochastic Oscillator
    current_stoch_k = get_attr_safe(technical_rec, 'Stochastic_K')
    current_stoch_d = get_attr_safe(technical_rec, 'Stochastic_D')
    prev_stoch_k = get_attr_safe(prev_tech_rec, 'Stochastic_K')
    prev_stoch_d = get_attr_safe(prev_tech_rec, 'Stochastic_D')
    if all(x is not None for x in [current_stoch_k, current_stoch_d, prev_stoch_k, prev_stoch_d]):
        if current_stoch_k > current_stoch_d and prev_stoch_k <= prev_stoch_d and current_stoch_d < 25:
            satisfied_filters.append("Stochastic_Bullish_Cross_Oversold")
            reason_parts["technical"].append("Stochastic bullish cross in oversold area.")
            
    # NEW: Squeeze Momentum
    current_squeeze_on = get_attr_safe(technical_rec, 'squeeze_on')
    prev_squeeze_on = get_attr_safe(prev_tech_rec, 'squeeze_on')
    if current_squeeze_on == False and prev_squeeze_on == True:
        satisfied_filters.append("Squeeze_Momentum_Fired_Long")
        reason_parts["technical"].append("Squeeze Momentum indicator fired long.")
        
    # NEW: HalfTrend
    current_halftrend = get_attr_safe(technical_rec, 'halftrend_signal')
    prev_halftrend = get_attr_safe(prev_tech_rec, 'halftrend_signal')
    if current_halftrend == 1 and prev_halftrend != 1:
        satisfied_filters.append("HalfTrend_Buy_Signal")
        
    # NEW: Support & Resistance Break
    resistance_broken = get_attr_safe(technical_rec, 'resistance_broken')
    if resistance_broken:
        satisfied_filters.append("Resistance_Broken")
        res_level = get_attr_safe(technical_rec, 'resistance_level_50d', 'N/A')
        reason_parts["technical"].append(f"Broke a key resistance level around {res_level}.")

    return satisfied_filters, reason_parts

def _check_fundamental_filters(fundamental_rec):
    satisfied_filters = []
    reason_parts = {"fundamental": []}
    if fundamental_rec:
        if fundamental_rec.pe is not None and 0 < fundamental_rec.pe < 20: satisfied_filters.append("Reasonable_PE")
        if fundamental_rec.ps is not None and 0 < fundamental_rec.ps < 5: satisfied_filters.append("Reasonable_PS")
        if fundamental_rec.pb is not None and 0 < fundamental_rec.pb < 2: satisfied_filters.append("Reasonable_PB")
        if fundamental_rec.roe is not None and fundamental_rec.roe > 15: satisfied_filters.append("High_ROE")
        if fundamental_rec.eps is not None and fundamental_rec.eps > 0: satisfied_filters.append("Positive_EPS")
    return satisfied_filters, reason_parts

def _check_smart_money_filters(hist_df):
    satisfied_filters = []
    reason_parts = {"smart_money": []}
    # IMPROVEMENT: Increased lookback to 10 days for more stable trend
    trend_lookback = 10
    if hist_df is None or hist_df.empty or 'buy_i_volume' not in hist_df.columns or len(hist_df) < trend_lookback:
        return satisfied_filters, reason_parts

    smart_money_flow_df = calculate_smart_money_flow(hist_df)
    if not smart_money_flow_df.empty and len(smart_money_flow_df) >= trend_lookback:
        trend_net_flow = smart_money_flow_df['individual_net_flow'].iloc[-trend_lookback:].sum()
        if trend_net_flow > 0:
            satisfied_filters.append("Positive_Real_Money_Flow_Trend_10D")
            reason_parts["smart_money"].append(f"Positive real money inflow over the last {trend_lookback} days.")
        elif trend_net_flow < 0:
            satisfied_filters.append("Negative_Real_Money_Flow_Trend_10D")

    return satisfied_filters, reason_parts

def _check_candlestick_filters(pattern_rec):
    """
    Checks for pre-detected bullish candlestick patterns from the database.
    Args:
        pattern_rec (CandlestickPatternDetection): The database record for today's pattern.
    Returns:
        Tuple[List[str], Dict]: A tuple of satisfied filters and reason parts.
    """
    satisfied_filters, reason_parts = [], {"candlestick": []}
    
    # If there is no pattern record for the symbol on the given day, return empty.
    if not pattern_rec:
        return satisfied_filters, reason_parts

    pattern_name = pattern_rec.pattern_name

    # Check for specific bullish patterns you have weights for
    if "Bullish Engulfing" in pattern_name:
        satisfied_filters.append("Bullish_Engulfing_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")
    
    if "Hammer" in pattern_name:
        satisfied_filters.append("Hammer_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")

    if "Morning Star" in pattern_name:
        satisfied_filters.append("Morning_Star_Detected")
        reason_parts["candlestick"].append(f"Detected: {pattern_name}")

    return satisfied_filters, reason_parts



def _check_advanced_fundamental_filters(ratios_df):
    """
    Analyzes historical financial ratios for positive trends.
    Args:
        ratios_df (pd.DataFrame): DataFrame of financial ratios for a single symbol.
    Returns:
        Tuple[List[str], Dict]: A tuple of satisfied filters and reason parts.
    """
    satisfied_filters, reason_parts = [], {"advanced_fundamental": []}
    if ratios_df is None or ratios_df.empty:
        return satisfied_filters, reason_parts

    # --- Check 1: Decreasing Debt to Equity ---
    debt_ratios = ratios_df[ratios_df['ratio_name'] == 'Debt to Equity'].sort_values('fiscal_year')
    
    # We need at least 2 years of data to see a trend
    if len(debt_ratios) >= 2:
        last_ratio = debt_ratios['ratio_value'].iloc[-1]
        prev_ratio = debt_ratios['ratio_value'].iloc[-2]
        
        # Check if the ratio has decreased
        if last_ratio < prev_ratio:
            satisfied_filters.append("Debt_To_Equity_Decreasing")
            reason_parts["advanced_fundamental"].append(f"Debt to Equity is decreasing (from {prev_ratio:.2f} to {last_ratio:.2f}).")
            
    return satisfied_filters, reason_parts        

def run_weekly_watchlist_selection():
    """
    Selects symbols for the weekly watchlist using a bulk data fetching and processing approach,
    incorporating a dynamic score threshold based on market sentiment.
    """
    logger.info("Starting Weekly Watchlist selection process.")

    # Step 1: Determine market sentiment for dynamic scoring
    market_sentiment = _get_market_sentiment()
    if market_sentiment == "Bullish":
        score_threshold = 7
    elif market_sentiment == "Neutral":
        score_threshold = 8
    else:  # Bearish
        score_threshold = 10
    logger.info(f"Market sentiment is '{market_sentiment}'. Score threshold set to >= {score_threshold}.")

    # Step 2: Bulk Data Fetching
    allowed_market_types = ['بورس', 'فرابورس', 'پایه فرابورس', 'بورس کالا', 'بورس انرژی']
    symbols_to_analyze = ComprehensiveSymbolData.query.filter(
        ComprehensiveSymbolData.market_type.in_(allowed_market_types)
    ).all()

    if not symbols_to_analyze:
        logger.warning("No symbols found for watchlist analysis. Skipping.")
        return False, "No symbols found for watchlist analysis."
    
    symbol_ids = [s.symbol_id for s in symbols_to_analyze]
    today_jdate = get_today_jdate_str()
    cutoff_date_j = (jdatetime.date.today() - jdatetime.timedelta(days=TECHNICAL_DATA_LOOKBACK_DAYS + 10)).strftime('%Y-%m-%d')
    
    logger.info(f"Fetching bulk data for {len(symbol_ids)} symbols for the last ~{TECHNICAL_DATA_LOOKBACK_DAYS+10} days...")
    
    try:
        historical_records = HistoricalData.query.filter(
            HistoricalData.symbol_id.in_(symbol_ids),
            HistoricalData.jdate >= cutoff_date_j
        ).all()
        hist_df = pd.DataFrame([rec.__dict__ for rec in historical_records])
        hist_df = hist_df.drop(columns=['_sa_instance_state'], errors='ignore')

        technical_records = TechnicalIndicatorData.query.filter(
            TechnicalIndicatorData.symbol_id.in_(symbol_ids),
            TechnicalIndicatorData.jdate >= cutoff_date_j
        ).all()
        tech_df = pd.DataFrame([rec.__dict__ for rec in technical_records])
        tech_df = tech_df.drop(columns=['_sa_instance_state'], errors='ignore')

        fundamental_records = FundamentalData.query.filter(FundamentalData.symbol_id.in_(symbol_ids)).all()
        
        # --- NEW: Fetch Candlestick, ML, and Financial Ratio data ---
        candlestick_records = CandlestickPatternDetection.query.filter(
            CandlestickPatternDetection.symbol_id.in_(symbol_ids),
            CandlestickPatternDetection.jdate == today_jdate
        ).all()

        ml_predictions = MLPrediction.query.filter(
            MLPrediction.symbol_id.in_(symbol_ids),
            MLPrediction.jprediction_date == today_jdate,
            MLPrediction.predicted_trend == 'Uptrend'
        ).all()
        
        financial_ratio_records = FinancialRatiosData.query.filter(
            FinancialRatiosData.symbol_id.in_(symbol_ids)
        ).all()
        financial_ratios_df = pd.DataFrame([rec.__dict__ for rec in financial_ratio_records])
        if not financial_ratios_df.empty:
            financial_ratios_df = financial_ratios_df.drop(columns=['_sa_instance_state'], errors='ignore')
        # --- END NEW ---

    except Exception as e:
        logger.error(f"❌ Error during bulk data fetching: {e}", exc_info=True)
        return False, "Data fetching failed."
    
    logger.info(f"Fetched {len(historical_records)} historical, {len(technical_records)} technical, and other related records.")
    
    # Step 3: Group data for efficient processing
    hist_groups = {
        k: v.sort_values(by='jdate')
        for k, v in hist_df.groupby("symbol_id")
    } if not hist_df.empty and 'symbol_id' in hist_df.columns else {}
    
    tech_groups = {
        k: v.sort_values(by='jdate')
        for k, v in tech_df.groupby("symbol_id")
    } if not tech_df.empty and 'symbol_id' in tech_df.columns else {}
    
    fundamental_map = {rec.symbol_id: rec for rec in fundamental_records}

    # --- NEW: Prepare newly fetched data ---
    candlestick_map = {rec.symbol_id: rec for rec in candlestick_records}
    ml_prediction_set = {rec.symbol_id for rec in ml_predictions}
    financial_ratios_groups = {
        k: v
        for k, v in financial_ratios_df.groupby("symbol_id")
    } if not financial_ratios_df.empty else {}
    # --- END NEW ---
    
    # Step 4: Process each symbol and score it
    watchlist_candidates = []
    for symbol in symbols_to_analyze:
        symbol_hist_df = hist_groups.get(symbol.symbol_id, pd.DataFrame()).copy()
        symbol_tech_df = tech_groups.get(symbol.symbol_id, pd.DataFrame()).copy()

        # Minimum data check
        if len(symbol_hist_df) < MIN_REQUIRED_HISTORY_DAYS:
            logger.debug(f"Skipping {symbol.symbol_name} due to insufficient historical rows.")
            continue
        
        # Fallback logic for technical data
        if symbol_tech_df.empty:
            last_close_series = _get_close_series_from_hist_df(symbol_hist_df)
            last_close = float(last_close_series.iloc[-1]) if not last_close_series.empty else None
            technical_rec = SimpleNamespace(
                close_price=last_close, MACD=None, MACD_Signal=None, RSI=None,
                SMA_20=None, SMA_50=None, Bollinger_Low=None, Bollinger_High=None, ATR=None
            )
        else:
            technical_rec = symbol_tech_df.iloc[-1]
        
        entry_price = getattr(technical_rec, 'close_price', None)
        if entry_price is None or pd.isna(entry_price):
            logger.warning(f"Skipping {symbol.symbol_name} due to missing entry price.")
            continue
            
        all_satisfied_filters = []
        all_reason_parts = {}

        # Run all filter checks
        tech_filters, tech_reasons = _check_technical_filters(symbol_hist_df, symbol_tech_df)
        all_satisfied_filters.extend(tech_filters)
        all_reason_parts.update(tech_reasons)
        
        fundamental_rec = fundamental_map.get(symbol.symbol_id)
        fund_filters, fund_reasons = _check_fundamental_filters(fundamental_rec)
        all_satisfied_filters.extend(fund_filters)
        all_reason_parts.update(fund_reasons)
        if not fundamental_rec:
            all_reason_parts["fundamental"] = ["No fundamental data available."]
            
        smart_money_filters, smart_money_reasons = _check_smart_money_filters(symbol_hist_df)
        all_satisfied_filters.extend(smart_money_filters)
        all_reason_parts.update(smart_money_reasons)

        mkt_cond_filters, mkt_cond_reasons = _check_market_condition_filters(symbol_hist_df, symbol_tech_df)
        all_satisfied_filters.extend(mkt_cond_filters)
        all_reason_parts.update(mkt_cond_reasons)

        # --- NEW: Run new filter checks ---
        pattern_rec = candlestick_map.get(symbol.symbol_id)
        candle_filters, candle_reasons = _check_candlestick_filters(pattern_rec)
        all_satisfied_filters.extend(candle_filters)
        all_reason_parts.update(candle_reasons)

        if symbol.symbol_id in ml_prediction_set:
            all_satisfied_filters.append("ML_Predicts_Uptrend")
            all_reason_parts.setdefault("ml_signal", []).append("ML model predicts a high-probability uptrend.")
            
        symbol_ratios_df = financial_ratios_groups.get(symbol.symbol_id)
        adv_fund_filters, adv_fund_reasons = _check_advanced_fundamental_filters(symbol_ratios_df)
        all_satisfied_filters.extend(adv_fund_filters)
        all_reason_parts.update(adv_fund_reasons)
        # --- END NEW ---

        # Calculate score and check against dynamic threshold
        score = sum(FILTER_WEIGHTS.get(f, 0) for f in all_satisfied_filters)

        if score >= score_threshold:
            watchlist_candidates.append({
                "symbol_id": symbol.symbol_id,
                "symbol_name": symbol.symbol_name,
                "entry_price": entry_price,
                "entry_date": date.today(),
                "jentry_date": get_today_jdate_str(),
                "outlook": "Bullish",
                "reason_json": json.dumps(all_reason_parts, ensure_ascii=False),
                "satisfied_filters": json.dumps(list(set(all_satisfied_filters)), ensure_ascii=False),
                "score": score
            })

    # Step 5: Sort candidates and save the top N to the database
    logger.info(f"Found {len(watchlist_candidates)} candidates meeting the threshold. Sorting and saving top 8.")
    watchlist_candidates.sort(key=lambda x: x['score'], reverse=True)
    final_watchlist = watchlist_candidates[:8]

    saved_count = 0
    for candidate in final_watchlist:
        existing_result = WeeklyWatchlistResult.query.filter_by(
            symbol_id=candidate['symbol_id'], jentry_date=candidate['jentry_date']
        ).first()

        if existing_result:
            existing_result.entry_price = candidate['entry_price']
            existing_result.outlook = candidate['outlook']
            existing_result.reason = candidate['satisfied_filters']
            existing_result.probability_percent = min(100, candidate['score'] * 5)
            existing_result.created_at = datetime.now()
        else:
            existing_result = WeeklyWatchlistResult(
                signal_unique_id=str(uuid.uuid4()),
                symbol_id=candidate['symbol_id'],
                symbol_name=candidate['symbol_name'],
                entry_price=candidate['entry_price'],
                entry_date=candidate['entry_date'],
                jentry_date=candidate['jentry_date'],
                outlook=candidate['outlook'],
                reason=candidate['satisfied_filters'],
                probability_percent=min(100, candidate['score'] * 5),
                status='active',
            )
        db.session.add(existing_result)
        saved_count += 1

    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. Saved top {saved_count} symbols."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error during DB commit: {e}", exc_info=True)
        return False, "Database commit failed."




#
# =================================================================================
# NEW: Performance Evaluation Logic (Replicated from Golden Key Service)
# =================================================================================

def _update_weekly_watchlist_performance(active_entries):
    """
    Updates the status and calculates P/L for active watchlist signals.
    If a signal meets exit criteria (TP, SL, Expired), its record is updated
    and a copy is saved to SignalsPerformance. This mirrors the logic of
    _update_golden_key_performance but is adapted for watchlist rules.
    """
    logger.info(f"Updating performance for {len(active_entries)} active watchlist signals.")
    updated_count = 0
    today_jdate_str = get_today_jdate_str()
    current_greg_date = datetime.now().date()

    for entry in active_entries:
        try:
            latest_historical = HistoricalData.query.filter_by(symbol_id=entry.symbol_id).order_by(HistoricalData.jdate.desc()).first()
            latest_technical = TechnicalIndicatorData.query.filter_by(symbol_id=entry.symbol_id).order_by(TechnicalIndicatorData.jdate.desc()).first()

            if not latest_historical or not latest_technical or latest_technical.ATR is None:
                logger.warning(f"Skipping update for {entry.symbol_name} due to missing data.")
                continue

            current_price = normalize_value(latest_historical.close)
            if current_price is None or current_price <= 0:
                continue

            # --- Define Exit Conditions based on Watchlist rules ---
            stop_loss_price = entry.entry_price - (1.5 * latest_technical.ATR)
            take_profit_price = entry.entry_price + (3 * latest_technical.ATR)
            
            entry_jdate = jdatetime.date(*map(int, entry.jentry_date.split('-')))
            days_passed = (jdatetime.date.today() - entry_jdate).days

            new_status = 'active'
            evaluation_reason = ""

            if current_price >= take_profit_price:
                new_status = 'closed_win'
                evaluation_reason = f"Hit Take Profit at {take_profit_price:.0f}"
            elif current_price <= stop_loss_price:
                new_status = 'closed_loss'
                evaluation_reason = f"Hit Stop Loss at {stop_loss_price:.0f}"
            elif days_passed >= 6:
                new_status = 'closed_expired'
                evaluation_reason = "Expired after 6 days."

            # --- If status changed, finalize the records ---
            if new_status != 'active':
                profit_loss_percent = ((current_price - entry.entry_price) / entry.entry_price) * 100
                
                # 1. Update the original WeeklyWatchlistResult record
                entry.status = new_status
                entry.exit_price = current_price
                entry.exit_date = current_greg_date
                entry.jexit_date = today_jdate_str
                entry.profit_loss_percentage = profit_loss_percent
                entry.updated_at = datetime.now()
                
                # 2. Create the archive record in SignalsPerformance
                performance_record = SignalsPerformance(
                    #signal_unique_id=entry.signal_unique_id,
                    symbol_id=entry.symbol_id,
                    symbol_name=entry.symbol_name,
                    signal_source='WeeklyWatchlist',
                    entry_date=entry.entry_date,
                    entry_price=entry.entry_price,
                    jentry_date=entry.jentry_date,
                    exit_date=current_greg_date,
                    jexit_date=today_jdate_str,
                    exit_price=current_price,
                    profit_loss_percentage=profit_loss_percent,
                    status=new_status,
                    reason=json.dumps({"original_reason": entry.reason, "evaluation_reason": evaluation_reason}), # Store reasons in the 'reason' text field
                    outlook=entry.outlook # Pass the original outlook
                )
                db.session.add(performance_record)
                db.session.add(entry)
                updated_count += 1
                logger.info(f"Closed signal for {entry.symbol_name}. Status: {new_status}, P/L: {profit_loss_percent:.2f}%.")
        
        except Exception as e:
            logger.error(f"Error updating performance for {entry.symbol_name}: {e}", exc_info=True)

    # Commit all updates at once
    try:
        if updated_count > 0:
            db.session.commit()
            logger.info(f"Successfully updated and closed {updated_count} watchlist signals.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"DB commit error during watchlist performance update: {e}", exc_info=True)


def _calculate_watchlist_performance_metrics(all_results, start_date):
    """
    Calculates performance metrics (win-rate, profit, loss) for a given
    set of results and a start date. Mirrors _calculate_performance_metrics.
    """
    successful, total = 0, 0
    total_profit, total_loss = 0.0, 0.0

    for res in all_results:
        # ✅ شرط کلیدی: فقط سیگنال‌هایی که بسته شده‌اند و در بازه زمانی مورد نظر هستند
        if res.status.startswith('closed_') and res.jentry_date >= start_date:
            if res.profit_loss_percentage is not None:
                total += 1
                profit_percent = res.profit_loss_percentage
                
                if profit_percent > 0:
                    successful += 1
                    total_profit += profit_percent
                else:
                    total_loss += abs(profit_percent)
    
    win_rate = (successful / total * 100) if total > 0 else 0.0
    return total, successful, win_rate, total_profit, total_loss


def _save_watchlist_performance_metrics(today_jdate_str, period_type, total_signals, successful_signals, win_rate, total_profit_percent, total_loss_percent):
    """
    Upserts aggregated performance metrics for the watchlist service.
    Mirrors _save_performance_metrics.
    """
    signal_source = 'WeeklyWatchlistService'
    try:
        existing = AggregatedPerformance.query.filter_by(
            report_date=today_jdate_str, period_type=period_type, signal_source=signal_source
        ).first()

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
            )
            db.session.add(newp)
            logger.info(f"Saved new aggregated performance for {signal_source} ({period_type})")
        
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving aggregated performance for {signal_source}: {e}", exc_info=True)


def evaluate_weekly_watchlist_performance():
    """
    Main orchestrator for performance evaluation, mirroring calculate_golden_key_win_rate.
    1. Updates the status of all 'active' signals.
    2. Calculates and saves aggregated performance metrics for all closed signals.
    """
    logger.info("Starting Weekly Watchlist performance metrics update and save process.")
    today_jdate_str = get_today_jdate_str()
    
    # 1. بازیابی سیگنال‌های فعال برای ارزیابی و به‌روزرسانی
    active_entries = WeeklyWatchlistResult.query.filter_by(status='active').all()
    if not active_entries:
        logger.warning("No active watchlist signals found to update.")
    else:
        # ✅ گام ۱: به‌روزرسانی وضعیت سیگنال‌های فعال
        _update_weekly_watchlist_performance(active_entries)

    # 2. بازیابی تمام نتایج برای محاسبه آمار کلی
    all_results = WeeklyWatchlistResult.query.all()
    if not all_results:
        logger.warning("No watchlist results found for aggregate performance computation.")
        return False, "Performance computation skipped: No watchlist results found."

    # 3. تعریف بازه‌های زمانی
    week_ago = (jdatetime.datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    month_ago = (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    # 4. محاسبه و ذخیره هفتگی
    w_total, w_success, w_win, w_profit, w_loss = _calculate_watchlist_performance_metrics(all_results, week_ago)
    _save_watchlist_performance_metrics(today_jdate_str, 'weekly', w_total, w_success, w_win, w_profit, w_loss)

    # 5. محاسبه و ذخیره ماهانه
    m_total, m_success, m_win, m_profit, m_loss = _calculate_watchlist_performance_metrics(all_results, month_ago)
    _save_watchlist_performance_metrics(today_jdate_str, 'monthly', m_total, m_success, m_win, m_profit, m_loss)

    logger.info("Weekly Watchlist performance metrics update process completed.")
    return True, "Weekly Watchlist win-rate calculation completed successfully."

        

# کد جدید برای برگرداندن نتایج هفتگی
# ----------------------------
def get_weekly_watchlist_results():
    """
    Retrieves the latest weekly watchlist results from the database.
    This function now explicitly fetches results for the latest available date.
    Returns a dictionary with 'top_watchlist_stocks' and 'last_updated'.
    """
    logger.info("Retrieving latest weekly watchlist results.")
    
    # Find the latest jentry_date available in the WeeklyWatchlistResult table
    latest_jdate_record_obj = WeeklyWatchlistResult.query.order_by(WeeklyWatchlistResult.jentry_date.desc()).first()
    
    if not latest_jdate_record_obj or not latest_jdate_record_obj.jentry_date:
        logger.warning("No weekly watchlist results found or latest jentry_date is null in the database.")
        return {
            "top_watchlist_stocks": [],
            "last_updated": "نامشخص"
        }

    latest_jdate_str = latest_jdate_record_obj.jentry_date
    logger.info(f"Latest Weekly Watchlist results date: {latest_jdate_str}")

    # Fetch all results for the latest jentry_date
    results = WeeklyWatchlistResult.query.filter_by(jentry_date=latest_jdate_str)\
                                         .order_by(WeeklyWatchlistResult.created_at.desc()).all() 

    output_stocks = []
    for r in results:
        output_stocks.append({
            'signal_unique_id': r.signal_unique_id, 
            'symbol_id': r.symbol_id, # Use symbol_id instead of 'symbol'
            'symbol_name': r.symbol_name,
            'outlook': r.outlook,
            'reason': r.reason,
            'entry_price': r.entry_price,
            'jentry_date': r.jentry_date,
            'exit_price': r.exit_price,
            'jexit_date': r.jexit_date,
            'profit_loss_percentage': r.profit_loss_percentage,
            'status': r.status,
            'probability_percent': r.probability_percent
        })
    
    logger.info(f"Retrieved {len(output_stocks)} weekly watchlist results.")
    
    return {
        "top_watchlist_stocks": output_stocks,
        "last_updated": latest_jdate_str
    }