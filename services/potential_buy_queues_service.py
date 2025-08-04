# services/potential_buy_queues_service.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, PotentialBuyQueueResult # Ensure PotentialBuyQueueResult is imported
from flask import current_app
import pandas as pd
from datetime import datetime, timedelta
import jdatetime # Import jdatetime for Jalali date handling
# مطمئن شوید get_today_jdate_str و normalize_value به درستی کار می‌کنند
from services.utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns 
import json # For handling JSON strings in DB

import logging
logger = logging.getLogger(__name__)

# Helper function to get the most reliable price
def get_reliable_price(data_row):
    """
    Attempts to get the 'final' price, falls back to 'close' price if 'final' is invalid.
    Assumes data_row is a pandas Series or dict with 'final' and 'close' keys.
    """
    final_price = data_row.get('final')
    close_price = data_row.get('close')

    if pd.notna(final_price) and final_price > 0:
        return float(final_price)
    elif pd.notna(close_price) and close_price > 0:
        return float(close_price)
    return 0.0

# Helper function to convert Jalali date string to Gregorian date object for Pandas
def convert_jalali_to_gregorian_for_pandas(jdate_str):
    if pd.isna(jdate_str) or not isinstance(jdate_str, str):
        return pd.NaT # Return Not a Time for invalid/missing dates
    try:
        # Attempt to parse using jdatetime.date.fromisoformat for robustness
        jy, jm, jd = map(int, jdate_str.split('-'))
        return jdatetime.date(jy, jm, jd).togregorian()
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to convert Jalali date '{jdate_str}' to Gregorian: {e}. Returning NaT.")
        return pd.NaT # Handle parsing errors by returning NaT

def run_potential_buy_queue_analysis_and_save():
    """
    Analyzes symbols to identify potential buy queues based on volume,
    real buyer power, technical indicators, and price action, then saves results.
    Excludes "حق تقدم" (pre-emptive rights) symbols.
    Returns a tuple (success_status, message).
    """
    current_app.logger.info("Starting Potential Buy Queues analysis and saving results with enhanced logic.")

    symbols = ComprehensiveSymbolData.query.all()
    
    # Separate lists for general symbols and funds
    general_potential_queues_candidates = []
    fund_potential_queues_candidates = []

    # Define fund keywords
    fund_keywords = ["صندوق", "سرمایه گذاری", "اعتبار", "آتیه", "یکتا", "بورس", "دارایی", "گیلان", "اختصاصی", 
                     "تدبیر", "دماوند", "سپهر", "سودمند", "کامیاب", "آشنا", "ماهور"] 
    
    # Keywords/patterns for pre-emptive rights (حق تقدم)
    preemptive_rights_patterns = ["ح", "حق"] # Common prefixes for pre-emptive rights symbols

    today_jdate_str = get_today_jdate_str()

    # Clear existing results for today's date to prevent duplicates
    try:
        PotentialBuyQueueResult.query.filter_by(jdate=today_jdate_str).delete()
        db.session.commit()
        logger.info(f"Cleared existing potential buy queue results for {today_jdate_str}.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error clearing old potential buy queue results: {e}", exc_info=True)


    for symbol_data in symbols:
        symbol_id = symbol_data.symbol_id
        symbol_name = symbol_data.symbol_name

        # Filter out pre-emptive rights symbols
        if any(symbol_name.startswith(p) for p in preemptive_rights_patterns):
            current_app.logger.debug(f"Skipping {symbol_name}: Identified as a pre-emptive rights symbol.")
            continue

        is_fund = any(keyword in symbol_name for keyword in fund_keywords)

        # Fetch historical and technical data
        historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id)\
                                                .order_by(HistoricalData.jdate.desc())\
                                                .limit(60).all() # Fetch enough data for indicators
        
        technical_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id)\
                                                .order_by(TechnicalIndicatorData.jdate.desc())\
                                                .limit(60).all() # Fetch enough data for indicators

        current_app.logger.debug(f"[{symbol_name}] Fetched {len(historical_records)} historical records.")
        current_app.logger.debug(f"[{symbol_name}] Fetched {len(technical_records)} technical records.")

        if not historical_records or not technical_records or len(historical_records) < 30 or len(technical_records) < 30:
            current_app.logger.debug(f"[{symbol_name}] Skipping: Not enough historical or technical data found (hist: {len(historical_records)}, tech: {len(technical_records)}).")
            continue

        hist_df = pd.DataFrame([rec.__dict__ for rec in historical_records]).drop(columns=['_sa_instance_state'], errors='ignore')
        tech_df = pd.DataFrame([rec.__dict__ for rec in technical_records]).drop(columns=['_sa_instance_state'], errors='ignore')

        current_app.logger.debug(f"[{symbol_name}] hist_df shape before date conversion: {hist_df.shape}, columns: {hist_df.columns.tolist()}")
        current_app.logger.debug(f"[{symbol_name}] tech_df shape before date conversion: {tech_df.shape}, columns: {tech_df.columns.tolist()}")

        # Use custom function to convert jdate to Gregorian for Pandas
        hist_df['greg_date'] = hist_df['jdate'].apply(convert_jalali_to_gregorian_for_pandas)
        tech_df['greg_date'] = tech_df['jdate'].apply(convert_jalali_to_gregorian_for_pandas)

        # Drop rows where date conversion failed
        hist_df = hist_df.dropna(subset=['greg_date'])
        tech_df = tech_df.dropna(subset=['greg_date'])

        current_app.logger.debug(f"[{symbol_name}] hist_df shape after dropna: {hist_df.shape}")
        current_app.logger.debug(f"[{symbol_name}] tech_df shape after dropna: {tech_df.shape}")

        # Ensure dataframes are sorted by the new Gregorian date column
        hist_df = hist_df.sort_values(by='greg_date', ascending=True).reset_index(drop=True)
        tech_df = tech_df.sort_values(by='greg_date', ascending=True).reset_index(drop=True)

        # Merge historical and technical data using a LEFT merge to keep all historical data
        # Columns unique to hist_df (like open, high, low, close) will NOT get suffixes.
        # Columns common to both (like symbol_id, created_at, updated_at) WILL get suffixes.
        merged_df = pd.merge(hist_df, tech_df, on='jdate', how='left', suffixes=('_hist', '_tech'))
        
        current_app.logger.debug(f"[{symbol_name}] merged_df shape after merge (left join): {merged_df.shape}")
        current_app.logger.debug(f"[{symbol_name}] merged_df columns: {merged_df.columns.tolist()}")

        if merged_df.empty:
            current_app.logger.debug(f"[{symbol_name}] Skipping: Merged data is empty after date conversion/merge. Final merged_df shape: {merged_df.shape}")
            continue
        
        # Ensure we have at least 3 rows for candlestick patterns and other lookbacks
        if len(merged_df) < 3:
            current_app.logger.debug(f"[{symbol_name}] Skipping: Merged data has less than 3 rows ({len(merged_df)}). Not enough for analysis.")
            continue

        latest_data = merged_df.iloc[-1]
        
        close_price = get_reliable_price(latest_data)
        if close_price <= 0:
            current_app.logger.debug(f"[{symbol_name}] Skipping: Invalid current price ({close_price}).")
            continue

        buy_queue_volume = latest_data.get('qd1', 0) 
        buy_queue_count = latest_data.get('zd1', 0) 

        # --- ENHANCED ANALYSIS LOGIC ---
        reasons = []
        probability_percent = 0 # Base probability

        # 1. Significant Buy Queue Presence
        if buy_queue_volume > 500000 and buy_queue_count > 50: # More realistic thresholds
            reasons.append("صف خرید قابل توجه")
            probability_percent += 25
            current_app.logger.debug(f"[{symbol_name}]: Significant Buy Queue.")

        # 2. Strong Real Buyer Power
        real_buy_power_ratio = 0.0
        if 'buy_count_i' in latest_data and 'sell_count_i' in latest_data and \
           (latest_data.get('buy_count_i', 0) > 0) and (latest_data.get('sell_count_i', 0) > 0): 
            avg_buy_vol_per_trade = (latest_data.get('buy_i_volume', 0) or 0) / (latest_data.get('buy_count_i', 1) or 1)
            avg_sell_vol_per_trade = (latest_data.get('sell_i_volume', 0) or 0) / (latest_data.get('sell_count_i', 1) or 1)
            if avg_sell_vol_per_trade > 0:
                real_buy_power_ratio = avg_buy_vol_per_trade / avg_sell_vol_per_trade
        
        if real_buy_power_ratio > 1.8: # Adjusted threshold for stronger signal
            reasons.append("قدرت خریدار حقیقی بالا")
            probability_percent += 20
            current_app.logger.debug(f"[{symbol_name}]: Strong Real Buyer Power ({real_buy_power_ratio:.2f}).")

        # 3. Price Action: Closing near High
        if latest_data.get('high', 0) > 0 and (latest_data.get('high', 0) - close_price) / latest_data.get('high', 1) < 0.01: 
            reasons.append("قیمت پایانی نزدیک به سقف روزانه")
            probability_percent += 15
            current_app.logger.debug(f"[{symbol_name}]: Closing Near High.")

        # 4. Volume Spike (compared to average)
        if 'Volume_MA_20_tech' in latest_data and pd.notna(latest_data.get('Volume_MA_20_tech')) and latest_data.get('Volume_MA_20_tech', 0) > 0:
            if latest_data.get('volume', 0) > (2.5 * latest_data.get('Volume_MA_20_tech', 0)): 
                reasons.append("افزایش حجم معاملات (حجم مشکوک)")
                probability_percent += 15
                current_app.logger.debug(f"[{symbol_name}]: Volume Spike.")

        # 5. RSI Bullish Signal (Rising from Oversold or Strong Momentum)
        if 'RSI_tech' in latest_data and pd.notna(latest_data.get('RSI_tech')):
            if latest_data.get('RSI_tech', 0) < 35: # Close to oversold
                reasons.append("RSI نزدیک به محدوده اشباع فروش")
                probability_percent += 5
                current_app.logger.debug(f"[{symbol_name}]: RSI near oversold.")
            
            # Check if RSI is rising (requires at least 2 data points)
            if len(merged_df) >= 2 and 'RSI_tech' in merged_df.columns:
                prev_rsi = merged_df.iloc[-2].get('RSI_tech')
                if pd.notna(prev_rsi) and latest_data.get('RSI_tech', 0) > prev_rsi and latest_data.get('RSI_tech', 0) < 70: # RSI rising, not overbought
                    reasons.append("RSI در حال صعود")
                    probability_percent += 10
                    current_app.logger.debug(f"[{symbol_name}]: RSI rising.")

        # 6. MACD Bullish Crossover
        if 'MACD_tech' in latest_data and 'MACD_Signal_tech' in latest_data and \
           pd.notna(latest_data.get('MACD_tech')) and pd.notna(latest_data.get('MACD_Signal_tech')) and len(merged_df) >= 2:
            if latest_data.get('MACD_tech', 0) > latest_data.get('MACD_Signal_tech', 0) and \
               merged_df.iloc[-2].get('MACD_tech', 0) <= merged_df.iloc[-2].get('MACD_Signal_tech', 0):
                reasons.append("تقاطع صعودی MACD")
                probability_percent += 20
                current_app.logger.debug(f"[{symbol_name}]: MACD Bullish Crossover.")

        # 7. SMA Cross (e.g., SMA_20 crossing above SMA_50)
        if 'SMA_20_tech' in latest_data and 'SMA_50_tech' in latest_data and \
           pd.notna(latest_data.get('SMA_20_tech')) and pd.notna(latest_data.get('SMA_50_tech')) and len(merged_df) >= 2:
            if latest_data.get('SMA_20_tech', 0) > latest_data.get('SMA_50_tech', 0) and \
               merged_df.iloc[-2].get('SMA_20_tech', 0) <= merged_df.iloc[-2].get('SMA_50_tech', 0):
                reasons.append("تقاطع صعودی میانگین متحرک (SMA20/SMA50)")
                probability_percent += 15
                current_app.logger.debug(f"[{symbol_name}]: SMA Cross.")

        # 8. Candlestick Patterns (e.g., Bullish Engulfing, Hammer)
        required_candle_cols = ['open', 'high', 'low', 'close'] 
        if all(col in merged_df.columns for col in required_candle_cols) and len(merged_df) >= 3:
            # Extract today's and yesterday's candle data as dictionaries
            today_candle_data = merged_df.iloc[-1][required_candle_cols].to_dict()
            yesterday_candle_data = merged_df.iloc[-2][required_candle_cols].to_dict()
            
            # Check for NaNs in the relevant columns for the last 2 rows
            if (pd.Series(today_candle_data).isnull().values.any() or 
                pd.Series(yesterday_candle_data).isnull().values.any()):
                current_app.logger.debug(f"[{symbol_name}] Skipping candlestick pattern check: Today's or yesterday's candle data contains NaN values for price columns. Today: {today_candle_data}, Yesterday: {yesterday_candle_data}")
            else:
                # Pass the full 'close' column as a numpy array for close_prices_series
                bullish_patterns = check_candlestick_patterns(
                    today_candle_data, 
                    yesterday_candle_data, 
                    merged_df['close'].values # Pass the full series for trend detection
                )
                if bullish_patterns:
                    reasons.append(f"الگوی کندل استیک صعودی: {', '.join(bullish_patterns)}")
                    probability_percent += 20 # High score for strong patterns
                    current_app.logger.debug(f"[{symbol_name}]: Bullish Candlestick Pattern detected: {bullish_patterns}.")
                else:
                    current_app.logger.debug(f"[{symbol_name}] No bullish candlestick pattern detected.")
        else:
            current_app.logger.debug(f"[{symbol_name}] Skipping candlestick pattern check: Initial check failed (missing columns or not enough data). Columns found: {[col for col in required_candle_cols if col in merged_df.columns]}, merged_df length: {len(merged_df)}.")


        # 9. Smart Money Flow (Individual Net Flow)
        smart_money_df = calculate_smart_money_flow(hist_df)
        if not smart_money_df.empty and 'individual_net_flow' in smart_money_df.columns:
            latest_net_flow = smart_money_df.iloc[-1]['individual_net_flow']
            if pd.notna(latest_net_flow) and latest_net_flow > 0 and latest_net_flow > (latest_data.get('value', 0) * 0.03): 
                reasons.append("ورود پول هوشمند (حقیقی)")
                probability_percent += 18
                current_app.logger.debug(f"[{symbol_name}]: Smart Money Inflow.")
        
        # Cap probability at 100%
        probability_percent = min(probability_percent, 100)

        current_app.logger.debug(f"[{symbol_name}] Final Probability: {probability_percent:.2f}%, Reasons: {reasons}")

        # Only save if probability is significant and reasons exist
        if probability_percent >= 35 and reasons: # Set a MINIMUM threshold for saving (lowered from 40 to 35)
            candidate = {
                'symbol_id': symbol_id, 
                'symbol_name': symbol_name, 
                'reason': ", ".join(reasons), 
                'jdate': today_jdate_str, 
                'current_price': close_price, 
                'volume_change_percent': (latest_data.get('volume', 0) / latest_data.get('Volume_MA_20_tech', 1) - 1) * 100 if 'Volume_MA_20_tech' in latest_data and latest_data.get('Volume_MA_20_tech', 0) > 0 else 0.0,
                'real_buyer_power_ratio': real_buy_power_ratio, # FIX: Corrected variable name
                'matched_filters': json.dumps(reasons), 
                'group_type': 'fund' if is_fund else 'general',
                'timestamp': datetime.now(),
                'probability_percent': probability_percent # Include calculated probability
            }
            
            if is_fund:
                fund_potential_queues_candidates.append(candidate)
            else:
                general_potential_queues_candidates.append(candidate)

    # Sort and select top N for each group (e.g., top 10 general, top 5 fund)
    # This will help control the number of results
    sorted_general_queues = sorted(general_potential_queues_candidates, key=lambda x: x['probability_percent'], reverse=True)[:10]
    sorted_fund_queues = sorted(fund_potential_queues_candidates, key=lambda x: x['probability_percent'], reverse=True)[:5]
    
    final_candidates = sorted_general_queues + sorted_fund_queues

    # Save candidates to database
    saved_count = 0
    for candidate_data in final_candidates:
        new_queue_result = PotentialBuyQueueResult(**candidate_data)
        db.session.add(new_queue_result)
        saved_count += 1
    
    try:
        db.session.commit()
        current_app.logger.info(f"Potential Buy Queues analysis completed. Saved {saved_count} results for {today_jdate_str}.")
        return True, f"Potential Buy Queues analysis completed. Saved {saved_count} results."
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error saving potential buy queue results: {e}", exc_info=True)
        return False, f"Error saving potential buy queue results: {str(e)}"


def get_potential_buy_queues_data(filters=None): # This is the function name expected by main.py
    """
    Retrieves potential buy queue results from the database.
    Returns:
        A dictionary containing 'top_queues' (list of queue results)
        and 'technical_filters' (list of all available filter definitions).
    """
    logger.info(f"Fetching potential buy queue results with filters: {filters}")

    query = PotentialBuyQueueResult.query

    # Get the latest date for which results exist
    latest_date_result = db.session.query(db.func.max(PotentialBuyQueueResult.jdate)).scalar()
    
    if latest_date_result:
        query = query.filter_by(jdate=latest_date_result)
        last_updated_display = latest_date_result
        logger.info(f"Latest potential buy queues date: {latest_date_result}")
    else:
        logger.warning("No potential buy queue results found in database for any date.")
        last_updated_display = "نامشخص"
        return {
            "top_queues": [],
            "technical_filters": get_potential_buy_queue_filter_definitions(),
            "last_updated": last_updated_display
        }

    # If filters are provided, apply them (similar to Golden Key)
    if filters:
        filters_list = [f.strip() for f in filters.split(',') if f.strip()]
        logger.info(f"Applying potential buy queue filters: {filters_list}")
        
        all_latest_results = query.all()
        filtered_results = []
        for r in all_latest_results:
            # Ensure matched_filters is parsed correctly
            r_matched_filters_from_db = json.loads(r.matched_filters) if r.matched_filters else []
            if all(f in r_matched_filters_from_db for f in filters_list):
                filtered_results.append(r)
        results = filtered_results
    else:
        results = query.all() # Fetch all results for the latest date if no filters

    output = []
    for r in results:
        output.append({
            "symbol_name": r.symbol_name,
            "symbol_id": r.symbol_id,
            "reason": r.reason,
            "jdate": r.jdate,
            "current_price": r.current_price,
            "volume_change_percent": r.volume_change_percent,
            "real_buyer_power_ratio": r.real_buyer_power_ratio,
            "matched_filters": json.loads(r.matched_filters) if r.matched_filters else [], # Ensure it's a list
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "group_type": r.group_type, # Ensure group_type is included
            'probability_percent': r.probability_percent # Include probability_percent
        })

    logger.info(f"Returning {len(output)} potential buy queue results after filtering.")
    
    # Sort the output by probability_percent in descending order before returning
    output_sorted = sorted(output, key=lambda x: x.get('probability_percent', 0), reverse=True)

    return {
        "top_queues": output_sorted,
        "technical_filters": get_potential_buy_queue_filter_definitions(),
        "last_updated": last_updated_display
    }

def get_potential_buy_queue_filter_definitions():
    """
    Returns a static list of all defined potential buy queue filters.
    """
    return [
        {"name": "صف خرید قابل توجه", "category": "صف", "description": "وجود صف خرید با حجم و تعداد قابل توجه."},
        {"name": "قدرت خریدار حقیقی بالا", "category": "جریان وجوه", "description": "نسبت قدرت خریدار حقیقی به فروشنده حقیقی بالا."},
        {"name": "قیمت پایانی نزدیک به سقف روزانه", "category": "روند قیمت", "description": "قیمت پایانی سهم بسیار نزدیک به بالاترین قیمت روزانه است."},
        {"name": "افزایش حجم معاملات (حجم مشکوک)", "category": "حجم", "description": "حجم معاملات امروز به طور قابل توجهی بالاتر از میانگین حجم ۲۰ روزه است."},
        {"name": "RSI در حال صعود", "category": "اندیکاتور", "description": "RSI سهم در حال افزایش است و نشان‌دهنده بهبود مومنتوم است."},
        {"name": "تقاطع صعودی MACD", "category": "اندیکاتور", "description": "خط MACD از خط سیگنال خود به سمت بالا عبور کرده است."},
        {"name": "تقاطع صعودی میانگین متحرک (SMA20/SMA50)", "category": "اندیکاتور", "description": "میانگین متحرک ۲۰ روزه از میانگین متحرک ۵۰ روزه به سمت بالا عبور کرده است."},
        {"name": "الگوی کندل استیک صعودی", "category": "الگوهای کلاسیک", "description": "تشخیص الگوهای کندل استیک صعودی مانند پوشا صعودی یا چکش."},
        {"name": "ورود پول هوشمند (حقیقی)", "category": "جریان وجوه", "description": "ورود قابل توجه پول حقیقی به سهم."},
        {"name": "افزایش NAV", "category": "صندوق", "description": "افزایش خالص ارزش دارایی‌های صندوق (برای صندوق‌ها)."}
    ]
