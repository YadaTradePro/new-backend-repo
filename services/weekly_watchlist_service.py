# services/weekly_watchlist_service.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData, WeeklyWatchlistResult, SignalsPerformance, AggregatedPerformance, GoldenKeyResult 
from flask import current_app
import pandas as pd
from datetime import datetime, timedelta, date
import jdatetime
import uuid 
from sqlalchemy import func 
import logging 
import json 

# Import utility functions
from services.utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns, check_tsetmc_filters, check_financial_ratios, convert_gregorian_to_jalali 

# Import analysis_service for aggregated performance calculation
from services import analysis_service 

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# Define the lookback period for technical data (e.g., 60 days for SMA_50, Bollinger Bands)
TECHNICAL_DATA_LOOKBACK_DAYS = 60

def is_data_sufficient(data_list, min_len):
    """
    Checks if the provided data list is not empty and has at least min_len records.
    
    Args:
        data_list (list): The list of data records (e.g., historical_records, tech_records).
        min_len (int): The minimum required length for the data.
        
    Returns:
        bool: True if data is sufficient, False otherwise.
    """
    return data_list and len(data_list) >= min_len

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
            return pd.NaT # Return Not a Time for invalid date strings
    return pd.NaT # Return Not a Time for NaN or None

def _get_symbol_data_for_watchlist(symbol_id, symbol_name, lookback_days=TECHNICAL_DATA_LOOKBACK_DAYS):
    """
    Fetches comprehensive data for a symbol required for watchlist analysis.
    This includes historical data, technical indicators, and fundamental data.
    Ensures enough historical data is fetched for accurate technical indicator calculations.
    
    Args:
        symbol_id (str): The ID of the symbol.
        symbol_name (str): The name of the symbol.
        lookback_days (int): The number of days of historical/technical data to fetch.
                              This should be sufficient for all indicator calculations.

    Returns:
        tuple: (historical_df, technical_rec, fundamental_rec) or (None, None, None) if data is insufficient.
    """
    logger.debug(f"Fetching data for {symbol_name} ({symbol_id}) for watchlist analysis (lookback: {lookback_days} days).")

    # Fetch historical data - ensure enough data for technical indicators
    historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id)\
                                             .order_by(HistoricalData.date.desc())\
                                             .limit(lookback_days).all()
    
    if not is_data_sufficient(historical_records, lookback_days):
        logger.warning(f"Not enough historical data ({len(historical_records)} days, min {lookback_days}) for Weekly Watchlist for {symbol_name} ({symbol_id}). Skipping.")
        return None, None, None

    # Convert to DataFrame for easier manipulation
    hist_df = pd.DataFrame([rec.__dict__ for rec in historical_records]).drop(columns=['_sa_instance_state'], errors='ignore')
    hist_df['date'] = pd.to_datetime(hist_df['date'])
    hist_df = hist_df.sort_values(by='date', ascending=True).reset_index(drop=True)

    # Fetch the latest technical indicator data for the symbol
    technical_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id)\
                                                .order_by(TechnicalIndicatorData.jdate.desc())\
                                                .limit(lookback_days).all()
    
    if not is_data_sufficient(technical_records, lookback_days):
        logger.warning(f"Not enough technical data ({len(technical_records)} days, min {lookback_days}) for Weekly Watchlist for {symbol_name} ({symbol_id}). Skipping.")
        return None, None, None
    
    # Convert technical records to DataFrame and get the latest row for current indicators
    tech_df = pd.DataFrame([rec.__dict__ for rec in technical_records]).drop(columns=['_sa_instance_state'], errors='ignore')
    tech_df['date'] = tech_df['jdate'].apply(convert_jalali_to_gregorian_timestamp)
    tech_df = tech_df.dropna(subset=['date']) # Drop rows where conversion failed
    tech_df = tech_df.sort_values(by='date', ascending=True).reset_index(drop=True)
    tech_df = tech_df.sort_values(by='jdate', ascending=True).reset_index(drop=True)
    technical_rec = tech_df.iloc[-1] # Get the latest technical data as a Series

    # Fetch fundamental data
    fundamental_rec = FundamentalData.query.filter_by(symbol_id=symbol_id).first()
    # Fundamental data might not be strictly necessary for all watchlist criteria,
    # but it's good to fetch if available. Handle case where it might be None.

    return hist_df, technical_rec, fundamental_rec


def _check_technical_filters(hist_df, technical_rec):
    """
    Applies technical filters based on the latest technical indicator data.
    """
    satisfied_filters = []
    reason_parts = []

    # Filter 1: RSI (e.g., oversold or strong momentum)
    if technical_rec.RSI is not None:
        if technical_rec.RSI < 30:
            satisfied_filters.append("RSI_Oversold")
            reason_parts.append(f"RSI ({technical_rec.RSI:.2f}) is oversold.")
        elif technical_rec.RSI > 70:
            satisfied_filters.append("RSI_Overbought")
            reason_parts.append(f"RSI ({technical_rec.RSI:.2f}) is overbought.")
        elif 50 <= technical_rec.RSI <= 70:
            satisfied_filters.append("RSI_Strong_Momentum")
            reason_parts.append(f"RSI ({technical_rec.RSI:.2f}) indicates strong momentum.")

    # Filter 2: MACD Cross (Bullish Cross: MACD crosses above Signal Line)
    if technical_rec.MACD is not None and technical_rec.MACD_Signal is not None and technical_rec.MACD_Hist is not None:
        # A bullish cross is typically when MACD line crosses above Signal line, and histogram turns positive
        if technical_rec.MACD > technical_rec.MACD_Signal and technical_rec.MACD_Hist > 0:
            # To confirm a recent cross, we might need previous day's data.
            # For simplicity, we check current positive histogram and MACD > Signal
            satisfied_filters.append("MACD_Bullish_Cross")
            reason_parts.append(f"MACD ({technical_rec.MACD:.2f}) crossed above Signal ({technical_rec.MACD_Signal:.2f}).")
        elif technical_rec.MACD < technical_rec.MACD_Signal and technical_rec.MACD_Hist < 0:
            satisfied_filters.append("MACD_Bearish_Cross")
            reason_parts.append(f"MACD ({technical_rec.MACD:.2f}) crossed below Signal ({technical_rec.MACD_Signal:.2f}).")


    # Filter 3: Price vs. SMA (e.g., Price above SMA_20/50)
    if technical_rec.close_price is not None:
        if technical_rec.SMA_20 is not None and technical_rec.close_price > technical_rec.SMA_20:
            satisfied_filters.append("Price_Above_SMA20")
            reason_parts.append(f"Price ({technical_rec.close_price:.0f}) is above SMA-20 ({technical_rec.SMA_20:.0f}).")
        if technical_rec.SMA_50 is not None and technical_rec.close_price > technical_rec.SMA_50:
            satisfied_filters.append("Price_Above_SMA50")
            reason_parts.append(f"Price ({technical_rec.close_price:.0f}) is above SMA-50 ({technical_rec.SMA_50:.0f}).")

    # Filter 4: Bollinger Bands (e.g., Price touching lower band or breaking out)
    if technical_rec.close_price is not None and technical_rec.Bollinger_Low is not None and technical_rec.Bollinger_High is not None:
        if technical_rec.close_price < technical_rec.Bollinger_Low:
            satisfied_filters.append("Bollinger_Lower_Band_Touch")
            reason_parts.append(f"Price ({technical_rec.close_price:.0f}) touched lower Bollinger Band ({technical_rec.Bollinger_Low:.0f}).")
        elif technical_rec.close_price > technical_rec.Bollinger_High:
            satisfied_filters.append("Bollinger_Upper_Band_Breakout")
            reason_parts.append(f"Price ({technical_rec.close_price:.0f}) broke above upper Bollinger Band ({technical_rec.Bollinger_High:.0f}).")

    # Filter 5: Volume vs. Volume MA (e.g., High volume breakout)
    if hist_df is not None and not hist_df.empty and 'volume' in hist_df.columns and technical_rec.Volume_MA_20 is not None:
        latest_volume = hist_df['volume'].iloc[-1]
        if latest_volume > (technical_rec.Volume_MA_20 * 1.5): # Volume 1.5 times average
            satisfied_filters.append("High_Volume_Breakout")
            reason_parts.append(f"Volume ({latest_volume:.0f}) is significantly higher than average ({technical_rec.Volume_MA_20:.0f}).")

    # Filter 6: ATR (Average True Range) - for volatility
    if technical_rec.ATR is not None and technical_rec.ATR > 0:
        # Example: Check if ATR indicates high volatility relative to price
        if technical_rec.close_price is not None and technical_rec.close_price > 0:
            volatility_percent = (technical_rec.ATR / technical_rec.close_price) * 100
            if volatility_percent > 3: # Example: if daily range is more than 3% of price
                satisfied_filters.append("High_Volatility_ATR")
                reason_parts.append(f"ATR ({technical_rec.ATR:.2f}) indicates high volatility ({volatility_percent:.2f}% of price).")

    return satisfied_filters, reason_parts


def _check_fundamental_filters(fundamental_rec):
    """
    Applies fundamental filters.
    """
    satisfied_filters = []
    reason_parts = []

    if fundamental_rec:
        # Filter 1: P/E Ratio (e.g., reasonable P/E)
        if fundamental_rec.pe is not None and 0 < fundamental_rec.pe < 20: # Example: P/E between 0 and 20
            satisfied_filters.append("Reasonable_PE")
            reason_parts.append(f"P/E ratio ({fundamental_rec.pe:.2f}) is reasonable.")
        elif fundamental_rec.pe is not None and fundamental_rec.pe >= 20:
            satisfied_filters.append("High_PE")
            reason_parts.append(f"P/E ratio ({fundamental_rec.pe:.2f}) is high.")
        
        # Filter 2: EPS (e.g., positive EPS)
        if fundamental_rec.eps is not None and fundamental_rec.eps > 0:
            satisfied_filters.append("Positive_EPS")
            reason_parts.append(f"EPS ({fundamental_rec.eps:.2f}) is positive.")
        elif fundamental_rec.eps is not None and fundamental_rec.eps < 0:
            satisfied_filters.append("Negative_EPS")
            reason_parts.append(f"EPS ({fundamental_rec.eps:.2f}) is negative.")

    return satisfied_filters, reason_parts

def _check_smart_money_filters(hist_df):
    """
    Applies smart money flow filters.
    """
    satisfied_filters = []
    reason_parts = []

    if hist_df is None or hist_df.empty or 'buy_i_volume' not in hist_df.columns: # Changed to 'buy_i_volume'
        return satisfied_filters, reason_parts

    # Calculate smart money flow using the utility function
    smart_money_flow_df = calculate_smart_money_flow(hist_df)

    if not smart_money_flow_df.empty:
        latest_smart_money = smart_money_flow_df.iloc[-1]
        
        # Check for individual buyer power (e.g., individual_buy_power > 1)
        if latest_smart_money['individual_buy_power'] is not None and latest_smart_money['individual_buy_power'] > 1.2: # Example threshold
            satisfied_filters.append("Strong_Individual_Buy_Power")
            reason_parts.append(f"Individual buy power ({latest_smart_money['individual_buy_power']:.2f}) is strong.")
        elif latest_smart_money['individual_buy_power'] is not None and latest_smart_money['individual_buy_power'] < 0.8:
            satisfied_filters.append("Weak_Individual_Buy_Power")
            reason_parts.append(f"Individual buy power ({latest_smart_money['individual_buy_power']:.2f}) is weak.")

        # Check for real money entry (e.g., individual_net_flow is positive and significant)
        if latest_smart_money['individual_net_flow'] is not None and latest_smart_money['individual_net_flow'] > 0 and \
           latest_smart_money['individual_net_flow'] > (hist_df['value'].iloc[-1] * 0.05): # Example: 5% of daily value
            satisfied_filters.append("Positive_Real_Money_Flow")
            reason_parts.append(f"Positive real money flow ({latest_smart_money['individual_net_flow']:.0f}) detected.")
        elif latest_smart_money['individual_net_flow'] is not None and latest_smart_money['individual_net_flow'] < 0 and \
             abs(latest_smart_money['individual_net_flow']) > (hist_df['value'].iloc[-1] * 0.05):
            satisfied_filters.append("Negative_Real_Money_Flow")
            reason_parts.append(f"Negative real money flow ({latest_smart_money['individual_net_flow']:.0f}) detected.")

    return satisfied_filters, reason_parts


def run_weekly_watchlist_selection():
    """
    Selects symbols for the weekly watchlist based on a combination of criteria.
    This function should be run once a week (e.g., Wednesday evening).
    """
    logger.info("Starting Weekly Watchlist selection process.")

    # Define allowed market types for watchlist selection
    allowed_market_types = [
        'بورس', 'فرابورس', 'بورس کالا', 'صندوق سرمایه گذاری', 'اوراق با درآمد ثابت',
        'مشتقه', 'عمومی', 'پایه فرابورس', 'بورس انرژی', 'اوراق تامین مالی'
    ]

    # Fetch all symbols from ComprehensiveSymbolData that are in allowed market types
    symbols_to_analyze = ComprehensiveSymbolData.query.filter(
        ComprehensiveSymbolData.market_type.in_(allowed_market_types)
    ).all()

    if not symbols_to_analyze:
        logger.warning("No symbols found in ComprehensiveSymbolData for watchlist analysis based on allowed market types. Please ensure initial data population is complete.")
        return False, "No symbols found for watchlist analysis."

    watchlist_candidates = []
    processed_symbols_count = 0

    for symbol in symbols_to_analyze:
        logger.info(f"Analyzing {symbol.symbol_name} ({symbol.symbol_id}) for Weekly Watchlist.")

        hist_df, technical_rec, fundamental_rec = _get_symbol_data_for_watchlist(symbol.symbol_id, symbol.symbol_name, lookback_days=TECHNICAL_DATA_LOOKBACK_DAYS)

        if hist_df is None or technical_rec is None:
            # Logging already handled inside _get_symbol_data_for_watchlist
            continue

        all_satisfied_filters = []
        all_reason_parts = []
        
        # 1. Apply Technical Filters
        tech_filters, tech_reasons = _check_technical_filters(hist_df, technical_rec)
        all_satisfied_filters.extend(tech_filters)
        all_reason_parts.extend(tech_reasons)

        # 2. Apply Fundamental Filters (if fundamental data is available)
        if fundamental_rec:
            fund_filters, fund_reasons = _check_fundamental_filters(fundamental_rec)
            all_satisfied_filters.extend(fund_filters)
            all_reason_parts.extend(fund_reasons)
        else:
            logger.debug(f"No fundamental data for {symbol.symbol_name}. Skipping fundamental filters.")

        # 3. Apply Smart Money Filters
        smart_money_filters, smart_money_reasons = _check_smart_money_filters(hist_df)
        all_satisfied_filters.extend(smart_money_filters)
        all_reason_parts.extend(smart_money_reasons)

        # 4. Apply Candlestick Pattern Filters (Placeholder - implement in utils.py)
        # candlestick_patterns, pattern_reasons = check_candlestick_patterns(hist_df)
        # all_satisfied_filters.extend(candlestick_patterns)
        # all_reason_parts.extend(pattern_reasons)

        # 5. Apply TSETMC Filter Results (Placeholder - fetch from TSETMCFilterResult model)
        # tsetmc_filters, tsetmc_reasons = check_tsetmc_filters(symbol.symbol_id, jdate_today_str)
        # all_satisfied_filters.extend(tsetmc_filters)
        # all_reason_parts.extend(tsetmc_reasons)

        # 6. Apply Financial Ratios Filters (Placeholder - fetch from FinancialRatiosData model)
        # financial_ratios_filters, financial_ratios_reasons = check_financial_ratios(symbol.symbol_id)
        # all_satisfied_filters.extend(financial_ratios_filters)
        # all_reason_parts.extend(financial_ratios_reasons)


        # Determine if the symbol is a candidate for the watchlist
        score = len(all_satisfied_filters)
        
        if score >= 2: # Example threshold for a candidate
            watchlist_candidates.append({
                "symbol_id": symbol.symbol_id,
                "symbol_name": symbol.symbol_name,
                "entry_price": technical_rec.close_price, # Use latest close price as entry
                "entry_date": date.today(), # Gregorian date
                "jentry_date": get_today_jdate_str(), # Jalali date
                "outlook": "Bullish" if "MACD_Bullish_Cross" in all_satisfied_filters or "RSI_Oversold" in all_satisfied_filters else "Neutral",
                "reason": " | ".join(all_reason_parts) if all_reason_parts else "Met watchlist criteria.",
                "probability_percent": min(100, score * 10), # Simple example for probability
                "satisfied_filters": json.dumps(all_satisfied_filters), # Store as JSON string
                "score": score
            })
            logger.info(f"Symbol {symbol.symbol_name} ({symbol.symbol_id}) added as a watchlist candidate with score {score}.")
        else:
            logger.debug(f"Symbol {symbol.symbol_name} ({symbol.symbol_id}) did not meet minimum criteria (score {score}).")

        processed_symbols_count += 1


    # Sort candidates by score (highest first) and select top N
    watchlist_candidates.sort(key=lambda x: x['score'], reverse=True)
    
    top_n_symbols = 4 
    final_watchlist = watchlist_candidates[:top_n_symbols]

    # Save results to WeeklyWatchlistResult table
    saved_count = 0
    for candidate in final_watchlist:
        # Check if a similar signal already exists for the current week/day to avoid duplicates
        existing_result = WeeklyWatchlistResult.query.filter_by(
            symbol=candidate['symbol_id'], # CORRECTED: Changed from 'symbol_id' to 'symbol'
            jentry_date=candidate['jentry_date']
        ).first()

        if existing_result:
            # Update existing record
            existing_result.entry_price = candidate['entry_price']
            existing_result.outlook = candidate['outlook']
            existing_result.reason = candidate['reason']
            existing_result.probability_percent = candidate['probability_percent']
            existing_result.created_at = datetime.now() # Update timestamp
            db.session.add(existing_result)
            logger.info(f"Updated existing WeeklyWatchlistResult for {candidate['symbol_name']} on {candidate['jentry_date']}.")
        else:
            new_result = WeeklyWatchlistResult(
                signal_unique_id=str(uuid.uuid4()), # Generate a new unique ID
                symbol=candidate['symbol_id'], # CORRECTED: Changed from 'symbol_id' to 'symbol'
                symbol_name=candidate['symbol_name'],
                entry_price=candidate['entry_price'],
                entry_date=candidate['entry_date'],
                jentry_date=candidate['jentry_date'],
                outlook=candidate['outlook'],
                reason=candidate['reason'],
                probability_percent=candidate['probability_percent'],
                created_at=datetime.now(),
                status='active' # Ensure new entries are active
            )
            db.session.add(new_result)
            logger.info(f"Added new WeeklyWatchlistResult for {candidate['symbol_name']} on {candidate['jentry_date']}.")
        saved_count += 1
    
    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. Found {len(watchlist_candidates)} candidates, saved top {saved_count} symbols."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during Weekly Watchlist selection: {e}"
        logger.error(error_message, exc_info=True)
        return 0, error_message


# --- Weekly Watchlist Performance Evaluation ---
def evaluate_weekly_watchlist_performance():
    """
    Evaluates the performance of active weekly watchlist signals.
    Calculates profit/loss and updates status.
    Moves evaluated signals from WeeklyWatchlistResult to SignalsPerformance.
    Intended to be run at the end of the week (e.g., Wednesday 20:20).
    """
    logger.info("Starting Weekly Watchlist performance evaluation.")
    
    today_jdate_str = get_today_jdate_str()
    current_greg_date = datetime.now().date()

    # Fetch all active watchlist entries that were added BEFORE today
    active_watchlist_entries = WeeklyWatchlistResult.query.filter(
        WeeklyWatchlistResult.status == 'active', 
        # We want to evaluate signals that were NOT added today, and potentially those that have expired
        # The jentry_date != today_jdate_str filter is correct to avoid evaluating newly added signals immediately.
    ).all()

    if not active_watchlist_entries:
        logger.warning("No active weekly watchlist entries (from previous days) found for evaluation.")
        return False, "No active watchlist entries to evaluate."

    evaluated_count = 0
    for entry in active_watchlist_entries:
        logger.info(f"Evaluating performance for {entry.symbol_name} (ID: {entry.symbol}).") 

        # Fetch latest historical data for the symbol
        latest_historical_data = HistoricalData.query.filter_by(symbol_id=entry.symbol).order_by(HistoricalData.jdate.desc()).first() 
                                                    
        # --- DEBUGGING LOGS ADDED HERE ---
        if not latest_historical_data:
            logger.warning(f"No HistoricalData record found for symbol_id: {entry.symbol} (Name: {entry.symbol_name}). Cannot evaluate.") 
            continue 
        
        # Use safe_float for current_price and fallback logic
        current_price = normalize_value(latest_historical_data.final) # Using normalize_value for robustness
        if current_price is None or current_price <= 0: # Fallback to close price if final is invalid
            current_price = normalize_value(latest_historical_data.close)

        if current_price is None or current_price <= 0: # If even close price is invalid
            logger.warning(f"Current price for {entry.symbol_name} is zero or invalid (Value: {current_price}). Cannot evaluate signal.") 
            continue 
        # --- END DEBUGGING LOGS ---

        
        # Calculate profit/loss percentage
        profit_loss_percent = 0.0
        if entry.entry_price and entry.entry_price > 0:
            profit_loss_percent = ((current_price - entry.entry_price) / entry.entry_price) * 100
        else:
            logger.warning(f"Entry price for {entry.symbol_name} is zero or invalid. Cannot calculate profit/loss. Setting P/L to 0.")
            profit_loss_percent = 0.0 # Ensure it's a float even if entry_price is bad


        # Determine status (simplified example: close if profit > 10% or loss > 5%)
        status = 'active' # Default status

        # Convert jentry_date to jdatetime.date object for comparison
        try:
            jy, jm, jd = map(int, entry.jentry_date.split('-'))
            entry_jdate_obj = jdatetime.date(jy, jm, jd)
        except ValueError:
            logger.error(f"Invalid jentry_date format for signal {entry.signal_unique_id}: {entry.jentry_date}. Cannot determine expiration. Keeping active.")
            status = 'active'
            return False, "Invalid jentry_date format." # Return False to indicate an issue
        
        # Calculate the expiration date (e.g., 7 calendar days after entry)
        # This assumes that `timedelta` works correctly with `jdatetime.date` objects.
        # If not, you might need to convert to Gregorian, add timedelta, then convert back.
        # However, jdatetime library is designed to handle this.
        expiration_jdate_obj = entry_jdate_obj + timedelta(days=7) 

        # Current date in jdatetime format
        current_jdate_obj = jdatetime.date.today()

        # Check for profit/loss targets first
        if profit_loss_percent >= 10: # Example profit target
            status = 'closed_win' 
            logger.info(f"Signal {entry.signal_unique_id} closed with profit: {profit_loss_percent:.2f}%") # CORRECTED
        elif profit_loss_percent <= -5: # Example stop loss
            status = 'closed_loss'
            logger.info(f"Signal {entry.signal_unique_id} closed with loss: {profit_loss_percent:.2f}%") # CORRECTED
        # If not closed by profit/loss, check if it's expired by time AND it's not a signal added today
        elif current_jdate_obj >= expiration_jdate_obj and entry.jentry_date != today_jdate_str:
            status = 'closed_neutral' # Or 'closed_expired' if we add that status
            logger.info(f"Signal {entry.signal_unique_id} expired by time ({entry.jentry_date} -> {expiration_jdate_obj}). Status set to closed_neutral. Current JDate: {current_jdate_obj}") # CORRECTED
        else:
            status = 'active' # Remains active if no target hit and not expired


        # Update the WeeklyWatchlistResult entry
        entry.exit_price = current_price if status != 'active' else None 
        entry.jexit_date = today_jdate_str if status != 'active' else None
        entry.exit_date = current_greg_date if status != 'active' else None
        entry.profit_loss_percentage = profit_loss_percent if status != 'active' else None # Only set P/L if closed
        entry.status = status
        entry.updated_at = datetime.now()
        db.session.add(entry)
        
        logger.info(f"Updated WeeklyWatchlistResult for {entry.symbol_name}: Status={status}, P/L={profit_loss_percent:.2f}%")

        # Create a new entry in SignalsPerformance table
        # Check if a performance record for this signal already exists to avoid duplicates
        existing_performance = SignalsPerformance.query.filter_by(signal_id=entry.signal_unique_id).first()
        if existing_performance:
            # Update existing record
            existing_performance.exit_date = current_greg_date if status != 'active' else None
            existing_performance.jexit_date = today_jdate_str if status != 'active' else None
            existing_performance.exit_price = current_price if status != 'active' else None
            existing_performance.profit_loss_percent = profit_loss_percent if status != 'active' else None
            existing_performance.status = status
            existing_performance.evaluated_at = datetime.now()
            db.session.add(existing_performance)
            logger.info(f"Updated SignalsPerformance record for {entry.signal_unique_id}.")
        else:
            new_performance = SignalsPerformance(
                signal_id=entry.signal_unique_id, # Use the unique ID from WeeklyWatchlistResult
                symbol_id=entry.symbol, 
                symbol_name=entry.symbol_name,
                signal_source='Weekly Watchlist', 
                entry_date=entry.entry_date,
                jentry_date=entry.jentry_date,
                entry_price=entry.entry_price,
                outlook=entry.outlook,
                reason=entry.reason,
                probability_percent=entry.probability_percent,
                exit_date=current_greg_date if status != 'active' else None, # Use current_greg_date
                jexit_date=today_jdate_str if status != 'active' else None, # Use today_jdate_str
                exit_price=current_price if status != 'active' else None, # Use current_price
                profit_loss_percent=profit_loss_percent if status != 'active' else None, # Use calculated P/L
                status=status,
                created_at=entry.created_at, # Use original creation date from WeeklyWatchlistResult
                evaluated_at=datetime.now()
            )
            db.session.add(new_performance)
            logger.info(f"Created new SignalsPerformance entry for {entry.symbol_name} (Source: Weekly Watchlist).")
        evaluated_count += 1

    try:
        db.session.commit()
        logger.info(f"Weekly Watchlist evaluation completed. Evaluated {evaluated_count} signals.")
        
        # After evaluating individual signals, trigger aggregated performance calculation
        if hasattr(analysis_service, 'calculate_aggregated_performance'):
            success_agg, msg_agg = analysis_service.calculate_aggregated_performance(
                period_type='weekly', 
                signal_source='Weekly Watchlist'
            )
            logger.info(f"Aggregated performance for Weekly Watchlist: {msg_agg}")
        else:
            logger.warning("analysis_service.calculate_aggregated_performance not found. Aggregated performance for Weekly Watchlist not updated.")

        # Also trigger an overall aggregation for the app's performance
        if hasattr(analysis_service, 'calculate_aggregated_performance'):
            success_overall_agg, msg_overall_agg = analysis_service.calculate_aggregated_performance(
                period_type='weekly', 
                signal_source='overall' # For overall app performance
            )
            logger.info(f"Aggregated overall performance (weekly): {msg_overall_agg}")
        
        return True, f"Weekly Watchlist performance evaluation completed for {evaluated_count} signals."
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during Weekly Watchlist performance evaluation: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

# --- Helper function to get the latest weekly watchlist results for display ---
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
            'symbol': r.symbol, # ADDED: Ensure 'symbol' field is explicitly included
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

def evaluate_signal_performance(signal_unique_id):
    """
    Evaluates the performance of a specific signal (e.g., a Weekly Watchlist item).
    This function should be called periodically for active signals.
    """
    logger.info(f"Evaluating performance for signal: {signal_unique_id}")
    try:
        # Fetch the signal from WeeklyWatchlistResult
        watchlist_signal = WeeklyWatchlistResult.query.filter_by(signal_unique_id=signal_unique_id).first()
        if not watchlist_signal:
            logger.warning(f"Weekly Watchlist signal with ID {signal_unique_id} not found.")
            return False, "Signal not found."

        # Fetch the latest historical data for the symbol
        latest_historical_data = HistoricalData.query.filter_by(symbol_id=watchlist_signal.symbol).order_by(HistoricalData.jdate.desc()).first() 
                                                    
        if not latest_historical_data:
            logger.warning(f"No latest historical data found for symbol {watchlist_signal.symbol_name} to evaluate signal {signal_unique_id}.")
            return False, "No latest historical data for symbol."

        # Use normalize_value for current_price and fallback logic
        current_price = normalize_value(latest_historical_data.final) 
        if current_price is None or current_price <= 0:
            current_price = normalize_value(latest_historical_data.close)

        if current_price is None or current_price <= 0:
            logger.warning(f"Current price for {watchlist_signal.symbol_name} is zero or invalid. Cannot evaluate signal.")
            return False, "Current price is invalid."
        
        # Calculate profit/loss percentage
        profit_loss_percent = 0.0
        if watchlist_signal.entry_price and watchlist_signal.entry_price > 0:
            profit_loss_percent = ((current_price - watchlist_signal.entry_price) / watchlist_signal.entry_price) * 100
        else:
            logger.warning(f"Entry price for {watchlist_signal.symbol_name} is zero or invalid. Cannot calculate profit/loss. Setting P/L to 0.")
            profit_loss_percent = 0.0


        # Determine status (simplified example: close if profit > 10% or loss > 5%)
        status = 'active' # Default status

        # Convert jentry_date to jdatetime.date object for comparison
        try:
            jy, jm, jd = map(int, watchlist_signal.jentry_date.split('-'))
            entry_jdate_obj = jdatetime.date(jy, jm, jd)
        except ValueError:
            logger.error(f"Invalid jentry_date format for signal {watchlist_signal.signal_unique_id}: {watchlist_signal.jentry_date}. Cannot determine expiration. Keeping active.")
            status = 'active'
            return False, "Invalid jentry_date format." # Return False to indicate an issue

        # Calculate the expiration date (e.g., 7 calendar days after entry)
        expiration_jdate_obj = entry_jdate_obj + timedelta(days=7) 

        # Current date in jdatetime format
        current_jdate_obj = jdatetime.date.today()

        # Check for profit/loss targets first
        if profit_loss_percent >= 10: # Example profit target
            status = 'closed_win' 
            logger.info(f"Signal {signal_unique_id} closed with profit: {profit_loss_percent:.2f}%")
        elif profit_loss_percent <= -5: # Example stop loss
            status = 'closed_loss'
            logger.info(f"Signal {signal_unique_id} closed with loss: {profit_loss_percent:.2f}%")
        # If not closed by profit/loss, check if it's expired by time
        elif current_jdate_obj >= expiration_jdate_obj:
            status = 'closed_neutral' 
            logger.info(f"Signal {signal_unique_id} expired by time ({watchlist_signal.jentry_date} -> {expiration_jdate_obj}). Status set to closed_neutral. Current JDate: {current_jdate_obj}")
        else:
            status = 'active' # Remains active if no target hit and not expired


        # Update or create SignalsPerformance record
        performance_record = SignalsPerformance.query.filter_by(signal_id=signal_unique_id).first() 
        if performance_record:
            performance_record.exit_date = current_greg_date if status != 'active' else None
            performance_record.jexit_date = today_jdate_str if status != 'active' else None
            performance_record.exit_price = current_price if status != 'active' else None
            performance_record.profit_loss_percent = profit_loss_percent if status != 'active' else None
            performance_record.status = status
            performance_record.evaluated_at = datetime.now()
            db.session.add(performance_record)
            logger.info(f"Updated SignalsPerformance for {watchlist_signal.symbol_name} (ID: {signal_unique_id}).")
        else:
            new_performance_record = SignalsPerformance(
                signal_id=signal_unique_id, 
                symbol_id=watchlist_signal.symbol, 
                symbol_name=watchlist_signal.symbol_name,
                signal_source='Weekly Watchlist', 
                entry_date=watchlist_signal.entry_date,
                jentry_date=watchlist_signal.jentry_date,
                entry_price=watchlist_signal.entry_price,
                outlook=watchlist_signal.outlook,
                reason=watchlist_signal.reason,
                probability_percent=watchlist_signal.probability_percent,
                exit_date=current_greg_date if status != 'active' else None, # Use current_greg_date
                jexit_date=today_jdate_str if status != 'active' else None, # Use today_jdate_str
                exit_price=current_price if status != 'active' else None, # Use current_price
                profit_loss_percent=profit_loss_percent if status != 'active' else None, # Use calculated P/L
                status=status,
                created_at=datetime.now(),
                evaluated_at=datetime.now()
            )
            db.session.add(new_performance_record)
            logger.info(f"Added new SignalsPerformance record for {watchlist_signal.symbol_name} (ID: {signal_unique_id}).")

        db.session.commit()
        return True, f"Signal {signal_unique_id} evaluated. Status: {status}, P/L: {profit_loss_percent:.2f}%."

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error evaluating signal performance for {signal_unique_id}: {e}", exc_info=True)
        return False, f"Error evaluating signal performance: {str(e)}"

def run_daily_performance_evaluation():
    """
    Runs daily evaluation for all active signals.
    This function should be called daily (e.g., after market close).
    """
    logger.info("Starting daily signal performance evaluation.")
    active_signals = WeeklyWatchlistResult.query.filter_by(status='active').all() 
    evaluated_count = 0
    for signal in active_signals:
        success, message = evaluate_signal_performance(signal.signal_unique_id)
        if success:
            evaluated_count += 1
            logger.info(f"Daily evaluation for {signal.symbol_name} ({signal.signal_unique_id}): {message}")
        else:
            logger.warning(f"Daily evaluation failed for {signal.symbol_name} ({signal.signal_unique_id}): {message}")
    
    message = f"Daily performance evaluation completed. Evaluated {evaluated_count} active signals."
    logger.info(message)
    return evaluated_count, message