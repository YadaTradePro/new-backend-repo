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

# Import utility functions
from services.utils import get_today_jdate_str, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow, check_candlestick_patterns, check_tsetmc_filters, check_financial_ratios, convert_gregorian_to_jalali, calculate_z_score

# Import analysis_service for aggregated performance calculation
from services import analysis_service

# تنظیمات لاگینگ برای این ماژول
logger = logging.getLogger(__name__)

# Define the lookback period for technical data (e.g., 60 days for SMA_50, Bollinger Bands)
TECHNICAL_DATA_LOOKBACK_DAYS = 90
# حداقل روزهای لازم برای محاسبه اندیکاتورهای پایه‌ای (MACD نیاز به 26 دارد)
MIN_REQUIRED_HISTORY_DAYS = 26

# Define filter weights for the new scoring algorithm
FILTER_WEIGHTS = {
    "MACD_Bullish_Cross_Confirmed": 5,
    "RSI_Positive_Divergence": 3,
    "High_Volume_ZScore": 2,
    "Reasonable_PE": 1,
    "Reasonable_PS": 1,
    "Reasonable_PB": 1,
    "High_ROE": 2,
    "Positive_Real_Money_Flow_Trend": 3,
    "High_Individual_Participation": 1,
    "Price_Above_SMA50": 2,
    "Price_Above_SMA20": 1,
    "Bollinger_Lower_Band_Touch": 1,
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
def _check_technical_filters(hist_df, tech_df):
    satisfied_filters = []
    reason_parts = {"technical": []}

    # اگر tech_df خیلی کوچک باشه، ما بعضی بررسی‌ها رو رد می‌کنیم اما اجازه می‌دهیم دیگران اجرا شوند
    if tech_df is None or len(tech_df) < 1:
        # هیچ رکورد تکنیکالی نداریم -> فقط بعضی فیلترها (مثل حجم/ATR از تاریخ) ممکن است قابل اجرا باشند
        technical_rec = None
        prev_tech_rec = None
    else:
        technical_rec = tech_df.iloc[-1]
        prev_tech_rec = tech_df.iloc[-2] if len(tech_df) >= 2 else None

    # close series امن از دیتای تاریخی
    close_ser = _get_close_series_from_hist_df(hist_df)
    # برای واگرایی به حداقل 2 مقدار نیاز است
    if len(close_ser) >= 2 and technical_rec is not None and hasattr(technical_rec, 'RSI'):
        try:
            last_close = close_ser.iloc[-1]
            prev_close = close_ser.iloc[-2]
            if technical_rec.RSI is not None and prev_tech_rec is not None and prev_tech_rec.RSI is not None:
                if last_close < prev_close and technical_rec.RSI > prev_tech_rec.RSI:
                    satisfied_filters.append("RSI_Positive_Divergence")
                    reason_parts["technical"].append(f"واگرایی مثبت در RSI ({technical_rec.RSI:.2f}) دیده شد.")
                if last_close > prev_close and technical_rec.RSI < prev_tech_rec.RSI:
                    satisfied_filters.append("RSI_Negative_Divergence")
                    reason_parts["technical"].append(f"واگرایی منفی در RSI ({technical_rec.RSI:.2f}) دیده شد.")
        except Exception as e:
            logger.debug(f"RSI divergence check failed for symbol: {e}")

    # MACD checks — فقط در صورتی که مقادیر MACD موجود باشند
    if technical_rec is not None and prev_tech_rec is not None and \
       getattr(technical_rec, 'MACD', None) is not None and getattr(technical_rec, 'MACD_Signal', None) is not None \
       and getattr(prev_tech_rec, 'MACD', None) is not None and getattr(prev_tech_rec, 'MACD_Signal', None) is not None:
        if technical_rec.MACD > technical_rec.MACD_Signal and prev_tech_rec.MACD <= prev_tech_rec.MACD_Signal:
            satisfied_filters.append("MACD_Bullish_Cross_Confirmed")
            reason_parts["technical"].append(f"کراس صعودی معتبر MACD ({technical_rec.MACD:.2f}) بالای سیگنال ({technical_rec.MACD_Signal:.2f}) رخ داد.")
        elif technical_rec.MACD < technical_rec.MACD_Signal and prev_tech_rec.MACD >= prev_tech_rec.MACD_Signal:
            satisfied_filters.append("MACD_Bearish_Cross_Confirmed")
            reason_parts["technical"].append(f"کراس نزولی معتبر MACD ({technical_rec.MACD:.2f}) زیر سیگنال ({technical_rec.MACD_Signal:.2f}) رخ داد.")

    # Price vs SMA — از technical_rec استفاده کن اگر هست، در غیر این صورت از historical close استفاده کن
    last_close_val = None
    if technical_rec is not None and getattr(technical_rec, 'close_price', None) is not None:
        last_close_val = technical_rec.close_price
    elif not close_ser.empty:
        last_close_val = close_ser.iloc[-1]

    if last_close_val is not None:
        if technical_rec is not None and getattr(technical_rec, 'SMA_20', None) is not None and last_close_val > technical_rec.SMA_20:
            satisfied_filters.append("Price_Above_SMA20")
            reason_parts["technical"].append(f"قیمت ({last_close_val:.0f}) بالای SMA-20 ({technical_rec.SMA_20:.0f}) است.")
        if technical_rec is not None and getattr(technical_rec, 'SMA_50', None) is not None and last_close_val > technical_rec.SMA_50:
            satisfied_filters.append("Price_Above_SMA50")
            reason_parts["technical"].append(f"قیمت ({last_close_val:.0f}) بالای SMA-50 ({technical_rec.SMA_50:.0f}) است.")

    # Bollinger — فقط اگر مقادیر موجود باشند
    if technical_rec is not None and getattr(technical_rec, 'Bollinger_Low', None) is not None and getattr(technical_rec, 'Bollinger_High', None) is not None and last_close_val is not None:
        if last_close_val < technical_rec.Bollinger_Low:
            satisfied_filters.append("Bollinger_Lower_Band_Touch")
            reason_parts["technical"].append(f"قیمت ({last_close_val:.0f}) به باند پایین بولینگر باند ({technical_rec.Bollinger_Low:.0f}) رسید.")
        elif last_close_val > technical_rec.Bollinger_High:
            satisfied_filters.append("Bollinger_Upper_Band_Breakout")
            reason_parts["technical"].append(f"قیمت ({last_close_val:.0f}) از باند بالای بولینگر باند ({technical_rec.Bollinger_High:.0f}) عبور کرد.")

    # Volume Z-score — فقط اگر ستون volume وجود داشته باشد و حداقل 20 مقدار برای Z-score داشته باشیم
    if hist_df is not None and 'volume' in hist_df.columns and len(hist_df) >= 20:
        try:
            volume_z_score = calculate_z_score(pd.to_numeric(hist_df['volume'], errors='coerce').dropna().iloc[-20:])
            if volume_z_score is not None and volume_z_score > 1.5:
                satisfied_filters.append("High_Volume_ZScore")
                reason_parts["technical"].append(f"حجم معاملات با Z-Score بالا ({volume_z_score:.2f}) غیرعادی است.")
        except Exception as e:
            logger.debug(f"Volume Z-score calculation failed: {e}")

    # ATR volatility
    if technical_rec is not None and getattr(technical_rec, 'ATR', None) is not None and last_close_val:
        try:
            if technical_rec.ATR > 0 and last_close_val > 0:
                volatility_percent = (technical_rec.ATR / last_close_val) * 100
                if volatility_percent > 3:
                    satisfied_filters.append("High_Volatility_ATR")
                    reason_parts["technical"].append(f"ATR ({technical_rec.ATR:.2f}) نشان‌دهنده نوسان بالا ({volatility_percent:.2f}% از قیمت) است.")
        except Exception as e:
            logger.debug(f"ATR check failed: {e}")

    return satisfied_filters, reason_parts

def _check_fundamental_filters(fundamental_rec):
    """
    Applies fundamental filters including P/S, P/B, ROE, and DPS.
    """
    satisfied_filters = []
    reason_parts = {"fundamental": []}

    if fundamental_rec:
        if fundamental_rec.pe is not None and 0 < fundamental_rec.pe < 20:
            satisfied_filters.append("Reasonable_PE")
            reason_parts["fundamental"].append(f"نسبت P/E ({fundamental_rec.pe:.2f}) مناسب است.")
        if fundamental_rec.ps is not None and fundamental_rec.ps > 0 and fundamental_rec.ps < 5:
            satisfied_filters.append("Reasonable_PS")
            reason_parts["fundamental"].append(f"نسبت P/S ({fundamental_rec.ps:.2f}) مناسب است.")
        if fundamental_rec.pb is not None and fundamental_rec.pb > 0 and fundamental_rec.pb < 2:
            satisfied_filters.append("Reasonable_PB")
            reason_parts["fundamental"].append(f"نسبت P/B ({fundamental_rec.pb:.2f}) مناسب است.")
        if fundamental_rec.roe is not None and fundamental_rec.roe > 15:
            satisfied_filters.append("High_ROE")
            reason_parts["fundamental"].append(f"بازده حقوق صاحبان سهام (ROE) بالا ({fundamental_rec.roe:.2f}%) است.")
        if fundamental_rec.eps is not None and fundamental_rec.eps > 0:
            satisfied_filters.append("Positive_EPS")
            reason_parts["fundamental"].append(f"EPS ({fundamental_rec.eps:.2f}) مثبت است.")
        if fundamental_rec.dps is not None and fundamental_rec.eps is not None and fundamental_rec.eps > 0:
            payout_ratio = (fundamental_rec.dps / fundamental_rec.eps) * 100
            if payout_ratio > 40:
                satisfied_filters.append("High_Payout_Ratio")
                reason_parts["fundamental"].append(f"نسبت سود تقسیمی ({payout_ratio:.2f}%) بالا است.")

    return satisfied_filters, reason_parts

def _check_smart_money_filters(hist_df):
    """
    Applies smart money flow filters, considering a trend over 3-5 days.
    """
    satisfied_filters = []
    reason_parts = {"smart_money": []}

    if hist_df is None or hist_df.empty or 'buy_i_volume' not in hist_df.columns or len(hist_df) < 5:
        return satisfied_filters, reason_parts

    smart_money_flow_df = calculate_smart_money_flow(hist_df)

    if not smart_money_flow_df.empty:
        trend_lookback = 3
        if len(smart_money_flow_df) >= trend_lookback:
            trend_net_flow = smart_money_flow_df['individual_net_flow'].iloc[-trend_lookback:].sum()
            if trend_net_flow > 0:
                satisfied_filters.append("Positive_Real_Money_Flow_Trend")
                reason_parts["smart_money"].append(f"روند {trend_lookback} روزه ورود پول حقیقی مثبت است.")
            elif trend_net_flow < 0:
                satisfied_filters.append("Negative_Real_Money_Flow_Trend")
                reason_parts["smart_money"].append(f"روند {trend_lookback} روزه خروج پول حقیقی منفی است.")
        
        latest_row = hist_df.iloc[-1]
        individual_buy_share = latest_row.get('buy_i_share', 0)
        institutional_buy_share = latest_row.get('buy_n_share', 0)
        total_buy_share = individual_buy_share + institutional_buy_share
        
        if total_buy_share > 0:
            individual_buy_percent = (individual_buy_share / total_buy_share) * 100
            if individual_buy_percent > 70:
                satisfied_filters.append("High_Individual_Participation")
                reason_parts["smart_money"].append(f"مشارکت بالای حقیقی‌ها در خرید ({individual_buy_percent:.2f}%) دیده شد.")
            elif individual_buy_percent < 30:
                satisfied_filters.append("High_Institutional_Participation")
                reason_parts["smart_money"].append(f"مشارکت بالای حقوقی‌ها در خرید ({100 - individual_buy_percent:.2f}%) دیده شد.")

    return satisfied_filters, reason_parts

def run_weekly_watchlist_selection():
    """
    Selects symbols for the weekly watchlist using a bulk data fetching and processing approach.
    """
    logger.info("Starting Weekly Watchlist selection process.")

    allowed_market_types = ['بورس', 'فرابورس', 'بورس کالا', 'بورس انرژی', 'پایه فرابورس']
    symbols_to_analyze = ComprehensiveSymbolData.query.filter(
        ComprehensiveSymbolData.market_type.in_(allowed_market_types)
    ).all()

    if not symbols_to_analyze:
        logger.warning("No symbols found for watchlist analysis. Skipping.")
        return False, "No symbols found for watchlist analysis."
    
    symbol_ids = [s.symbol_id for s in symbols_to_analyze]
    
    # ✅ اصلاح: استفاده از jdatetime برای محاسبه تاریخ برش شمسی
    cutoff_date_j = (jdatetime.date.today() - jdatetime.timedelta(days=TECHNICAL_DATA_LOOKBACK_DAYS + 10)).strftime('%Y-%m-%d')
    
    # Bulk Data Fetching with a date filter
    logger.info(f"Fetching bulk data for {len(symbol_ids)} symbols for the last ~{TECHNICAL_DATA_LOOKBACK_DAYS+10} days...")
    logger.info(f"Querying for symbols: {symbol_ids[:5]}... (first 5 of {len(symbol_ids)})")
    logger.info(f"Querying with cutoff jdate: {cutoff_date_j}")
    
    try:
        # ✅ استفاده از ORM برای فراخوانی داده‌های تاریخی (مدل صحیح)
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

    except Exception as e:
        logger.error(f"❌ Error during bulk data fetching: {e}", exc_info=True)
        # ⚠️ اطمینان از بازگرداندن یک DataFrame خالی در صورت خطا
        hist_df = pd.DataFrame()
        tech_df = pd.DataFrame()
    
    logger.info(f"Fetched {len(historical_records)} historical records")
    logger.info(f"Historical DataFrame columns: {list(hist_df.columns) if not hist_df.empty else 'Empty DataFrame'}")
    logger.info(f"Fetched {len(technical_records)} technical records") 
    logger.info(f"Technical DataFrame columns: {list(tech_df.columns) if not tech_df.empty else 'Empty DataFrame'}")

    # ⚠️ اصلاح اصلی: بررسی وجود داده و ستون قبل از گروه‌بندی
    if not hist_df.empty and 'symbol_id' in hist_df.columns:
        hist_df = hist_df.drop(columns=['_sa_instance_state'], errors='ignore')
        hist_groups = {k: v.sort_values(by='jdate') for k, v in hist_df.groupby("symbol_id")}
    else:
        hist_groups = {}
        logger.warning("Historical data DataFrame is empty or missing 'symbol_id' column. Analysis will be limited.")

    if not tech_df.empty and 'symbol_id' in tech_df.columns:
        tech_df = tech_df.drop(columns=['_sa_instance_state'], errors='ignore')
        tech_groups = {k: v.sort_values(by='jdate') for k, v in tech_df.groupby("symbol_id")}
    else:
        tech_groups = {}
        logger.warning("Technical data DataFrame is empty or missing 'symbol_id' column. Analysis will be limited.")

    fundamental_records = FundamentalData.query.filter(FundamentalData.symbol_id.in_(symbol_ids)).all()
    fundamental_map = {rec.symbol_id: rec for rec in fundamental_records}
    
    watchlist_candidates = []
    
    # Process each symbol using the pre-grouped DataFrames
    for symbol in symbols_to_analyze:
        
        # استفاده از .get() برای جلوگیری از KeyError
        symbol_hist_df = hist_groups.get(symbol.symbol_id, pd.DataFrame()).copy()
        symbol_tech_df = tech_groups.get(symbol.symbol_id, pd.DataFrame()).copy()

        hist_count = 0 if symbol_hist_df.empty else len(symbol_hist_df)
        tech_count = 0 if symbol_tech_df.empty else len(symbol_tech_df)
        logger.debug(f"{symbol.symbol_name} ({symbol.symbol_id}) - hist rows: {hist_count}, tech rows: {tech_count}")

        # حداقل شرط: حداقل MIN_REQUIRED_HISTORY_DAYS داده تاریخی لازم است
        if hist_count < MIN_REQUIRED_HISTORY_DAYS:
            logger.debug(f"Skipping {symbol.symbol_name} due to insufficient historical rows ({hist_count} < {MIN_REQUIRED_HISTORY_DAYS}).")
            continue

        # اگر technical data کمتر از مقدار lookback است، باز هم ادامه بده اما با fallback
        # فقط داده‌های در دسترس را برای تحلیل بردار
        symbol_hist_df = symbol_hist_df.tail(min(TECHNICAL_DATA_LOOKBACK_DAYS, hist_count))
        if not symbol_tech_df.empty:
            symbol_tech_df = symbol_tech_df.tail(min(TECHNICAL_DATA_LOOKBACK_DAYS, len(symbol_tech_df)))

        # اگر tech داده ندارد، بسادگی technical_rec را از آخرین close تاریخی fallback کن
        if symbol_tech_df.empty:
            last_close_series = _get_close_series_from_hist_df(symbol_hist_df)
            last_close = float(last_close_series.iloc[-1]) if not last_close_series.empty else None
            technical_rec = SimpleNamespace(
                close_price = last_close,
                MACD = None, MACD_Signal = None, RSI = None,
                SMA_20 = None, SMA_50 = None,
                Bollinger_Low = None, Bollinger_High = None,
                ATR = None
            )
            # برای _check_technical_filters ما نیاز به dataframe تکنیکال داریم؛
            # می‌فرستیم یک df خالی تا داخل تابع تشخیص بدهد و از technical_rec استفاده کند.
            tech_filters, tech_reasons = _check_technical_filters(symbol_hist_df, pd.DataFrame())
        else:
            technical_rec = symbol_tech_df.iloc[-1]
            tech_filters, tech_reasons = _check_technical_filters(symbol_hist_df, symbol_tech_df)

        fundamental_rec = fundamental_map.get(symbol.symbol_id)
        
        all_satisfied_filters = []
        all_reason_parts = {}
            
        all_satisfied_filters.extend(tech_filters)
        all_reason_parts.update(tech_reasons)

        fund_filters, fund_reasons = _check_fundamental_filters(fundamental_rec)
        all_satisfied_filters.extend(fund_filters)
        all_reason_parts.update(fund_reasons)
        if not fundamental_rec:
            all_reason_parts["fundamental"] = ["No fundamental data available."]

        smart_money_filters, smart_money_reasons = _check_smart_money_filters(symbol_hist_df)
        all_satisfied_filters.extend(smart_money_filters)
        all_reason_parts.update(smart_money_reasons)
        
        score = sum(FILTER_WEIGHTS.get(f, 0) for f in all_satisfied_filters)

        # از technical_rec.close_price استفاده کن، چون با fallback هم مقدار دارد
        entry_price = getattr(technical_rec, 'close_price', None)
        if entry_price is None or pd.isna(entry_price):
            logger.warning(f"Skipping {symbol.symbol_name} due to missing entry price.")
            continue
            
        if score >= 5:
            watchlist_candidates.append({
                "symbol_id": symbol.symbol_id,
                "symbol_name": symbol.symbol_name,
                "entry_price": entry_price,
                "entry_date": date.today(),
                "jentry_date": get_today_jdate_str(),
                "outlook": "Bullish" if score > 5 else "Neutral",
                "reason_json": json.dumps(all_reason_parts),
                "satisfied_filters": json.dumps(all_satisfied_filters),
                "score": score
            })
    
    logger.info(f"Found {len(watchlist_candidates)} total candidates. Sorting and saving top candidates.")
            
    watchlist_candidates.sort(key=lambda x: x['score'], reverse=True)
    top_n_symbols = 8
    final_watchlist = watchlist_candidates[:top_n_symbols]

    saved_count = 0
    for candidate in final_watchlist:
        existing_result = WeeklyWatchlistResult.query.filter_by(
            symbol_id=candidate['symbol_id'],
            jentry_date=candidate['jentry_date']
        ).first()

        if existing_result:
            existing_result.entry_price = candidate['entry_price']
            existing_result.outlook = candidate['outlook']
            existing_result.reason = candidate['satisfied_filters'] 
            existing_result.probability_percent = min(100, candidate['score'] * 5)
            existing_result.created_at = datetime.now()
            db.session.add(existing_result)
        else:
            new_result = WeeklyWatchlistResult(
                signal_unique_id=str(uuid.uuid4()),
                symbol_id=candidate['symbol_id'],
                symbol_name=candidate['symbol_name'],
                entry_price=candidate['entry_price'],
                entry_date=candidate['entry_date'],
                jentry_date=candidate['jentry_date'],
                outlook=candidate['outlook'],
                reason=candidate['satisfied_filters'], 
                probability_percent=min(100, candidate['score'] * 5),
                created_at=datetime.now(),
                status='active',
            )
            db.session.add(new_result)
        saved_count += 1
    
    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. Saved top {saved_count} symbols."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during Weekly Watchlist selection: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

# --- Weekly Watchlist Performance Evaluation ---
def evaluate_weekly_watchlist_performance():
    """
    Evaluates the performance of active weekly watchlist signals.
    Calculates profit/loss and updates status using dynamic stop-loss/take-profit.
    Moves evaluated signals from WeeklyWatchlistResult to SignalsPerformance.
    """
    logger.info("Starting Weekly Watchlist performance evaluation.")
    
    today_jdate_str = get_today_jdate_str()
    current_greg_date = datetime.now().date()

    active_watchlist_entries = WeeklyWatchlistResult.query.filter(
        WeeklyWatchlistResult.status == 'active',
    ).all()

    if not active_watchlist_entries:
        logger.warning("No active weekly watchlist entries found for evaluation.")
        return False, "No active watchlist entries to evaluate."

    evaluated_count = 0
    for entry in active_watchlist_entries:
        logger.debug(f"Evaluating performance for {entry.symbol_name} (ID: {entry.symbol_id}).")

        # ✅ استفاده از ORM برای فراخوانی داده‌های تاریخی
        latest_historical_data = HistoricalData.query.filter(
            HistoricalData.symbol_id == entry.symbol_id
        ).order_by(HistoricalData.jdate.desc()).first()
        
        latest_technical_data = TechnicalIndicatorData.query.filter_by(symbol_id=entry.symbol_id).order_by(TechnicalIndicatorData.jdate.desc()).first()

        if not latest_historical_data:
            logger.warning(f"No HistoricalData found for {entry.symbol_id}. Cannot evaluate.")
            continue
        if not latest_technical_data or latest_technical_data.ATR is None:
            logger.warning(f"No TechnicalIndicatorData with ATR for {entry.symbol_id}. Cannot evaluate with dynamic SL/TP. Skipping this signal.")
            continue

        current_price = normalize_value(latest_historical_data.final)
        if current_price is None or current_price <= 0:
            logger.warning(f"Invalid current price for {entry.symbol_name}. Cannot evaluate.")
            continue

        stop_loss_price = entry.entry_price - (1.5 * latest_technical_data.ATR)
        take_profit_price = entry.entry_price + (3 * latest_technical_data.ATR)

        profit_loss_percent = ((current_price - entry.entry_price) / entry.entry_price) * 100

        status = 'active'
        evaluation_reason = ""
        
        try:
            entry_jdate_obj = jdatetime.date(*map(int, entry.jentry_date.split('-')))
            current_jdate_obj = jdatetime.date.today()
            if (current_jdate_obj - entry_jdate_obj).days >= 7:
                status = 'closed_expired'
                evaluation_reason = "Expired after 7 days."
        except ValueError:
            pass

        if current_price >= take_profit_price:
            status = 'closed_win'
            evaluation_reason = f"Hit Take Profit target ({take_profit_price:.0f})."
        elif current_price <= stop_loss_price:
            status = 'closed_loss'
            evaluation_reason = f"Hit Stop Loss target ({stop_loss_price:.0f})."
        elif status == 'active':
            if (current_jdate_obj - entry_jdate_obj).days >= 7:
                status = 'closed_neutral'
                evaluation_reason = "Expired after 7 trading days without hitting target."

        if status != 'active':
            performance_record = SignalsPerformance(
                signal_unique_id=entry.signal_unique_id,
                symbol_id=entry.symbol_id,
                symbol_name=entry.symbol_name,
                entry_date=entry.entry_date,
                entry_price=entry.entry_price,
                exit_date=current_greg_date,
                exit_price=current_price,
                profit_loss_percentage=profit_loss_percent,
                final_status=status,
                reasons=entry.reason,
                evaluation_reason=evaluation_reason,
                created_at=datetime.now()
            )
            db.session.add(performance_record)

            entry.exit_price = current_price
            entry.jexit_date = today_jdate_str
            entry.exit_date = current_greg_date
            entry.profit_loss_percentage = profit_loss_percent
            entry.status = status
            entry.updated_at = datetime.now()
            db.session.add(entry)
            logger.info(f"Signal {entry.signal_unique_id} for {entry.symbol_name} evaluated. Status: {status}, P/L: {profit_loss_percent:.2f}%.")
            evaluated_count += 1
    
    try:
        db.session.commit()
        message = f"Weekly Watchlist selection completed. Saved top {saved_count} symbols."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during Weekly Watchlist selection: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message