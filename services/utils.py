# -*- coding: utf-8 -*-
# services/utils.py - توابع کمکی برای محاسبات مالی و تبدیل تاریخ

from __future__ import annotations
import jdatetime
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import func
import logging
from functools import lru_cache
from typing import Union, List, Dict, Optional, Tuple, Any

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# --- توابع عمومی و تبدیل تاریخ ---

def convert_gregorian_to_jalali(gregorian_date_obj: Union[datetime.date, datetime.datetime, Any]) -> Optional[str]:
    """
    تبدیل یک شیء datetime.date یا datetime.datetime به رشته تاریخ جلالی (YYYY-MM-DD).
    """
    try:
        if pd.isna(gregorian_date_obj):
            return None

        if isinstance(gregorian_date_obj, datetime.datetime):
            gregorian_dt = gregorian_date_obj
        elif isinstance(gregorian_date_obj, datetime.date):
            gregorian_dt = datetime.datetime(gregorian_date_obj.year, gregorian_date_obj.month, gregorian_date_obj.day)
        else:
            logger.warning(f"نوع ورودی نامعتبر برای تبدیل تاریخ: {type(gregorian_date_obj)}")
            return None

        jdate_obj = jdatetime.date.fromgregorian(
            year=gregorian_dt.year,
            month=gregorian_dt.month,
            day=gregorian_dt.day
        ).strftime('%Y-%m-%d')

        return jdate_obj
    except (ValueError, TypeError) as e:
        logger.error(f"خطا در تبدیل تاریخ میلادی به جلالی: {e} - ورودی: {gregorian_date_obj}")
        return None
    except Exception as e:
        logger.error(f"خطای ناشناخته در تبدیل تاریخ میلادی به جلالی: {e} - ورودی: {gregorian_date_obj}")
        return None

def get_today_jdate_str() -> str:
    """
    بازگرداندن تاریخ امروز به فرمت جلالی (شمسی) به صورت رشته YYYY-MM-DD.
    """
    return jdatetime.date.today().strftime('%Y-%m-%d')

def normalize_value(val: Any) -> Optional[Union[float, int]]:
    """
    نرمال‌سازی یک مقدار، با مدیریت لیست‌ها، Pandas Series و فرمت‌های رشته‌ای خاص
    برای استخراج یک مقدار عددی اسکالر.
    """
    if isinstance(val, (list, pd.Series)):
        return val.iloc[0] if len(val) > 0 else None
    elif isinstance(val, str):
        if 'Name:' in val:
            try:
                parts = val.split()
                for part in parts:
                    if part.replace('.', '', 1).isdigit():
                        return float(part)
            except ValueError:
                logger.warning(f"خطا در تبدیل رشته '{val}' به عدد.")
                return None
        try:
            return float(val)
        except ValueError:
            logger.warning(f"خطا در تبدیل رشته '{val}' به عدد.")
            return None
    return val

# --- توابع تحلیل تکنیکال با Caching و Type Hints ---

@lru_cache(maxsize=128)
def calculate_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """
    محاسبه شاخص قدرت نسبی (RSI).
    Args:
        series (pd.Series): سری قیمت‌های بسته شدن.
        window (int): دوره بازبینی.
    Returns:
        pd.Series: سری مقادیر RSI.
    """
    series = pd.to_numeric(series, errors='coerce')
    series_cleaned = series.dropna()
    if series.isnull().all() or len(series_cleaned) < window:
        return pd.Series([np.nan] * len(series), index=series.index)

    delta = series_cleaned.diff().dropna()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)

    avg_gain = gain.ewm(span=window, adjust=False).mean()
    avg_loss = loss.ewm(span=window, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0)

    rsi = 100 - (100 / (1 + rs))
    final_rsi = rsi.replace([np.inf, -np.inf], np.nan)

    return final_rsi.reindex(series.index)

@lru_cache(maxsize=128)
def calculate_macd(series: pd.Series, short_window: int = 12, long_window: int = 26, signal_window: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    محاسبه MACD، خط سیگنال MACD و هیستوگرام MACD.
    Args:
        series (pd.Series): سری قیمت‌های بسته شدن.
    Returns:
        Tuple[pd.Series, pd.Series, pd.Series]: (MACD, MACD Signal, MACD Histogram)
    """
    series = pd.to_numeric(series, errors='coerce')
    series_cleaned = series.dropna()
    if series.isnull().all() or len(series_cleaned) < long_window:
        nan_series = pd.Series([np.nan] * len(series), index=series.index)
        return nan_series, nan_series, nan_series

    exp1 = series_cleaned.ewm(span=short_window, adjust=False).mean()
    exp2 = series_cleaned.ewm(span=long_window, adjust=False).mean()
    macd = exp1 - exp2
    macd_signal = macd.ewm(span=signal_window, adjust=False).mean()
    macd_hist = macd - macd_signal

    return (macd.reindex(series.index), macd_signal.reindex(series.index), macd_hist.reindex(series.index))

@lru_cache(maxsize=128)
def calculate_sma(series: pd.Series, window: int) -> pd.Series:
    """
    محاسبه میانگین متحرک ساده (SMA).
    Args:
        series (pd.Series): سری قیمت‌ها.
        window (int): دوره بازبینی.
    Returns:
        pd.Series: سری مقادیر SMA.
    """
    series = pd.to_numeric(series, errors='coerce')
    series_cleaned = series.dropna()
    if series.isnull().all() or len(series_cleaned) < window:
        return pd.Series([np.nan] * len(series), index=series.index)

    sma = series_cleaned.rolling(window=window).mean()
    return sma.reindex(series.index)

@lru_cache(maxsize=128)
def calculate_bollinger_bands(series: pd.Series, window: int = 20, num_std_dev: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    محاسبه باندهای بولینگر.
    Args:
        series (pd.Series): سری قیمت‌ها.
        window (int): دوره بازبینی.
        num_std_dev (int): تعداد انحراف معیار.
    Returns:
        Tuple[pd.Series, pd.Series, pd.Series]: (میانگین متحرک, باند بالا, باند پایین)
    """
    series = pd.to_numeric(series, errors='coerce')
    series_cleaned = series.dropna()
    if series.isnull().all() or len(series_cleaned) < window:
        nan_series = pd.Series([np.nan] * len(series), index=series.index)
        return nan_series, nan_series, nan_series

    ma = series_cleaned.rolling(window=window).mean()
    std = series_cleaned.rolling(window=window).std()

    upper_band = ma + (std * num_std_dev)
    lower_band = ma - (std * num_std_dev)

    return (ma.reindex(series.index), upper_band.reindex(series.index), lower_band.reindex(series.index))

@lru_cache(maxsize=128)
def calculate_volume_ma(series: pd.Series, window: int = 20) -> pd.Series:
    """
    محاسبه میانگین متحرک حجم.
    Args:
        series (pd.Series): سری حجم معاملات.
        window (int): دوره بازبینی.
    Returns:
        pd.Series: سری مقادیر میانگین متحرک حجم.
    """
    series = pd.to_numeric(series, errors='coerce')
    series_cleaned = series.dropna()
    if series.isnull().all() or len(series_cleaned) < window:
        return pd.Series([np.nan] * len(series), index=series.index)

    volume_ma = series_cleaned.rolling(window=window).mean()
    return volume_ma.reindex(series.index)

@lru_cache(maxsize=128)
def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
    """
    محاسبه Average True Range (ATR).
    Args:
        high (pd.Series): سری قیمت‌های بالا.
        low (pd.Series): سری قیمت‌های پایین.
        close (pd.Series): سری قیمت‌های بسته شدن.
        window (int): دوره بازبینی.
    Returns:
        pd.Series: سری مقادیر ATR.
    """
    combined_df = pd.DataFrame({'high': high, 'low': low, 'close': close})
    if combined_df.empty or len(combined_df.dropna()) < window + 1:
        return pd.Series([np.nan] * len(high), index=high.index)

    tr1 = combined_df['high'] - combined_df['low']
    tr2 = np.abs(combined_df['high'] - combined_df['close'].shift(1))
    tr3 = np.abs(combined_df['low'] - combined_df['close'].shift(1))
    
    true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    
    atr = true_range.ewm(span=window, adjust=False, min_periods=window).mean()
    
    return atr.reindex(high.index)

def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """
    محاسبه Volume-Weighted Average Price (VWAP).
    Args:
        df (pd.DataFrame): DataFrame شامل ستون‌های 'close_price' و 'volume'.
    Returns:
        pd.Series: سری مقادیر VWAP.
    """
    if 'close_price' not in df.columns or 'volume' not in df.columns or df.empty:
        logger.error("برای محاسبه VWAP، ستون‌های 'close_price' و 'volume' لازم هستند.")
        return pd.Series(index=df.index, dtype=float)
    
    df_copy = df.copy()
    df_copy['close_price'] = pd.to_numeric(df_copy['close_price'], errors='coerce')
    df_copy['volume'] = pd.to_numeric(df_copy['volume'], errors='coerce')
    
    df_copy['pv'] = df_copy['close_price'] * df_copy['volume']
    
    vwap = df_copy['pv'].cumsum() / df_copy['volume'].cumsum()
    return vwap

def get_symbol_id(input_param: Optional[str]) -> Optional[str]:
    """
    تبدیل نام نماد (نام کوتاه فارسی) به symbol_id، یا تلاش برای یافتن نام کوتاه
    فارسی در صورت ارائه ISIN.
    """
    from models import ComprehensiveSymbolData, db

    if input_param is None:
        return None

    session = db.session if hasattr(db, 'session') else None
    if session is None:
        logger.error("db.session در دسترس نیست. نمی‌توان symbol_id را واکشی کرد.")
        return None

    try:
        symbol_data = session.query(ComprehensiveSymbolData).filter(
            func.lower(ComprehensiveSymbolData.symbol_name) == func.lower(input_param)
        ).first()
        if symbol_data:
            return symbol_data.symbol_id

        if isinstance(input_param, str) and input_param.startswith('IRO1'):
            symbol_data = session.query(ComprehensiveSymbolData).filter_by(isin=input_param).first()
            if symbol_data:
                return symbol_data.symbol_id

        symbol_data = session.query(ComprehensiveSymbolData).filter(
            func.lower(ComprehensiveSymbolData.company_name) == func.lower(input_param)
        ).first()
        if symbol_data:
            return symbol_data.symbol_id

    except Exception as e:
        logger.error(f"خطا در واکشی symbol_id برای '{input_param}': {e}")
    finally:
        pass

    logger.warning(f"symbol_id برای ورودی '{input_param}' یافت نشد.")
    return None

# --- توابع اضافه شده برای سرویس Weekly Watchlist ---

def calculate_smart_money_flow(df: pd.DataFrame) -> pd.DataFrame:
    """
    محاسبه معیارهای جریان پول هوشمند از داده‌های تاریخی.
    Args:
        df (pd.DataFrame): DataFrame شامل ستون‌های 'buy_i_volume', 'sell_i_volume',
                         'buy_count_i', 'sell_count_i', 'value'.
    Returns:
        pd.DataFrame: DataFrameای حاوی معیارهای محاسبه شده.
    """
    required_cols = ['buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i', 'value']
    missing_columns = [col for col in required_cols if col not in df.columns]
    
    df_copy = df.copy()
    if missing_columns:
        logger.warning(f"ستون‌های مورد نیاز برای محاسبه جریان پول هوشمند یافت نشدند: {missing_columns}.")
        for col in missing_columns:
            df_copy[col] = np.nan
    
    for col in required_cols:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    df_copy['individual_buy_power'] = df_copy['buy_i_volume'] / df_copy['sell_i_volume'].replace(0, np.nan)
    df_copy['individual_buy_power'] = df_copy['individual_buy_power'].replace([np.inf, -np.inf], np.nan).fillna(0)

    df_copy['individual_net_flow'] = df_copy['buy_i_volume'] - df_copy['sell_i_volume']

    df_copy['individual_buy_per_trade'] = df_copy['buy_i_volume'] / df_copy['buy_count_i'].replace(0, np.nan)
    df_copy['individual_sell_per_trade'] = df_copy['sell_i_volume'] / df_copy['sell_count_i'].replace(0, np.nan)
    df_copy['individual_buy_per_trade'] = df_copy['individual_buy_per_trade'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df_copy['individual_sell_per_trade'] = df_copy['individual_sell_per_trade'].replace([np.inf, -np.inf], np.nan).fillna(0)

    if 'jdate' in df_copy.columns:
        return df_copy[['jdate', 'individual_buy_power', 'individual_net_flow', 'individual_buy_per_trade', 'individual_sell_per_trade']].copy()
    else:
        return df_copy[['individual_buy_power', 'individual_net_flow', 'individual_buy_per_trade', 'individual_sell_per_trade']].copy()

def check_candlestick_patterns(today_candle_data: Dict[str, Union[float, int]], 
                              yesterday_candle_data: Dict[str, Union[float, int]], 
                              historical_data: pd.DataFrame) -> List[str]:
    """
    بررسی الگوهای شمعی رایج و پیشرفته با تأیید حجم.
    Args:
        today_candle_data (dict): دیکشنری با 'open', 'high', 'low', 'close', 'volume' برای امروز.
        yesterday_candle_data (dict): دیکشنری با 'open', 'high', 'low', 'close', 'volume' برای دیروز.
        historical_data (pd.DataFrame): DataFrame کامل داده‌های تاریخی شامل 'close_price' و 'volume'.
    Returns:
        List[str]: لیستی از نام الگوهای شمعی شناسایی شده.
    """
    detected_patterns = []

    if not all(k in today_candle_data and k in yesterday_candle_data for k in ['open', 'high', 'low', 'close']):
        logger.warning("داده‌های شمعی ناقص برای بررسی الگوهای شمعی.")
        return detected_patterns

    is_in_downtrend = False
    if 'close_price' in historical_data.columns and len(historical_data) >= 10:
        recent_closes = historical_data['close_price'].iloc[-10:]
        if not recent_closes.empty and recent_closes.iloc[0] > recent_closes.iloc[-1]:
            is_in_downtrend = True
            
    is_in_uptrend = False
    if 'close_price' in historical_data.columns and len(historical_data) >= 10:
        recent_closes = historical_data['close_price'].iloc[-10:]
        if not recent_closes.empty and recent_closes.iloc[0] < recent_closes.iloc[-1]:
            is_in_uptrend = True
            
    volume_t = today_candle_data.get('volume', 0)
    avg_volume = historical_data['volume'].iloc[-20:].mean() if 'volume' in historical_data.columns else 0
    is_high_volume = volume_t > 1.5 * avg_volume if avg_volume > 0 else False
    
    open_t, high_t, low_t, close_t = today_candle_data['open'], today_candle_data['high'], today_candle_data['low'], today_candle_data['close']
    open_y, high_y, low_y, close_y = yesterday_candle_data['open'], yesterday_candle_data['high'], yesterday_candle_data['low'], yesterday_candle_data['close']

    # --- الگوی Hammer ---
    body_t = abs(close_t - open_t)
    range_t = high_t - low_t
    lower_shadow_t = min(open_t, close_t) - low_t
    upper_shadow_t = high_t - max(open_t, close_t)
    if (range_t > 0 and body_t > 0 and body_t < (0.3 * range_t) and 
        lower_shadow_t >= 2 * body_t and upper_shadow_t < 0.1 * body_t and 
        is_in_downtrend):
        detected_patterns.append("Hammer")

    # --- الگوی Bullish Engulfing ---
    if (close_y < open_y and close_t > open_t and open_t < close_y and close_t > open_y and 
        is_in_downtrend and is_high_volume):
        detected_patterns.append("Bullish Engulfing (با تأیید حجم)")

    # --- الگوی Morning Star (نیاز به داده سه روزه) ---
    if len(historical_data) >= 3:
        day1 = historical_data.iloc[-3]
        day2 = historical_data.iloc[-2]
        day3 = historical_data.iloc[-1]
        
        if day1['close_price'] < day1['open_price']:
            if abs(day2['close_price'] - day2['open_price']) < (day2['high_price'] - day2['low_price']) * 0.2:
                if (day3['close_price'] > day3['open_price'] and
                    day3['close_price'] > (day1['open_price'] + day1['close_price']) / 2):
                    if is_in_downtrend:
                        detected_patterns.append("Morning Star")
                        
    # --- الگوی Evening Star (نیاز به داده سه روزه) ---
    if len(historical_data) >= 3:
        day1 = historical_data.iloc[-3]
        day2 = historical_data.iloc[-2]
        day3 = historical_data.iloc[-1]
        
        if day1['close_price'] > day1['open_price']:
            if abs(day2['close_price'] - day2['open_price']) < (day2['high_price'] - day2['low_price']) * 0.2:
                if (day3['close_price'] < day3['open_price'] and
                    day3['close_price'] < (day1['open_price'] + day1['close_price']) / 2):
                    if is_in_uptrend:
                        detected_patterns.append("Evening Star")

    return detected_patterns

def check_tsetmc_filters(symbol_id: str, jdate_str: str) -> Tuple[List[str], List[str]]:
    """
    تابع Placeholder برای بررسی نتایج فیلترهای TSETMC.
    """
    satisfied_filters = []
    reasons = []
    return satisfied_filters, reasons

def check_financial_ratios(symbol_id: str) -> Tuple[List[str], List[str]]:
    """
    تابع Placeholder برای بررسی نسبت‌های مالی.
    """
    satisfied_ratios = []
    reasons = []
    return satisfied_ratios, reasons
    
def calculate_z_score(series: pd.Series) -> Optional[float]:
    """
    محاسبه Z-Score برای یک pandas Series.
    Args:
        series (pd.Series): سری داده‌های عددی.
    Returns:
        Optional[float]: مقدار Z-Score آخرین نقطه داده یا None.
    """
    series_cleaned = pd.to_numeric(series, errors='coerce').dropna()
    if series_cleaned.empty or len(series_cleaned) < 2:
        return None
    
    mean = series_cleaned.mean()
    std = series_cleaned.std()
    
    if std == 0:
        return 0.0
        
    z_score = (series_cleaned.iloc[-1] - mean) / std
    return float(z_score)