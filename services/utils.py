# -*- coding: utf-8 -*-
# services/utils.py - توابع کمکی برای محاسبات مالی و تبدیل تاریخ

import jdatetime
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import func # برای استفاده از توابع دیتابیس مانند lower در کوئری‌ها

import logging # برای لاگ‌نویسی
logger = logging.getLogger(__name__) # مقداردهی اولیه logger برای این ماژول

def convert_gregorian_to_jalali(gregorian_date_obj):
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
            return None # نوع ورودی نامعتبر

        jdate_obj = jdatetime.date.fromgregorian(
            year=gregorian_dt.year,
            month=gregorian_dt.month,
            day=gregorian_dt.day
        ).strftime('%Y-%m-%d')

        return jdate_obj
    except ValueError as e:
        logger.error(f"خطا در تبدیل تاریخ میلادی به جلالی (ValueError): {e} - ورودی: {gregorian_date_obj}")
        return None
    except Exception as e:
        logger.error(f"خطای ناشناخته در تبدیل تاریخ میلادی به جلالی: {e} - ورودی: {gregorian_date_obj}")
        return None

def get_today_jdate_str():
    """
    بازگرداندن تاریخ امروز به فرمت جلالی (شمسی) به صورت رشته YYYY-MM-DD.
    """
    return jdatetime.date.today().strftime('%Y-%m-%d')

def normalize_value(val):
    """
    نرمال‌سازی یک مقدار، با مدیریت لیست‌ها، Pandas Series و فرمت‌های رشته‌ای خاص
    برای استخراج یک مقدار عددی اسکالر.
    """
    if isinstance(val, (list, pd.Series)):
        return val[0] if len(val) > 0 else None
    elif isinstance(val, str):
        if 'Name:' in val: # برای مدیریت خروجی‌های خاص Pandas Series در برخی موارد
            try:
                parts = val.split()
                for part in parts:
                    if part.replace('.', '', 1).isdigit(): # بررسی که آیا رشته عددی است
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

def calculate_rsi(series, window=14):
    """
    محاسبه شاخص قدرت نسبی (RSI).
    ورودی: pandas Series از قیمت‌های بسته شدن.
    """
    if not isinstance(series, pd.Series):
        logger.error("ورودی برای RSI باید یک pandas Series باشد.")
        raise TypeError("Input for RSI must be a pandas Series.")

    # اطمینان از نوع عددی و مدیریت NaNها
    series = pd.to_numeric(series, errors='coerce')
    # اگر بعد از تبدیل، همه مقادیر NaN شدند، یک سری با NaN برمی‌گردانیم.
    if series.isnull().all():
        return pd.Series([np.nan] * len(series), index=series.index)

    # حذف NaNها برای محاسبات (توجه: این ممکن است طول سری را تغییر دهد)
    series_cleaned = series.dropna()
    if len(series_cleaned) < window:
        logger.warning(f"داده کافی ({len(series_cleaned)}) برای محاسبه RSI با window={window} وجود ندارد. بازگرداندن NaN.")
        return pd.Series([np.nan] * len(series), index=series.index)

    delta = series_cleaned.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)

    avg_gain = gain.ewm(span=window, adjust=False).mean()
    avg_loss = loss.ewm(span=window, adjust=False).mean()

    # جلوگیری از تقسیم بر صفر با جایگزینی 0 با NaN در avg_loss قبل از تقسیم
    rs = avg_gain / avg_loss.replace(0, np.nan)
    # مدیریت مقادیر نامحدود که ممکن است از تقسیم بر صفر ناشی شوند، سپس NaNها را با 0 پر می‌کنیم.
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0)

    rsi = 100 - (100 / (1 + rs))
    # اطمینان از عدم وجود inf/NaN در خروجی نهایی، با 0 یا مقدار مناسب دیگر پر می‌کنیم.
    final_rsi = rsi.replace([np.inf, -np.inf], np.nan).fillna(0)

    # بازگرداندن سری با ایندکس اصلی
    return final_rsi.reindex(series.index)

def calculate_macd(series, short_window=12, long_window=26, signal_window=9):
    """
    محاسبه MACD، خط سیگنال MACD و هیستوگرام MACD.
    ورودی: pandas Series از قیمت‌های بسته شدن.
    """
    if not isinstance(series, pd.Series):
        logger.error("ورودی برای MACD باید یک pandas Series باشد.")
        raise TypeError("Input for MACD must be a pandas Series.")

    series = pd.to_numeric(series, errors='coerce')
    if series.isnull().all():
        return (pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index))

    series_cleaned = series.dropna()
    if len(series_cleaned) < long_window: # MACD نیاز به داده کافی برای پنجره طولانی‌تر دارد
        logger.warning(f"داده کافی ({len(series_cleaned)}) برای محاسبه MACD با long_window={long_window} وجود ندارد. بازگرداندن NaN.")
        return (pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index))

    exp1 = series_cleaned.ewm(span=short_window, adjust=False).mean()
    exp2 = series_cleaned.ewm(span=long_window, adjust=False).mean()
    macd = exp1 - exp2
    macd_signal = macd.ewm(span=signal_window, adjust=False).mean()
    macd_hist = macd - macd_signal

    # بازگرداندن سری‌ها با ایندکس اصلی
    return (macd.reindex(series.index),
            macd_signal.reindex(series.index),
            macd_hist.reindex(series.index))

def calculate_sma(series, window):
    """
    محاسبه میانگین متحرک ساده (SMA).
    ورودی: pandas Series از قیمت‌ها.
    """
    if not isinstance(series, pd.Series):
        logger.error("ورودی برای SMA باید یک pandas Series باشد.")
        raise TypeError("Input for SMA must be a pandas Series.")

    series = pd.to_numeric(series, errors='coerce')
    if series.isnull().all():
        return pd.Series([np.nan] * len(series), index=series.index)

    series_cleaned = series.dropna()
    if len(series_cleaned) < window:
        logger.warning(f"داده کافی ({len(series_cleaned)}) برای محاسبه SMA با window={window} وجود ندارد. بازگرداندن NaN.")
        return pd.Series([np.nan] * len(series), index=series.index)

    sma = series_cleaned.rolling(window=window).mean()
    return sma.reindex(series.index)

def calculate_bollinger_bands(series, window=20, num_std_dev=2):
    """
    محاسبه باندهای بولینگر.
    ورودی: pandas Series از قیمت‌ها.
    """
    if not isinstance(series, pd.Series):
        logger.error("ورودی برای باندهای بولینگر باید یک pandas Series باشد.")
        raise TypeError("Input for Bollinger Bands must be a pandas Series.")

    series = pd.to_numeric(series, errors='coerce')
    if series.isnull().all():
        return (pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index))

    series_cleaned = series.dropna()
    if len(series_cleaned) < window:
        logger.warning(f"داده کافی ({len(series_cleaned)}) برای محاسبه باندهای بولینگر با window={window} وجود ندارد. بازگرداندن NaN.")
        return (pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index),
                pd.Series([np.nan] * len(series), index=series.index))

    ma = series_cleaned.rolling(window=window).mean()
    std = series_cleaned.rolling(window=window).std()

    upper_band = ma + (std * num_std_dev)
    lower_band = ma - (std * num_std_dev)

    # بازگرداندن سری‌ها با ایندکس اصلی
    return (ma.reindex(series.index),
            upper_band.reindex(series.index),
            lower_band.reindex(series.index))

def calculate_volume_ma(series, window=20):
    """
    محاسبه میانگین متحرک حجم.
    ورودی: pandas Series از حجم معاملات.
    """
    if not isinstance(series, pd.Series):
        logger.error("ورودی برای میانگین متحرک حجم باید یک pandas Series باشد.")
        raise TypeError("Input for Volume MA must be a pandas Series.")

    series = pd.to_numeric(series, errors='coerce')
    if series.isnull().all():
        return pd.Series([np.nan] * len(series), index=series.index)

    series_cleaned = series.dropna()
    if len(series_cleaned) < window:
        logger.warning(f"داده کافی ({len(series_cleaned)}) برای محاسبه Volume MA با window={window} وجود ندارد. بازگرداندن NaN.")
        return pd.Series([np.nan] * len(series), index=series.index)

    volume_ma = series_cleaned.rolling(window=window).mean()
    return volume_ma.reindex(series.index)

def calculate_atr(high, low, close, window=14):
    """
    محاسبه Average True Range (ATR).
    Args:
        high (pd.Series): سری قیمت‌های بالا.
        low (pd.Series): سری قیمت‌های پایین.
        close (pd.Series): سری قیمت‌های بسته شدن.
        window (int): دوره بازبینی برای ATR.
    Returns:
        pd.Series: سری مقادیر ATR.
    """
    if not (isinstance(high, pd.Series) and isinstance(low, pd.Series) and isinstance(close, pd.Series)):
        logger.error("ورودی‌های ATR باید pandas Series باشند.")
        raise TypeError("Inputs for ATR must be pandas Series.")

    # اطمینان از نوع عددی و مدیریت NaNها با پر کردن 0 در صورت لزوم برای محاسبات موقت
    # استفاده از ffill() برای پر کردن NaNها از مقادیر قبلی، سپس fillna(0) برای ابتدای سری.
    # این تغییر برای رفع FutureWarning اعمال شده است.
    high_cleaned = pd.to_numeric(high, errors='coerce').ffill().fillna(0)
    low_cleaned = pd.to_numeric(low, errors='coerce').ffill().fillna(0)
    close_cleaned = pd.to_numeric(close, errors='coerce').ffill().fillna(0)

    if len(high_cleaned) < 2: # ATR حداقل به 2 نقطه داده نیاز دارد (قیمت فعلی و بسته شدن قبلی)
        return pd.Series([np.nan] * len(high), index=high.index) # بازگرداندن NaN با ایندکس اصلی

    # محاسبه True Range (TR)
    tr1 = high_cleaned - low_cleaned
    tr2 = abs(high_cleaned - close_cleaned.shift(1)) # استفاده از shift(1) برای قیمت بسته شدن روز قبل
    tr3 = abs(low_cleaned - close_cleaned.shift(1))

    # انتخاب حداکثر از سه مقدار TR و پر کردن هر NaN با 0 (برای اولین ردیف shift)
    true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1).fillna(0)

    # محاسبه ATR با استفاده از میانگین متحرک نمایی (EMA) از TR
    atr = true_range.ewm(span=window, adjust=False).mean()

    # بازگرداندن سری ATR با ایندکس اصلی
    return atr.reindex(high.index)


def get_symbol_id(input_param):
    """
    تبدیل نام نماد (نام کوتاه فارسی) به symbol_id (که همان نام کوتاه فارسی است)،
    یا تلاش برای یافتن نام کوتاه فارسی در صورت ارائه ISIN.
    symbol_id (نام کوتاه فارسی) را بازمی‌گرداند یا None اگر یافت نشد.
    """
    # ایمپورت در داخل تابع برای جلوگیری از وابستگی چرخشی با models، در صورتی که models هم utils را ایمپورت کند.
    from models import ComprehensiveSymbolData, db

    if input_param is None:
        return None

    # ایجاد یک سشن جدید برای کوئری گرفتن
    # این کار برای اطمینان از اینکه تابع می‌تواند به دیتابیس دسترسی پیدا کند، لازم است
    # مگر اینکه این تابع در یک context اپلیکیشن Flask فراخوانی شود که سشن از قبل موجود است.
    # در این حالت، ما یک سشن موقت ایجاد می‌کنیم.
    session = db.session if hasattr(db, 'session') else None
    if session is None:
        # اگر db.session در دسترس نیست (مثلاً در یک اسکریپت standalone)، یک سشن جدید ایجاد کنید.
        # این نیاز به دسترسی به engine دیتابیس دارد که باید از جایی تنظیم شود.
        # برای جلوگیری از پیچیدگی در این اسکریپت، فرض می‌کنیم در context Flask فراخوانی می‌شود.
        logger.error("db.session در دسترس نیست. نمی‌توان symbol_id را واکشی کرد. مطمئن شوید در context Flask هستید یا db.session به درستی مقداردهی شده است.")
        return None

    try:
        # ابتدا، تلاش برای یافتن با symbol_name (که در دیتابیس شما همان symbol_id است)
        # استفاده از func.lower برای جستجوی case-insensitive
        symbol_data = session.query(ComprehensiveSymbolData).filter(
            func.lower(ComprehensiveSymbolData.symbol_name) == func.lower(input_param)
        ).first()
        if symbol_data:
            return symbol_data.symbol_id # بازگرداندن symbol_id واقعی (نام کوتاه فارسی)

        # اگر با symbol_name یافت نشد، تلاش با ISIN (اگر ورودی شبیه ISIN باشد)
        if isinstance(input_param, str) and input_param.startswith('IRO1'): # پیشوند رایج ISIN برای سهام ایران
            symbol_data = session.query(ComprehensiveSymbolData).filter_by(isin=input_param).first()
            if symbol_data:
                return symbol_data.symbol_id

        # اگر هنوز یافت نشد، تلاش با company_name
        symbol_data = session.query(ComprehensiveSymbolData).filter(
            func.lower(ComprehensiveSymbolData.company_name) == func.lower(input_param)
        ).first()
        if symbol_data:
            return symbol_data.symbol_id

    except Exception as e:
        logger.error(f"خطا در واکشی symbol_id برای '{input_param}': {e}")
    finally:
        # اگر سشن موقت ایجاد کرده‌اید، باید آن را ببندید.
        # در اینجا فرض می‌کنیم از db.session استفاده می‌کنیم که توسط Flask مدیریت می‌شود.
        pass

    logger.warning(f"symbol_id برای ورودی '{input_param}' یافت نشد.")
    return None

# --- توابع اضافه شده برای سرویس Weekly Watchlist ---

def calculate_smart_money_flow(df):
    """
    محاسبه معیارهای جریان پول هوشمند از داده‌های تاریخی.
    فرض می‌کند df شامل ستون‌های 'buy_i_volume', 'sell_i_volume', 'buy_count_i', 'sell_count_i', 'value' است.

    DataFrameای حاوی معیارهای محاسبه شده را بازمی‌گرداند.
    """
    logger.debug("در حال محاسبه جریان پول هوشمند.")

    # ستون‌های مورد نیاز برای محاسبه (بر اساس مدل HistoricalData شما)
    required_cols = [
        'buy_i_volume', 'sell_i_volume', 'buy_count_i',
        'sell_count_i', 'value'
    ]

    # بررسی وجود تمام ستون‌های مورد نیاز در DataFrame
    missing_columns = [col for col in required_cols if col not in df.columns]
    if missing_columns:
        logger.warning(f"ستون‌های مورد نیاز برای محاسبه جریان پول هوشمند یافت نشدند: {missing_columns}. محاسبات ممکن است ناقص باشند.")
        # اضافه کردن ستون‌های گم شده با NaN برای جلوگیری از KeyError
        for col in missing_columns:
            df[col] = np.nan
        # اگر بعد از این کار هم DataFrame خالی شد یا داده‌ای برای محاسبات نبود، یک DataFrame خالی برمی‌گردانیم.
        if df.empty or not all(col in df.columns for col in required_cols):
            return pd.DataFrame()

    df_copy = df.copy() # کار روی یک کپی برای جلوگیری از SettingWithCopyWarning

    # اطمینان از نوع عددی و پر کردن NaNها با 0 برای ستون‌های مرتبط
    for col in required_cols:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    # قدرت خرید حقیقی (Individual Buy Power)
    # نسبت حجم خرید حقیقی به حجم فروش حقیقی
    df_copy['individual_buy_power'] = df_copy['buy_i_volume'] / df_copy['sell_i_volume'].replace(0, np.nan)
    df_copy['individual_buy_power'] = df_copy['individual_buy_power'].replace([np.inf, -np.inf], np.nan).fillna(0)

    # خالص ورود/خروج پول حقیقی (Individual Net Flow)
    # تفاوت بین حجم خرید حقیقی و حجم فروش حقیقی
    df_copy['individual_net_flow'] = df_copy['buy_i_volume'] - df_copy['sell_i_volume']

    # سرانه خرید/فروش حقیقی (Individual Buy/Sell per Trade)
    df_copy['individual_buy_per_trade'] = df_copy['buy_i_volume'] / df_copy['buy_count_i'].replace(0, np.nan)
    df_copy['individual_sell_per_trade'] = df_copy['sell_i_volume'] / df_copy['sell_count_i'].replace(0, np.nan)
    df_copy['individual_buy_per_trade'] = df_copy['individual_buy_per_trade'].replace([np.inf, -np.inf], np.nan).fillna(0)
    df_copy['individual_sell_per_trade'] = df_copy['individual_sell_per_trade'].replace([np.inf, -np.inf], np.nan).fillna(0)

    # بازگرداندن ستون‌های محاسبه شده.
    # فرض می‌شود 'jdate' در DataFrame ورودی موجود است.
    if 'jdate' in df_copy.columns:
        return df_copy[['jdate', 'individual_buy_power', 'individual_net_flow', 'individual_buy_per_trade', 'individual_sell_per_trade']].copy()
    else:
        logger.warning("ستون 'jdate' در DataFrame برای خروجی جریان پول هوشمند یافت نشد. بدون 'jdate' بازگردانده می‌شود.")
        return df_copy[['individual_buy_power', 'individual_net_flow', 'individual_buy_per_trade', 'individual_sell_per_trade']].copy()


def check_candlestick_patterns(today_candle_data, yesterday_candle_data, close_prices_series):
    """
    بررسی الگوهای شمعی رایج مانند Hammer و Bullish Engulfing.
    Args:
        today_candle_data (dict): دیکشنری با 'open', 'high', 'low', 'close', 'volume' برای امروز.
        yesterday_candle_data (dict): دیکشنری با 'open', 'high', 'low', 'close', 'volume' برای دیروز.
        close_prices_series (numpy.ndarray or list): سری قیمت‌های بسته شدن تاریخی (مثلاً merged_df['close_hist'].values)
                                                     برای تشخیص روند استفاده می‌شود.
    Returns:
        list: لیستی از نام الگوهای شمعی شناسایی شده (مثلاً ["Hammer", "Bullish Engulfing"]).
    """
    detected_patterns = []

    # اطمینان از اعتبار داده‌ها
    if not all(k in today_candle_data and k in yesterday_candle_data for k in ['open', 'high', 'low', 'close']):
        logger.warning("داده‌های شمعی ناقص برای بررسی الگوهای شمعی.")
        return detected_patterns

    # استخراج داده‌های شمعی امروز
    open_t = today_candle_data['open']
    high_t = today_candle_data['high']
    low_t = today_candle_data['low']
    close_t = today_candle_data['close']
    volume_t = today_candle_data.get('volume', 0)

    # استخراج داده‌های شمعی دیروز
    open_y = yesterday_candle_data['open']
    high_y = yesterday_candle_data['high']
    low_y = yesterday_candle_data['low']
    close_y = yesterday_candle_data['close']

    # بررسی ساده برای روند نزولی (برای الگوهایی مانند Hammer در کف)
    is_in_downtrend = False
    if isinstance(close_prices_series, np.ndarray) and len(close_prices_series) >= 10:
        recent_closes = close_prices_series[-10:]
        if np.min(recent_closes) > 0: # جلوگیری از تقسیم بر صفر
            # بررسی اینکه آیا قیمت بسته شدن فعلی نزدیک به کمترین قیمت 10 روز اخیر است و روند کلی نزولی بوده.
            if close_t <= np.min(recent_closes) * 1.02 and recent_closes[0] > close_t:
                is_in_downtrend = True

    logger.debug(f"بررسی شمعی: امروز: O={open_t:.0f}, H={high_t:.0f}, L={low_t:.0f}, C={close_t:.0f}. دیروز: O={open_y:.0f}, H={high_y:.0f}, L={low_y:.0f}, C={close_y:.0f}. در روند نزولی: {is_in_downtrend}")

    # --- الگوی Hammer ---
    # بدنه کوچک، سایه پایینی بلند (حداقل 2 برابر بدنه)، سایه بالایی کوچک/بدون سایه
    body_t = abs(close_t - open_t)
    range_t = high_t - low_t
    if range_t > 0: # جلوگیری از تقسیم بر صفر
        lower_shadow_t = min(open_t, close_t) - low_t
        upper_shadow_t = high_t - max(open_t, close_t)

        if (body_t > 0 and 
            body_t < (0.3 * range_t) and # بدنه کوچک
            lower_shadow_t >= 2 * body_t and # سایه پایینی بلند
            upper_shadow_t < 0.1 * body_t and # سایه بالایی بسیار کوچک
            is_in_downtrend): # باید در انتهای یک روند نزولی باشد
            detected_patterns.append("Hammer")
            logger.debug("الگوی Hammer شناسایی شد.")

    # --- الگوی Bullish Engulfing ---
    # شمع دیروز نزولی، شمع امروز صعودی و بدنه شمع دیروز را کاملاً در بر می‌گیرد.
    if (close_y < open_y and # دیروز نزولی بود
        close_t > open_t and # امروز صعودی است
        open_t < close_y and # امروز پایین‌تر از بسته شدن دیروز باز می‌شود
        close_t > open_y): # امروز بالاتر از باز شدن دیروز بسته می‌شود
        detected_patterns.append("Bullish Engulfing")
        logger.debug("الگوی Bullish Engulfing شناسایی شد.")

    # می‌توانید الگوهای بیشتری را در اینجا اضافه کنید (مانند Morning Star, Piercing Line و غیره).

    return detected_patterns


def check_tsetmc_filters(symbol_id, jdate_str):
    """
    تابع Placeholder برای بررسی نتایج فیلترهای TSETMC.
    نیاز به کوئری گرفتن از مدل TSETMCFilterResult دارد.

    Args:
        symbol_id (str): ID نماد.
        jdate_str (str): رشته تاریخ جلالی (YYYY-MM-DD).

    Returns:
        tuple: (لیست نام فیلترهای راضی شده، لیست دلایل)
    """
    satisfied_filters = []
    reasons = []

    # مثال: کوئری گرفتن از مدل TSETMCFilterResult برای فیلترهای این نماد و تاریخ
    # from models import TSETMCFilterResult # در صورت نیاز به جلوگیری از وابستگی چرخشی، در داخل تابع ایمپورت کنید
    # filters_found = TSETMCFilterResult.query.filter_by(symbol_id=symbol_id, jdate=jdate_str).all()
    # for f in filters_found:
    #     satisfied_filters.append(f.filter_name)
    #     reasons.append(f"فیلتر TSETMC '{f.filter_name}' راضی شد.")

    # در حال حاضر، لیست‌های خالی را بازمی‌گردانیم زیرا این یک Placeholder است.
    return satisfied_filters, reasons

def check_financial_ratios(symbol_id):
    """
    تابع Placeholder برای بررسی نسبت‌های مالی.
    نیاز به کوئری گرفتن از مدل FinancialRatiosData دارد.

    Args:
        symbol_id (str): ID نماد.

    Returns:
        tuple: (لیست معیارهای نسبت راضی شده، لیست دلایل)
    """
    satisfied_ratios = []
    reasons = []

    # مثال: کوئری گرفتن از مدل FinancialRatiosData برای نسبت‌های مرتبط
    # from models import FinancialRatiosData # در صورت نیاز به جلوگیری از وابستگی چرخشی، در داخل تابع ایمپورت کنید
    # latest_ratios = FinancialRatiosData.query.filter_by(symbol_id=symbol_id)\
    #                                  .order_by(FinancialRatiosData.fiscal_year.desc()).all()
    # if latest_ratios:
    #     for ratio in latest_ratios:
    #         if ratio.ratio_name == 'DebtToEquity' and ratio.ratio_value < 0.5:
    #             satisfied_ratios.append("Low_Debt_To_Equity")
    #             reasons.append(f"نسبت بدهی به حقوق صاحبان سهام پایین است ({ratio.ratio_value:.2f}).")

    # در حال حاضر، لیست‌های خالی را بازمی‌گردانیم زیرا این یک Placeholder است.
    return satisfied_ratios, reasons