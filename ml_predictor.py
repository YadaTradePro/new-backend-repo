# -*- coding: utf-8 -*-
# ml_predictor.py - ماژول برای بارگذاری مدل ML و انجام پیش‌بینی‌ها

import pandas as pd
import numpy as np
import joblib
import os
import sys
from datetime import datetime, timedelta
import logging
from sklearn.preprocessing import StandardScaler

# ایمپورت کلاس Config از فایل config.py در پوشه 'backend'
# فرض می کنیم این فایل در 'services' یا 'backend' است
from config import Config

logger = logging.getLogger(__name__)

# --- ایمپورت توابع کمکی ---
try:
    # این ایمپورت به ساختار 'backend/services/utils.py' اشاره می‌کند
    from services.utils import calculate_rsi, calculate_macd, calculate_sma, calculate_volume_ma, calculate_atr
except ImportError as e:
    logger.error(f"خطا: توابع کمکی از utils.py ایمپورت نشدند. {e}")
    logger.error("لطفا مطمئن شوید utils.py وجود دارد و شامل این توابع است.")
    sys.exit(1)

# --- تابع برای پیدا کردن فایل‌های مدل ---
def find_latest_model_files(model_dir):
    """
    آخرین فایل‌های مدل، ویژگی‌ها، نگاشت کلاس و Scaler را بر اساس timestamp پیدا می‌کند.
    """
    timestamps = []

    # اطمینان از وجود مسیر قبل از لیست کردن محتویات آن
    if not os.path.exists(model_dir):
        logger.error(f"خطا: پوشه مدل‌ها در مسیر {model_dir} یافت نشد.")
        return None, None, None, None

    for f in os.listdir(model_dir):
        if f.startswith('trained_ml_model_') and f.endswith('.pkl'):
            ts_str = f[len('trained_ml_model_'):-len('.pkl')]
            try:
                timestamps.append(datetime.strptime(ts_str, "%Y%m%d_%H%M%S"))
            except ValueError:
                continue

    if not timestamps:
        logger.warning(f"هیچ فایل مدل آموزش‌دیده در {model_dir} یافت نشد.")
        return None, None, None, None

    latest_timestamp = max(timestamps)
    latest_ts_str = latest_timestamp.strftime("%Y%m%d_%H%M%S")

    model_path = os.path.join(model_dir, f'trained_ml_model_{latest_ts_str}.pkl')
    feature_names_path = os.path.join(model_dir, f'feature_names_{latest_ts_str}.pkl')
    class_labels_map_path = os.path.join(model_dir, f'class_labels_map_{latest_ts_str}.pkl')
    scaler_path = os.path.join(model_dir, f'scaler_{latest_ts_str}.pkl')

    if os.path.exists(model_path) and os.path.exists(feature_names_path) and \
       os.path.exists(class_labels_map_path) and os.path.exists(scaler_path):
        return model_path, feature_names_path, class_labels_map_path, scaler_path
    else:
        logger.warning(f"فایل‌های مدل کامل (شامل Scaler) برای آخرین timestamp {latest_ts_str} یافت نشدند.")
        return None, None, None, None

# --- استفاده از مسیر تعریف‌شده در Config ---
MODEL_DIR = Config.MODEL_DIR

# بارگذاری فایل‌های مدل
LATEST_MODEL_PATH, LATEST_FEATURE_NAMES_PATH, LATEST_CLASS_LABELS_MAP_PATH, LATEST_SCALER_PATH = find_latest_model_files(MODEL_DIR)

if not (LATEST_MODEL_PATH and LATEST_FEATURE_NAMES_PATH and LATEST_CLASS_LABELS_MAP_PATH and LATEST_SCALER_PATH):
    logger.error(f"خطا: مدل آموزش‌دیده کامل (شامل Scaler) در مسیر {MODEL_DIR} یافت نشد. لطفاً ابتدا train_model.py را اجرا کنید.")
    raise FileNotFoundError(f"مدل آموزش‌دیده کامل در مسیر {MODEL_DIR} یافت نشد.")

try:
    _model = joblib.load(LATEST_MODEL_PATH)
    _feature_names = joblib.load(LATEST_FEATURE_NAMES_PATH)
    _class_labels_map = joblib.load(LATEST_CLASS_LABELS_MAP_PATH)
    _scaler = joblib.load(LATEST_SCALER_PATH)
    logger.info("مدل ML، نام ویژگی‌ها، نگاشت کلاس و Scaler با موفقیت بارگذاری شدند.")
except Exception as e:
    logger.error(f"خطا در بارگذاری مدل ML یا فایل‌های مرتبط: {e}", exc_info=True)
    raise RuntimeError(f"خطا در بارگذاری مدل ML: {e}")

# --- تابع مهندسی ویژگی برای داده‌های جدید (باید با train_model.py یکسان باشد) ---
def _perform_feature_engineering_for_prediction(df_symbol_hist, symbol_id_for_logging="N/A"):
    """
    انجام مهندسی ویژگی بر روی داده‌های تاریخی یک نماد برای پیش‌بینی.
    این تابع باید دقیقاً با منطق feature engineering در train_model.py مطابقت داشته باشد.
    """
    # اطمینان از مرتب بودن داده‌ها بر اساس تاریخ و ایجاد کپی صریح
    df_processed = df_symbol_hist.sort_values(by='gregorian_date').set_index('gregorian_date').copy()

    # --- محاسبه شاخص‌های تکنیکال ---
    df_processed.loc[:, 'rsi'] = calculate_rsi(df_processed['close'])
    macd_line, signal_line, _ = calculate_macd(df_processed['close'])
    df_processed.loc[:, 'macd'] = macd_line
    df_processed.loc[:, 'signal_line'] = signal_line
    df_processed.loc[:, 'sma_20'] = calculate_sma(df_processed['close'], window=20)
    df_processed.loc[:, 'sma_50'] = calculate_sma(df_processed['close'], window=50)
    df_processed.loc[:, 'volume_ma_5_day'] = calculate_volume_ma(df_processed['volume'], window=5)
    df_processed.loc[:, 'atr'] = calculate_atr(df_processed['high'], df_processed['low'], df_processed['close'])

    # --- ویژگی‌های جدید اضافه شده (باید با train_model.py یکسان باشد) ---
    # Stochastic Oscillator
    window_stoch = 14
    df_processed.loc[:, 'lowest_low_stoch'] = df_processed['low'].rolling(window=window_stoch).min()
    df_processed.loc[:, 'highest_high_stoch'] = df_processed['high'].rolling(window=window_stoch).max()
    denominator_stoch = df_processed['highest_high_stoch'] - df_processed['lowest_low_stoch']
    df_processed.loc[:, '%K'] = ((df_processed['close'] - df_processed['lowest_low_stoch']) / denominator_stoch.replace(0, np.nan)) * 100
    df_processed.loc[:, '%D'] = df_processed['%K'].rolling(window=3).mean()

    # On-Balance Volume (OBV)
    close_shifted = df_processed['close'].shift(1)
    volume_numeric = pd.to_numeric(df_processed['volume'], errors='coerce').fillna(0)
    df_processed.loc[:, 'obv'] = (np.where(df_processed['close'] > close_shifted, volume_numeric,
                                         np.where(df_processed['close'] < close_shifted, -volume_numeric, 0))).cumsum()

    # ویژگی‌های لگ (Lagged Features) برای تغییرات قیمت و حجم
    df_processed.loc[:, 'price_change_1d'] = df_processed['close'].pct_change()
    df_processed.loc[:, 'volume_change_1d'] = df_processed['volume'].pct_change()
    df_processed.loc[:, 'price_change_3d'] = df_processed['close'].pct_change(periods=3)
    df_processed.loc[:, 'volume_change_3d'] = df_processed['volume'].pct_change(periods=3)
    df_processed.loc[:, 'price_change_5d'] = df_processed['close'].pct_change(periods=5)
    df_processed.loc[:, 'volume_change_5d'] = df_processed['volume'].pct_change(periods=5)

    # نسبت قدرت خریدار حقیقی (Real Buyer Power Ratio)
    buy_i_vol = pd.to_numeric(df_processed['buy_i_volume'], errors='coerce').fillna(0)
    sell_i_vol = pd.to_numeric(df_processed['sell_i_volume'], errors='coerce').fillna(0)
    buy_count_i = pd.to_numeric(df_processed['buy_count_i'], errors='coerce').fillna(0)
    sell_count_i = pd.to_numeric(df_processed['sell_count_i'], errors='coerce').fillna(0)

    denominator_buy_power = (sell_i_vol * sell_count_i)
    df_processed.loc[:, 'individual_buy_power_ratio'] = (buy_i_vol * buy_count_i) / denominator_buy_power.replace(0, np.nan)

    # --- مدیریت مقادیر گمشده و نامحدود (NaN/Inf) ---
    df_processed.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_processed = df_processed.ffill().bfill()
    df_processed.fillna(0, inplace=True)

    # انتخاب ویژگی‌های نهایی برای مدل (باید با _feature_names از مدل آموزش‌دیده مطابقت داشته باشد)
    # اطمینان از اینکه فقط ستون‌های موجود در _feature_names انتخاب می‌شوند و ترتیب آن‌ها صحیح است.
    available_features = [col for col in _feature_names if col in df_processed.columns]
    features_df = df_processed[available_features].copy()

    # اگر تعداد ویژگی‌های موجود با تعداد ویژگی‌های مورد انتظار مدل مطابقت ندارد،
    # ویژگی‌های گم شده را با 0 پر می‌کنیم و ترتیب را تنظیم می‌کنیم.
    if len(features_df.columns) != len(_feature_names):
        missing_features = set(_feature_names) - set(features_df.columns)
        logger.warning(f"برای نماد {symbol_id_for_logging}: ویژگی‌های مورد نیاز مدل ({missing_features}) در داده‌های ورودی یافت نشدند. این ممکن است بر دقت پیش‌بینی تأثیر بگذارد.")
        for feature in missing_features:
            features_df[feature] = 0 # پر کردن ویژگی‌های گم شده با 0
        features_df = features_df[_feature_names] # اطمینان از ترتیب صحیح ستون‌ها

    return features_df


# --- تابع اصلی پیش‌بینی ---
def predict_trend_for_symbol(historical_data_df, symbol_id_for_logging="N/A"):
    """
    پیش‌بینی روند برای یک نماد بر اساس داده‌های تاریخی آن.
    """
    if historical_data_df.empty or len(historical_data_df) < 60:
        logger.warning(f"داده تاریخی کافی برای نماد {symbol_id_for_logging} برای پیش‌بینی وجود ندارد (حداقل 60 روز نیاز است).")
        return None, None

    try:
        features_for_prediction = _perform_feature_engineering_for_prediction(historical_data_df.copy(), symbol_id_for_logging)

        if features_for_prediction.empty:
            logger.warning(f"برای نماد {symbol_id_for_logging}: پس از مهندسی ویژگی و پاکسازی، هیچ داده معتبری برای پیش‌بینی باقی نماند.")
            return None, None

        latest_features = features_for_prediction.iloc[[-1]]

        # --- اعمال Scaler ---
        latest_features_scaled = _scaler.transform(latest_features)

        probabilities = _model.predict_proba(latest_features_scaled)[0]

        predicted_class_idx = np.argmax(probabilities)
        predicted_probability = probabilities[predicted_class_idx]

        predicted_trend_label = _model.classes_[predicted_class_idx]

        return predicted_trend_label, predicted_probability

    except Exception as e:
        logger.error(f"خطا در هنگام پیش‌بینی روند برای نماد {symbol_id_for_logging}: {e}", exc_info=True)
        return None, None

# مثال استفاده (برای تست ماژول به صورت standalone):
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.info("تست ماژول ml_predictor.py")

    # برای تست، از یک دیتابیس موقت یا داده‌های ساختگی استفاده کنید.
    # این بخش نیاز دارد که مدل قبلاً توسط train_model.py آموزش داده شده باشد.

    # مثال با داده‌های ساختگی: (این بخش را از train_model.py کپی کنید تا ویژگی‌ها یکسان باشند)
    sample_data = {
        'gregorian_date': pd.to_datetime(pd.date_range(end=datetime.now(), periods=200, freq='D')),
        'open': np.random.rand(200) * 1000 + 1000,
        'high': np.random.rand(200) * 1000 + 1100,
        'low': np.random.rand(200) * 1000 + 900,
        'close': np.random.rand(200) * 1000 + 1050,
        'volume': np.random.rand(200) * 1000000 + 100000,
        'buy_i_volume': np.random.rand(200) * 500000 + 50000,
        'sell_i_volume': np.random.rand(200) * 500000 + 50000,
        'buy_count_i': np.random.randint(10, 100, 200),
        'sell_count_i': np.random.randint(10, 100, 200),
        'num_trades': np.random.randint(100, 1000, 200),
        'zd1': np.random.randint(1, 10, 200), 'qd1': np.random.rand(200) * 10000, 'pd1': np.random.rand(200) * 1000,
        'zo1': np.random.randint(1, 10, 200), 'qo1': np.random.rand(200) * 10000, 'po1': np.random.rand(200) * 1000,
        'zd2': np.random.randint(1, 10, 200), 'qd2': np.random.rand(200) * 10000, 'pd2': np.random.rand(200) * 1000,
        'zo2': np.random.randint(1, 10, 200), 'qo2': np.random.rand(200) * 10000, 'po2': np.random.rand(200) * 1000,
        'zd3': np.random.randint(1, 10, 200), 'qd3': np.random.rand(200) * 10000, 'pd3': np.random.rand(200) * 1000,
        'zo3': np.random.randint(1, 10, 200), 'qo3': np.random.rand(200) * 10000, 'po3': np.random.rand(200) * 1000,
        'zd4': np.random.randint(1, 10, 200), 'qd4': np.random.rand(200) * 10000, 'pd4': np.random.rand(200) * 1000,
        'zo4': np.random.randint(1, 10, 200), 'qo4': np.random.rand(200) * 10000, 'po4': np.random.rand(200) * 1000,
        'zd5': np.random.randint(1, 10, 200), 'qd5': np.random.rand(200) * 10000, 'pd5': np.random.rand(200) * 1000,
        'zo5': np.random.randint(1, 10, 200), 'qo5': np.random.rand(200) * 10000, 'po5': np.random.rand(200) * 1000,
        'plc': np.random.rand(200), 'plp': np.random.rand(200), 'pcc': np.random.rand(200), 'pcp': np.random.rand(200),
        'mv': np.random.rand(200) * 1e9, 'final': np.random.rand(200) * 1000 + 1050, 'yesterday_price': np.random.rand(200) * 1000 + 1050,
        'value': np.random.rand(200) * 1e9
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df['symbol_id'] = 'SAMPLE'
    sample_df['symbol_name'] = 'نماد نمونه'
    sample_df['jdate'] = sample_df['gregorian_date'].apply(lambda x: jdatetime.date.fromgregorian(year=x.year, month=x.month, day=x.day).strftime('%Y-%m-%d'))


    trend, prob = predict_trend_for_symbol(sample_df, symbol_id_for_logging='SAMPLE')
    if trend:
        logger.info(f"روند پیش‌بینی شده: {trend} با احتمال: {prob:.2f}")
    else:
        logger.info("پیش‌بینی انجام نشد.")