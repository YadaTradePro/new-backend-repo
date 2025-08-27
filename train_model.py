# -*- coding: utf-8 -*-
# train_model.py - اسکریپت آموزش و ارزیابی مدل یادگیری ماشین برای پیش‌بینی روند سهام

import pandas as pd
import numpy as np
import jdatetime
from datetime import datetime, timedelta, date
import logging
import os
import joblib 
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import StandardScaler 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import Config

# --- تنظیمات لاگ‌نویسی ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- تنظیمات مسیردهی پروژه ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(PROJECT_ROOT, 'models')
if not os.path.exists(MODELS_DIR):
    os.makedirs(MODELS_DIR)

# اضافه کردن مسیر 'services' به sys.path برای ایمپورت توابع کمکی
import sys
SERVICES_PATH = os.path.join(PROJECT_ROOT, 'services')
if SERVICES_PATH not in sys.path:
    sys.path.insert(0, SERVICES_PATH)

# ایمپورت مدل‌ها و توابع کمکی
try:
    from models import HistoricalData, ComprehensiveSymbolData 
    from utils import calculate_rsi, calculate_macd, calculate_sma, calculate_volume_ma, calculate_atr
except ImportError as e:
    logger.error(f"خطا در ایمپورت ماژول‌ها: {e}")
    logger.error("لطفاً مطمئن شوید models.py و services/utils.py در مسیرهای صحیح قرار دارند.")
    sys.exit(1)

# --- تنظیمات دیتابیس ---
DATABASE_URL = f"sqlite:///{os.path.join(PROJECT_ROOT, 'app.db')}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# --- تابع مهندسی ویژگی ---
def _perform_feature_engineering(df_symbol_hist, symbol_id_for_logging="N/A"):
    """
    انجام مهندسی ویژگی بر روی داده‌های تاریخی یک نماد.
    این تابع داده‌های تاریخی را به ویژگی‌های قابل استفاده برای مدل ML تبدیل می‌کند.
    """
    # اطمینان از مرتب بودن داده‌ها بر اساس تاریخ و ایجاد کپی صریح
    df_processed = df_symbol_hist.sort_values(by='gregorian_date').set_index('gregorian_date').copy()

    initial_rows = len(df_processed)

    # --- محاسبه شاخص‌های تکنیکال ---
    df_processed.loc[:, 'rsi'] = calculate_rsi(df_processed['close'])
    macd_line, signal_line, _ = calculate_macd(df_processed['close'])
    df_processed.loc[:, 'macd'] = macd_line
    df_processed.loc[:, 'signal_line'] = signal_line
    df_processed.loc[:, 'sma_20'] = calculate_sma(df_processed['close'], window=20)
    df_processed.loc[:, 'sma_50'] = calculate_sma(df_processed['close'], window=50)
    df_processed.loc[:, 'volume_ma_5_day'] = calculate_volume_ma(df_processed['volume'], window=5)
    df_processed.loc[:, 'atr'] = calculate_atr(df_processed['high'], df_processed['low'], df_processed['close'])

    # --- ویژگی‌های جدید اضافه شده ---
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
    
    rows_after_fillna = len(df_processed)
    if rows_after_fillna < initial_rows:
        logger.debug(f"برای نماد {symbol_id_for_logging}: {initial_rows - rows_after_fillna} ردیف به دلیل NaN/Inf پس از مهندسی ویژگی حذف شد.")
    
    # --- انتخاب ویژگی‌های نهایی برای مدل ---
    feature_columns = [
        'open', 'high', 'low', 'close', 'volume', 'num_trades',
        'rsi', 'macd', 'signal_line', 'sma_20', 'sma_50', 'volume_ma_5_day', 'atr',
        '%K', '%D', 'obv',
        'price_change_1d', 'volume_change_1d',
        'price_change_3d', 'volume_change_3d',
        'price_change_5d', 'volume_change_5d',
        'individual_buy_power_ratio',
        'buy_count_i', 'sell_count_i', 'buy_i_volume', 'sell_i_volume', 
        'zd1', 'qd1', 'pd1', 'zo1', 'qo1', 'po1',
        'zd2', 'qd2', 'pd2', 'zo2', 'qo2', 'po2',
        'zd3', 'qd3', 'pd3', 'zo3', 'qo3', 'po3',
        'zd4', 'qd4', 'pd4', 'zo4', 'qo4', 'po4',
        'zd5', 'qd5', 'pd5', 'zo5', 'qo5', 'po5'
    ]
    
    features_df = df_processed[feature_columns].copy() 
    
    if features_df.isnull().values.any() or np.isinf(features_df.values).any():
        logger.warning(f"برای نماد {symbol_id_for_logging}: مقادیر NaN یا Inf پس از پاکسازی نهایی در ویژگی‌ها باقی مانده‌اند. این ممکن است نشان‌دهنده مشکل در داده‌های خام یا منطق مهندسی ویژگی باشد.")
        features_df.fillna(0, inplace=True)
        features_df.replace([np.inf, -np.inf], 0, inplace=True) 

    return features_df

# --- تابع اصلی ---
def train_model():
    logger.info("در حال اتصال به دیتابیس و بارگذاری داده‌ها به صورت دسته‌ای (chunked)...")
    
    CHUNK_SIZE = 10000
    all_chunks_df = []

    try:
        query = "SELECT * FROM stock_data ORDER BY symbol_id, date"
        for chunk_df in pd.read_sql_query(query, engine, chunksize=CHUNK_SIZE):
            logger.info(f"دسته جدید با {len(chunk_df)} ردیف بارگذاری شد.")
            all_chunks_df.append(chunk_df)

        if not all_chunks_df:
            logger.error("داده تاریخی در دیتابیس یافت نشد. لطفاً ابتدا داده‌ها را جمع‌آوری کنید.")
            return

        logger.info(f"تمام دسته‌ها با موفقیت بارگذاری شدند. در حال ادغام و پیش‌پردازش...")
        df_hist = pd.concat(all_chunks_df, ignore_index=True)
        
        logger.info(f"تعداد کل نقاط داده تاریخی پس از واکشی: {len(df_hist)}")

        df_hist['gregorian_date'] = pd.to_datetime(df_hist['date'])
        
        df_hist.drop(columns=['_sa_instance_state'], errors='ignore', inplace=True)
        numeric_cols = ['open', 'high', 'low', 'close', 'final', 'yesterday_price', 'volume', 'value', 'num_trades',
                         'plc', 'plp', 'pcc', 'pcp', 'mv',
                         'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n',
                         'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume',
                         'zd1', 'qd1', 'pd1', 'zo1', 'qo1', 'po1',
                         'zd2', 'qd2', 'pd2', 'zo2', 'qo2', 'po2',
                         'zd3', 'qd3', 'pd3', 'zo3', 'qo3', 'po3',
                         'zd4', 'qd4', 'pd4', 'zo4', 'qo4', 'po4',
                         'zd5', 'qd5', 'pd5', 'zo5', 'qo5', 'po5']
        for col in numeric_cols:
            if col in df_hist.columns:
                df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')
        
        initial_rows_count = len(df_hist)
        df_hist.dropna(subset=['close', 'volume', 'high', 'low', 'open'], inplace=True)
        logger.info(f"تعداد نقاط داده پس از پیش‌پردازش اولیه و حذف NaNهای اساسی: {len(df_hist)} (حذف شده: {initial_rows_count - len(df_hist)})")

        logger.info("در حال شروع مهندسی ویژگی‌ها...")
        all_features_df = pd.DataFrame()
        skipped_symbols = []

        for symbol_id in df_hist['symbol_id'].unique():
            df_symbol = df_hist[df_hist['symbol_id'] == symbol_id].copy()
            
            if len(df_symbol) < 60:
                skipped_symbols.append(f"{symbol_id}: داده کافی ({len(df_symbol)} روز) برای محاسبه ویژگی‌ها وجود ندارد.")
                continue

            features_df = _perform_feature_engineering(df_symbol, symbol_id_for_logging=symbol_id)
            
            if features_df.empty:
                skipped_symbols.append(f"{symbol_id}: پس از مهندسی ویژگی، هیچ داده معتبری باقی نماند.")
                continue

            features_df['symbol_id'] = symbol_id
            features_df['jdate'] = df_symbol.set_index('gregorian_date').loc[features_df.index, 'jdate']
            features_df['close_hist'] = df_symbol.set_index('gregorian_date').loc[features_df.index, 'close'] 

            all_features_df = pd.concat([all_features_df, features_df], ignore_index=False)
        
        if skipped_symbols:
            logger.warning("نمادهای زیر به دلیل داده ناکافی یا نامعتبر پرش شدند:")
            for msg in skipped_symbols:
                logger.warning(f"پرش از نماد {msg}")

        logger.info(f"تعداد کل نقاط داده پس از مهندسی ویژگی و حذف NaN/Inf: {len(all_features_df)}")

        if all_features_df.empty:
            logger.error("پس از مهندسی ویژگی، هیچ داده‌ای برای آموزش مدل باقی نماند. فرآیند آموزش متوقف شد.")
            return

        logger.info("در حال تعریف برچسب‌ها (متغیر هدف)...")
        all_features_df.sort_values(by=['symbol_id', 'gregorian_date'], inplace=True)
        
        all_features_df['future_close'] = all_features_df.groupby('symbol_id')['close_hist'].shift(-7)
        all_features_df['percentage_change'] = ((all_features_df['future_close'] - all_features_df['close_hist']) / all_features_df['close_hist']) * 100
        
        initial_rows_after_features = len(all_features_df)
        all_features_df.dropna(subset=['percentage_change'], inplace=True)
        logger.info(f"تعداد کل نقاط داده آموزشی پس از تعریف برچسب: {len(all_features_df)} (حذف شده: {initial_rows_after_features - len(all_features_df)})")

        if all_features_df.empty:
            logger.error("پس از تعریف برچسب‌ها، هیچ داده‌ای برای آموزش مدل باقی نماند. فرآیند آموزش متوقف شد.")
            return

        lower_bound = all_features_df['percentage_change'].quantile(0.33)
        upper_bound = all_features_df['percentage_change'].quantile(0.66)

        logger.info(f"آستانه نزولی (Quantile 33%): {lower_bound:.2f}%")
        logger.info(f"آستانه صعودی (Quantile 66%): {upper_bound:.2f}%")

        def get_trend(change):
            if change > upper_bound:
                return 'Uptrend'
            elif change < lower_bound:
                return 'Downtrend'
            else:
                return 'Sideways'

        all_features_df['trend'] = all_features_df['percentage_change'].apply(get_trend)

        logger.info("توزیع کلاس‌ها در داده‌های آموزشی (پس از برچسب‌گذاری با کوانتایل):")
        logger.info(all_features_df['trend'].value_counts(normalize=True))

        X = all_features_df.drop(columns=['symbol_id', 'jdate', 'close_hist', 'future_close', 'percentage_change', 'trend'])
        y = all_features_df['trend']

        logger.info("در حال شروع آموزش مدل ML با اعتبارسنجی Walk-Forward...")

        initial_train_window_days = 252 
        test_window_days = 21 
        step_window_days = 21 

        fold_reports = []
        fold_accuracies = []
        
        X.index = all_features_df.index 
        y.index = all_features_df.index

        unique_dates = X.index.unique().sort_values()
        
        if len(unique_dates) < initial_train_window_days + test_window_days:
            logger.warning(f"داده تاریخی کافی برای اجرای اعتبارسنجی Walk-Forward وجود ندارد. حداقل {initial_train_window_days + test_window_days} روز منحصر به فرد نیاز است. یافت شده: {len(unique_dates)} روز.")
            logger.info("آموزش مدل نهایی روی کل داده‌های موجود (بدون Walk-Forward) به دلیل داده ناکافی برای اعتبارسنجی.")
            
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            X_scaled_df = pd.DataFrame(X_scaled, columns=X.columns, index=X.index)

            model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
            model.fit(X_scaled_df, y)
            
            y_pred = model.predict(X_scaled_df)
            report = classification_report(y, y_pred, output_dict=True, zero_division=0)
            accuracy = accuracy_score(y, y_pred)
            
            logger.info("\nگزارش ارزیابی مدل نهایی (روی کل داده‌های موجود):")
            logger.info(pd.DataFrame(report).transpose())
            logger.info(f"دقت کلی: {accuracy:.2f}")

            final_model = model
            final_scaler = scaler

        else:
            start_idx_for_test_window = initial_train_window_days
            
            while start_idx_for_test_window + test_window_days <= len(unique_dates):
                train_end_date = unique_dates[start_idx_for_test_window - 1]
                test_start_date = unique_dates[start_idx_for_test_window]
                test_end_date = unique_dates[min(start_idx_for_test_window + test_window_days - 1, len(unique_dates) - 1)]

                X_train_fold = X.loc[X.index <= train_end_date].copy()
                y_train_fold = y.loc[y.index <= train_end_date].copy()
                X_test_fold = X.loc[(X.index >= test_start_date) & (X.index <= test_end_date)].copy()
                y_test_fold = y.loc[(y.index >= test_start_date) & (y.index <= test_end_date)].copy()

                X_train_fold.fillna(0, inplace=True) 
                y_train_fold = y_train_fold.loc[X_train_fold.index] 

                X_test_fold.fillna(0, inplace=True) 
                y_test_fold = y_test_fold.loc[X_test_fold.index] 

                if X_train_fold.empty or X_test_fold.empty:
                    logger.warning(f"فولد با تاریخ آموزش تا {train_end_date} و تست از {test_start_date} تا {test_end_date} به دلیل داده خالی پرش شد.")
                    start_idx_for_test_window += step_window_days
                    continue

                scaler = StandardScaler()
                X_train_scaled = scaler.fit_transform(X_train_fold)
                X_test_scaled = scaler.transform(X_test_fold)

                X_train_scaled_df = pd.DataFrame(X_train_scaled, columns=X_train_fold.columns, index=X_train_fold.index)
                X_test_scaled_df = pd.DataFrame(X_test_scaled, columns=X_test_fold.columns, index=X_test_fold.index)

                model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
                model.fit(X_train_scaled_df, y_train_fold)

                y_pred_fold = model.predict(X_test_scaled_df)
                report = classification_report(y_test_fold, y_pred_fold, output_dict=True, zero_division=0)
                fold_reports.append(report)
                fold_accuracies.append(accuracy_score(y_test_fold, y_pred_fold))
                
                logger.info(f"فولد کامل شد: آموزش تا {train_end_date}, تست از {test_start_date} تا {test_end_date}. دقت: {accuracy_score(y_test_fold, y_pred_fold):.2f}")

                start_idx_for_test_window += step_window_days

            if not fold_reports:
                logger.error("هیچ فولدی برای ارزیابی Walk-Forward کامل نشد. فرآیند آموزش متوقف شد.")
                return

            avg_precision = {cls: np.mean([f['precision'][cls] for f in fold_reports]) for cls in ['Uptrend', 'Downtrend', 'Sideways']}
            avg_recall = {cls: np.mean([f['recall'][cls] for f in fold_reports]) for cls in ['Uptrend', 'Downtrend', 'Sideways']}
            avg_f1_score = {cls: np.mean([f['f1-score'][cls] for f in fold_reports]) for cls in ['Uptrend', 'Downtrend', 'Sideways']}
            avg_accuracy = np.mean(fold_accuracies)

            logger.info("\nمیانگین گزارش ارزیابی (اعتبارسنجی Walk-Forward):")
            avg_report_df = pd.DataFrame({
                'precision': avg_precision,
                'recall': avg_recall,
                'f1-score': avg_f1_score
            }).transpose()
            logger.info(avg_report_df)
            logger.info(f"میانگین دقت: {avg_accuracy:.2f}")

            final_model = model 
            final_scaler = scaler 

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_save_path = os.path.join(MODELS_DIR, f'trained_ml_model_{timestamp}.pkl')
        feature_names_save_path = os.path.join(MODELS_DIR, f'feature_names_{timestamp}.pkl')
        class_labels_map_save_path = os.path.join(MODELS_DIR, f'class_labels_map_{timestamp}.pkl')
        scaler_save_path = os.path.join(MODELS_DIR, f'scaler_{timestamp}.pkl') 

        joblib.dump(final_model, model_save_path)
        joblib.dump(X.columns.tolist(), feature_names_save_path) 
        joblib.dump(final_model.classes_.tolist(), class_labels_map_save_path) 
        joblib.dump(final_scaler, scaler_save_path) 

        logger.info(f"مدل در مسیر {model_save_path} ذخیره شد.")
        logger.info(f"نام ویژگی‌ها در مسیر {feature_names_save_path} ذخیره شد.")
        logger.info(f"نگاشت برچسب‌های کلاس در مسیر {class_labels_map_save_path} ذخیره شد.")
        logger.info(f"Scaler در مسیر {scaler_save_path} ذخیره شد.")

        snapshot_path = os.path.join(PROJECT_ROOT, f'training_dataset_snapshot_{timestamp}.csv')
        all_features_df.to_csv(snapshot_path, index=True)
        logger.info(f"Snapshot داده‌های آموزشی در {snapshot_path} ذخیره شد.")

        logger.info("فرآیند آموزش و ذخیره‌سازی مدل ML با موفقیت کامل شد!")

    except Exception as e:
        logger.error(f"خطای کلی در فرآیند آموزش مدل: {e}", exc_info=True)
    finally:
        if 'session' in locals() and session:
            session.close()

if __name__ == "__main__":
    train_model()