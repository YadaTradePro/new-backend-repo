# services/fetch_latest_brsapi_eod.py

import requests
import pandas as pd
from typing import List, Optional, Tuple
from datetime import date, timedelta # 💡 timedelta اضافه شد (مورد نیاز در توابع دیگر)
from sqlalchemy.orm import Session
from sqlalchemy import func
import jdatetime
import traceback
import numpy as np
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import or_
import time # 💡 time اضافه شد (مورد نیاز در توابع دیگر)

# فرض کنید اینها در جایی تعریف شده‌اند
from models import HistoricalData, ComprehensiveSymbolData 
from services.utils import logger, safe_sleep, DEFAULT_PER_SYMBOL_DELAY
# فرض کنید توابع زیر تعریف شده‌اند (چون در ادامه کد شما استفاده شده‌اند)
# از آنجا که توابع زیر تعریف نشده‌اند، اما در کد شما استفاده شده‌اند، 
# فقط برای جلوگیری از خطای NameError (در صورت اجرای کامل کد)، آن‌ها را به صورت موقت تعریف می‌کنیم.
def fetch_symbols_from_pytse_client(db_session, limit): return 0, ""
def fetch_and_process_historical_data(db_session, limit=None, specific_symbols_list=None): return 0, ""
def fetch_realtime_data_for_all_symbols(db_session): return 0
def fetch_realtime_snapshot(db_session, symbol_name, symbol_id): return True, ""
def update_symbol_fundamental_data(db_session, specific_symbols_list=None): return 0, ""
def run_technical_analysis(db_session, limit=None, symbols_list=None): return 0, ""
def run_candlestick_detection(db_session, limit=None, symbols_list=None): return 0
# --------------------------------------------------------------------------------


# **نکته:** برای اجرای این کد، نیاز به تنظیم کلید API واقعی و توابع کمکی دارید.
BRSAPI_ALL_SYMBOLS_URL = "https://BrsApi.ir/Api/Tsetmc/AllSymbols.php"
# 👈 توجه: حتماً این کلید را با کلید API واقعی خود جایگزین کنید تا خطای 403 رخ ندهد.
API_KEY = "BvhdYHBjqiyIQ7eTuQBKN17ZuLpHkQZ1" 


# --------------------------------------------------------------------------------
# 1. تابع فچ داده (بدون تغییر)
# --------------------------------------------------------------------------------
def fetch_latest_brsapi_eod() -> Optional[pd.DataFrame]:
    """
    فچ آخرین وضعیت معاملاتی (لحظه‌ای) تمام نمادها از وب‌سرویس BRSAPI.
    """
    url = f"https://brsapi.ir/Api/Tsetmc/AllSymbols.php?key=BvhdYHBjqiyIQ7eTuQBKN17ZuLpHkQZ1&type=1" 
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    logger.info("🌐 در حال فچ آخرین وضعیت نمادها از BRSAPI...")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() 
        
        data = response.json()
        
        if not data or isinstance(data, dict) and data.get('Error'):
            logger.error(f"❌ خطای API BRSAPI: {data}")
            return None

        df = pd.DataFrame(data)
        logger.info(f"✅ با موفقیت {len(df)} رکورد لحظه‌ای از BRSAPI دریافت شد.")
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ خطای درخواست از BRSAPI: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در فچ BRSAPI: {e}")
        return None

# --------------------------------------------------------------------------------
# 2. تابع اصلی آپدیت EOD (فقط گام ۵ تغییر کرده است)
# --------------------------------------------------------------------------------

def update_daily_eod_from_brsapi(db_session: Session) -> Tuple[int, str, List[int]]:
    """
    تابع کاملاً مستقل برای دریافت و ذخیره داده‌های کامل EOD روز جاری
    (شامل OHLCV، حقیقی/حقوقی و عمق بازار) از BRSAPI در HistoricalData.
    همچنین تاریخ آخرین EOD را در ComprehensiveSymbolData به‌روز می‌کند.
    """
    logger.info("⚡️ شروع به‌روزرسانی داده‌های EOD امروز از BRSAPI...")
    
    # 1. فچ داده‌های لحظه‌ای (EOD) تمام نمادها
    df_eod = fetch_latest_brsapi_eod()
    if df_eod is None or df_eod.empty:
        return 0, "❌ فچ داده‌های لحظه‌ای از BRSAPI ناموفق بود یا دیتایی دریافت نشد.", [] 

    # 2. آماده‌سازی DataFrame برای دیتابیس
    today_gregorian = date.today()
    df_eod['date'] = today_gregorian
    df_eod['jdate'] = jdatetime.date.fromgregorian(date=today_gregorian).strftime("%Y-%m-%d")
    
    # تغییر نام ستون‌ها
    column_mapping = {
        'l18': 'symbol_name', 'id': 'tse_index', 'pf': 'open', 'pmax': 'high', 'pmin': 'low', 
        'pl': 'close', 'pc': 'final', 'py': 'yesterday_price', 'plc': 'plc', 'plp': 'plp', 
        'pcc': 'pcc', 'pcp': 'pcp', 'tvol': 'volume', 'tval': 'value', 'tno': 'num_trades', 
        'mv': 'mv', 'Buy_CountI': 'buy_count_i', 'Buy_CountN': 'buy_count_n', 
        'Sell_CountI': 'sell_count_i', 'Sell_CountN': 'sell_count_n', 'Buy_I_Volume': 'buy_i_volume', 
        'Buy_N_Volume': 'buy_n_volume', 'Sell_I_Volume': 'sell_i_volume', 'Sell_N_Volume': 'sell_n_volume', 
        'zd1': 'zd1', 'qd1': 'qd1', 'pd1': 'pd1', 'zo1': 'zo1', 'qo1': 'qo1', 'po1': 'po1', 
        'zd2': 'zd2', 'qd2': 'qd2', 'pd2': 'pd2', 'zo2': 'zo2', 'qo2': 'qo2', 'po2': 'po2', 
        'zd3': 'zd3', 'qd3': 'qd3', 'pd3': 'pd3', 'zo3': 'zo3', 'qo3': 'qo3', 'po3': 'po3', 
        'zd4': 'zd4', 'qd4': 'qd4', 'pd4': 'pd4', 'zo4': 'zo4', 'qo4': 'po4', 'po4': 'po4', 
        'zd5': 'zd5', 'qd5': 'qd5', 'pd5': 'pd5', 'zo5': 'zo5', 'qo5': 'qo5', 'po5': 'po5',
    }
    
    df_eod.rename(columns=column_mapping, inplace=True)
    df_eod.replace([np.inf, -np.inf], None, inplace=True)
    
    # 3. یافتن Symbol ID داخلی (symbol_id) و فیلتر کردن نمادها
    try:
        tse_indices_from_brsapi = df_eod['tse_index'].astype(str).tolist()

        symbol_map = db_session.query(
            ComprehensiveSymbolData.tse_index, 
            ComprehensiveSymbolData.symbol_id,
            ComprehensiveSymbolData.symbol_name
        ).filter(ComprehensiveSymbolData.tse_index.in_(tse_indices_from_brsapi)).all()

        symbol_id_lookup = {str(tse): internal_id for tse, internal_id, _ in symbol_map}
        tse_index_lookup = {str(tse): tse for tse, internal_id, _ in symbol_map}

        df_eod['symbol_id'] = df_eod['tse_index'].astype(str).map(symbol_id_lookup)
        df_eod['tse_index'] = df_eod['tse_index'].astype(str).map(tse_index_lookup) # نگهداری tse_index موقت
        
        initial_count = len(df_eod)
        df_eod.dropna(subset=['symbol_id'], inplace=True)
        final_count = len(df_eod)
        
        if df_eod.empty:
            logger.warning(f"❌ از {initial_count} رکورد دریافتی، هیچ نمادی در ComprehensiveSymbolData مطابقت نداشت.")
            return 0, "❌ هیچ نمادی از BRSAPI با نمادهای موجود در ComprehensiveSymbolData مطابقت نداشت.", []
            
        logger.info(f"ℹ️ از {initial_count} رکورد دریافتی، {final_count} رکورد با ComprehensiveSymbolData مپ شدند.")
        
        df_eod['symbol_id'] = df_eod['symbol_id'].astype(int)

    except Exception as e:
        logger.error(f"❌ خطا در مپ کردن شناسه نمادها: {e}", exc_info=True)
        return 0, f"Error mapping symbols: {e}", []

    # 4. لاگ و دیباگ: تنظیم نهایی ستون‌ها
    # 💥 FIX: حذف 'tse_index' که در مدل HistoricalData وجود ندارد.
    columns_for_historical_data = [
        'symbol_id', 'symbol_name', 'date', 'jdate', 'open', 'high', 'low', 
        'close', 'final', 'yesterday_price', 'volume', 'value', 'num_trades', 'mv', 
        'buy_count_i', 'buy_count_n', 'sell_count_i', 'sell_count_n', 
        'buy_i_volume', 'buy_n_volume', 'sell_i_volume', 'sell_n_volume', 
        'zd1', 'qd1', 'pd1', 'zo1', 'qo1', 'po1', 
        'zd2', 'qd2', 'pd2', 'zo2', 'qo2', 'po2', 
        'zd3', 'qd3', 'pd3', 'zo3', 'qo3', 'po3', 
        'zd4', 'qd4', 'pd4', 'zo4', 'qo4', 'po4', 
        'zd5', 'qd5', 'pd5', 'zo5', 'qo5', 'po5',
        'plc', 'plp', 'pcc', 'pcp',
    ]

    final_columns = [col for col in columns_for_historical_data if col in df_eod.columns]
    
    logger.debug(f"📐 DataFrame Shape قبل از درج/آپدیت: {df_eod.shape}. ستون‌های نهایی: {final_columns}")
    
    
    # =========================================================================
    # 5. اجرای Upsert در HistoricalData 💥 بهینه‌سازی شده با Bulk Merge 💥
    # =========================================================================
    updated_symbol_ids = []
    total_processed_count = 0
    
    try:
        # تبدیل DataFrame به لیست دیکشنری برای پردازش آسان با ORM
        records = df_eod[final_columns].to_dict('records')
        
        logger.info(f"💾 شروع Bulk Merge برای {len(records)} رکورد در HistoricalData...")
        
        for record in records:
            # 💡 استفاده از merge برای "درج یا آپدیت"
            # merge() به طور خودکار چک می‌کند که آیا یک رکورد با کلید اصلی
            # (symbol_id + date که در مدل HistoricalData باید unique باشد) وجود دارد یا خیر.
            
            # باید نمونه‌ای از مدل HistoricalData ایجاد شود
            record_object = HistoricalData(**record)
            
            # merge() را برای ORM اجرا می‌کنیم
            db_session.merge(record_object)
            
            updated_symbol_ids.append(record['symbol_id'])
            total_processed_count += 1
            
        # ثبت تمام تغییرات در دیتابیس (یک COMMIT واحد)
        logger.info(f"⏳ در حال انجام Commit برای {total_processed_count} رکورد...")
        db_session.commit()
        logger.info(f"✅ عملیات درج/آپدیت {total_processed_count} رکورد با موفقیت به پایان رسید.")
        
    except SQLAlchemyError as e:
        db_session.rollback()
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"❌ خطای دیتابیس در ذخیره‌سازی/آپدیت EOD: {e}", exc_info=True)
        logger.debug(f"❌ جزئیات خطا (Traceback):\n{error_trace}")
        return 0, f"❌ خطای دیتابیس در ذخیره‌سازی EOD: {e}", []

    # 6. آپدیت تاریخ آخرین EOD در ComprehensiveSymbolData
    unique_updated_symbol_ids = list(set(updated_symbol_ids))
    
    try:
        # ⚠️ توجه: نام ستون در مدل ComprehensiveSymbolData را از ورودی شما ('last_historical_update_date') استفاده می‌کنیم.
        db_session.query(ComprehensiveSymbolData).filter(
            ComprehensiveSymbolData.symbol_id.in_(unique_updated_symbol_ids)
        ).update(
            {ComprehensiveSymbolData.last_historical_update_date: today_gregorian}, 
            synchronize_session='fetch'
        )
        db_session.commit()
        logger.info(f"✅ تاریخ آخرین EOD برای {len(unique_updated_symbol_ids)} نماد در ComprehensiveSymbolData به‌روز شد.")
        
    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"❌ خطا در آپدیت ComprehensiveSymbolData: {e}", exc_info=True)
        pass 
        
    
    message = f"✅ آپدیت EOD از BRSAPI کامل شد. {total_processed_count} رکورد در HistoricalData درج/به‌روزرسانی شد. (شامل {len(unique_updated_symbol_ids)} نماد یکتا)."
    logger.info(message)
    
    return total_processed_count, message, unique_updated_symbol_ids