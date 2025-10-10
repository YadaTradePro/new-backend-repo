import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
# --- افزودن کتابخانه requests برای API جدید ---
import requests 

# --- تنظیمات API جدید BrsApi.ir ---
# کلید API دریافتی از BrsApi.ir (لطفاً محرمانه نگه دارید)
B_R_S_API_KEY = "BvhdYHBjqiyIQ7eTuQBKN17ZuLpHkQZ1"
# آدرس اصلی API جدید (استفاده از روش GET)
B_R_S_API_URL = "https://brsapi.ir/Api/Tsetmc/Index.php"
API_TYPE_PARAM = 3 # پارامتر type=3 برای دریافت شاخص‌های اصلی

# حذف وابستگی به pytse_client و wrapper
try:
    # این خطوط دیگر نیازی نیستند و حذف شده‌اند تا Dependency های قدیمی حذف شوند.
    # from services.pytse_wrapper import download_financial_indexes_safe
    pass
except Exception as _e:
    pass

# متغیرهای مربوط به wrapper حذف شدند.
# WRAPPER_AVAILABLE = False 
    
try:
    from flask import current_app
    FLASK_AVAILABLE = True
except Exception:
    FLASK_AVAILABLE = False

logger = logging.getLogger(__name__)

# --- نگاشت نام‌های شاخص به‌روز شده ---
INDEX_NAME_MAPPING = {
    # شاخص‌های موجود در خروجی API جدید که با ساختار قدیمی شما تطبیق دارند
    "شاخص کل": "Total_Index",
    "شاخص کل (هم وزن)": "Equal_Weighted_Index",
    "شاخص قیمت (هم وزن)": "Price_Equal_Weighted_Index",
    # شاخص صنعت در خروجی جدید نیست و با مقدار پیش‌فرض None باقی می‌ماند.
}

def _default_index_payload() -> Dict[str, Dict[str, Any]]:
    """
    خروجی پیش‌فرض و ایمن برای زمانی که نمی‌توانیم دادهٔ واقعی شاخص‌ها را بگیریم.
    """
    return {
        "Total_Index": {"value": None, "change": None, "percent": None, "date": None},
        "Equal_Weighted_Index": {"value": None, "change": None, "percent": None, "date": None},
        "Price_Equal_Weighted_Index": {"value": None, "change": None, "percent": None, "date": None},
        "Industry_Index": {"value": None, "change": None, "percent": None, "date": None},
    }

def _pytse_enabled_by_config() -> bool:
    """
    این تابع به دلیل حذف pytse_client دیگر مورد نیاز نیست، اما برای جلوگیری از خطا در کدهای دیگر 
    که ممکن است آن را صدا بزنند، به سادگی True برمی‌گرداند.
    """
    return True # منطق مربوط به pytse حذف شد

def _safe_to_float(x) -> Optional[float]:
    """
    تبدیل ایمن مقدار به float.
    """
    try:
        if x is None:
            return None
        # برای سازگاری، استفاده از pandas حفظ شد
        val = pd.to_numeric(x, errors="coerce")
        return float(val) if pd.notna(val) else None
    except Exception:
        return None

def _format_date(d) -> Optional[str]:
    """
    فرمت‌دهی ایمن تاریخ.
    """
    if isinstance(d, (pd.Timestamp, datetime)):
        return d.strftime("%Y-%m-%d")
    return str(d) if d is not None else None

def fetch_iran_market_indices() -> Dict[str, Dict[str, Any]]:
    """
    دریافت لحظه‌ای داده‌های شاخص بازار از طریق API جدید BrsApi.ir (روش GET).
    - شاخص‌های مورد نیاز را استخراج و در فرمت استاندارد خروجی می‌دهد.
    - هیچ Exception ی به بیرون نشت نمی‌کند.
    """
    logger.info(f"در حال تلاش برای دریافت داده‌های شاخص بازار ایران از {B_R_S_API_URL}")

    result = _default_index_payload()

    # تنظیم پارامترهای GET (کلید و نوع)
    params = {
        'key': B_R_S_API_KEY,
        'type': API_TYPE_PARAM,
    }

    try:
        # ارسال درخواست GET به API
        response = requests.get(B_R_S_API_URL, params=params, timeout=15)
        response.raise_for_status() # برای تشخیص خطاهای HTTP

        data_list = response.json()
        
        if not isinstance(data_list, list) or not data_list:
            logger.warning("پاسخ API خالی است یا ساختار صحیحی ندارد. بازگشت دادهٔ پیش‌فرض.")
            return result
        
        # --- تحلیل و پردازش داده‌های API جدید ---
        for index_item in data_list:
            # استفاده از کلید 'name' از خروجی JSON
            index_name_raw = index_item.get("name")
            
            # تطبیق نام دریافتی با نام‌های داخلی مورد انتظار
            friendly_name = INDEX_NAME_MAPPING.get(index_name_raw)
            if not friendly_name:
                # این شامل شاخص‌هایی مثل 'شاخص بازار اول' می‌شود که نیاز به ذخیره ندارند.
                logger.debug(f"شاخص ناشناخته/غیرضروری از API دریافت شد: {index_name_raw}")
                continue
                
            # استخراج مقادیر بر اساس نام ستون‌های جدید API
            # کلیدهای JSON: index, index_change, index_change_percent
            value = _safe_to_float(index_item.get("index"))
            # index_change: مقدار تغییر شاخص
            change = _safe_to_float(index_item.get("index_change")) 
            # index_change_percent: درصد تغییر شاخص
            percent = _safe_to_float(index_item.get("index_change_percent"))
            
            # API جدید تاریخ (date) را نمی‌دهد، لذا از تاریخ فعلی سیستم استفاده می‌کنیم.
            date_fmt = datetime.now().strftime("%Y-%m-%d")

            result[friendly_name] = {
                "value": value,
                "change": change,
                "percent": percent,
                "date": date_fmt,
            }

        logger.info("داده‌های شاخص بازار با موفقیت از BrsApi.ir دریافت و پردازش شد.")
            
    except requests.exceptions.Timeout:
        logger.error("خطا: درخواست API به دلیل timeout (۱۵ ثانیه) لغو شد.")
    except requests.exceptions.RequestException as e:
        logger.error(f"خطا در برقراری ارتباط با API BrsApi.ir: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"خطای غیرمنتظره در پردازش داده‌ها: {e}", exc_info=True)

    # اگر شاخص Industry_Index در API جدید وجود نداشت، مقدار پیش‌فرض آن (None) باقی می‌ماند.
    return result