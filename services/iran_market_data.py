# services/iran_market_data.py

import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd

# نکته: دیگر مستقیماً pytse_client را اینجا وارد نمی‌کنیم
# و هیچ monkey-patch روی requests انجام نمی‌دهیم.
# تمام ارتباط با TSETMC فقط از مسیر Wrapper انجام می‌شود.
try:
    from services.pytse_wrapper import download_financial_indexes_safe
    WRAPPER_AVAILABLE = True
except Exception as _e:
    WRAPPER_AVAILABLE = False
    
try:
    from flask import current_app
    FLASK_AVAILABLE = True
except Exception:
    FLASK_AVAILABLE = False

logger = logging.getLogger(__name__)

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
    اگر داخل App Context هستیم و کلید تنظیمات وجود دارد، از آن پیروی می‌کنیم.
    در غیر این صورت، True برمی‌گردانیم تا رفتار پیش‌فرض، فعال بودن باشد.
    """
    if not FLASK_AVAILABLE:
        return True
    try:
        # ممکن است خارج از app context صدا زده شود
        return bool(getattr(current_app, "config", {}).get("PYTSE_CLIENT_AVAILABLE", True))
    except Exception:
        return True

def _safe_to_float(x):
    """
    تبدیل ایمن مقدار به float.
    """
    try:
        val = pd.to_numeric(x, errors="coerce")
        return float(val) if pd.notna(val) else None
    except Exception:
        return None

def _format_date(d):
    """
    فرمت‌دهی ایمن تاریخ.
    """
    if isinstance(d, (pd.Timestamp, datetime)):
        return d.strftime("%Y-%m-%d")
    return str(d) if d is not None else None

def fetch_iran_market_indices() -> Dict[str, Dict[str, Any]]:
    """
    دریافت لحظه‌ای داده‌های شاخص بازار از طریق Wrapper ایمن (TSETMC).
    - اگر Wrapper/کتابخانه در دسترس نباشد یا TSETMC پاسخ ندهد، خروجی پیش‌فرض می‌دهد.
    - هیچ Exception ی به بیرون نشت نمی‌کند.
    """
    logger.info("در حال تلاش برای دریافت داده‌های شاخص بازار ایران از طریق Wrapper امن pytse-client.")

    result = _default_index_payload()

    # اگر بر اساس تنظیمات پروژه، pytse غیرفعال شده باشد
    if not _pytse_enabled_by_config():
        logger.error("ماژول pytse_client بر اساس تنظیمات اپ غیرفعال است. بازگشت دادهٔ پیش‌فرض شاخص‌ها.")
        return result

    # اگر خود Wrapper در دسترس نباشد
    if not WRAPPER_AVAILABLE:
        logger.error("Wrapper خدمات TSETMC (pytse_wrapper) در دسترس نیست. بازگشت دادهٔ پیش‌فرض شاخص‌ها.")
        return result

    # اسامی شاخص‌ها به شکل مورد انتظار pytse
    index_symbols_to_fetch = [
        "شاخص كل",
        "شاخص كل (هم وزن)",
        "شاخص قيمت (هم وزن)",
        "شاخص صنعت",
    ]

    # نگاشت فارسی -> کلید دوستانهٔ API
    reverse_mapping = {
        "شاخص كل": "Total_Index",
        "شاخص كل (هم وزن)": "Equal_Weighted_Index",
        "شاخص قيمت (هم وزن)": "Price_Equal_Weighted_Index",
        "شاخص صنعت": "Industry_Index",
    }

    try:
        # فقط یک فراخوانی به Wrapper که خودش timeout/retry/circuit را مدیریت می‌کند.
        financial_indexes = download_financial_indexes_safe(
            symbols=index_symbols_to_fetch,
            timeout=10,
            retries=2,
            backoff=5,
        )

        if not financial_indexes:
            logger.warning("Wrapper هیچ داده‌ای برای شاخص‌ها برنگرداند. بازگشت دادهٔ پیش‌فرض.")
            return result

        for pytse_name, df in financial_indexes.items():
            friendly_name = reverse_mapping.get(pytse_name)
            if not friendly_name:
                logger.debug(f"شاخص ناشناخته از pytse دریافت شد: {pytse_name}")
                continue

            # DataFrame ممکن است خالی یا دارای داده نامعتبر باشد
            if df is None or getattr(df, "empty", True):
                logger.warning(f"DataFrame مربوط به '{pytse_name}' خالی است. مقدار پیش‌فرض برای '{friendly_name}'.")
                continue

            try:
                latest = df.iloc[-1]
            except Exception:
                logger.warning(f"امکان دسترسی به سطر آخر DataFrame برای '{pytse_name}' نبود. مقدار پیش‌فرض برای '{friendly_name}'.")
                continue

            date_fmt = _format_date(latest.get("date"))
            close_val = _safe_to_float(latest.get("close"))
            open_val = _safe_to_float(latest.get("open"))

            change_val = None
            percent_val = None
            if close_val is not None and open_val not in (None, 0):
                try:
                    change_val = close_val - open_val
                    percent_val = round((change_val / open_val) * 100, 2)
                except Exception:
                    change_val, percent_val = None, None

            result[friendly_name] = {
                "value": close_val,
                "change": change_val,
                "percent": percent_val,
                "date": date_fmt,
            }

    except Exception as e:
        # هر خطایی اینجا رخ بده، لاگ می‌کنیم و خروجی پیش‌فرض می‌دهیم.
        logger.error(f"خطا در دریافت یا پردازش داده‌های شاخص بازار از Wrapper: {e}", exc_info=True)
        return result

    return result