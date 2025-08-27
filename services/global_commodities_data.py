# services/global_commodities_data.py
import requests
import logging
from flask import current_app

# تنظیم لاگینگ
logger = logging.getLogger(__name__)

def fetch_global_commodities():
    """
    دریافت لحظه‌ای قیمت کالاهای جهانی از API Metals.dev.
    """
    logger.info("در حال تلاش برای دریافت قیمت کالاهای جهانی از API Metals.dev.")
    METALS_DEV_API_KEY = current_app.config.get('METALS_DEV_API_KEY')

    if not METALS_DEV_API_KEY:
        logger.error("METALS_DEV_API_KEY در تنظیمات Flask تعریف نشده است.")
        return {}

    url = f"https://api.metals.dev/v1/latest?api_key={METALS_DEV_API_KEY}&currency=USD&unit=toz"
    prices = {}
    try:
        response = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "metals" not in data:
            logger.warning("کلید 'metals' در پاسخ از Metals.dev یافت نشد.")
            return {}

        commodities_map = {"gold": "gold", "silver": "silver", "platinum": "platinum", "copper": "copper"}
        for name, key in commodities_map.items():
            price = data["metals"].get(key)
            if price is not None:
                prices[name] = price
    except requests.exceptions.RequestException as e:
        logger.error(f"خطا در دریافت قیمت کالاها از Metals.dev: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"خطای غیرمنتظره در حین دریافت قیمت کالا: {e}", exc_info=True)
    
    return prices