# routes/market_data.py
import requests
import jdatetime
import logging
from flask import current_app, jsonify
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required

# وارد کردن سرویس‌های داده جدید
from services.iran_market_data import fetch_iran_market_indices
from services.global_commodities_data import fetch_global_commodities

# تنظیم لاگینگ
logger = logging.getLogger(__name__)

# --- تعریف namespace و مدل‌ها برای Swagger UI ---
market_overview_ns = Namespace('market-overview', description='Market overview data')

# مدل داده برای TGJU
tgju_data_model = market_overview_ns.model('TGJUData', {
    'gold_prices': fields.Raw(description='List of gold prices from TGJU.'),
    'currency_prices': fields.Raw(description='List of currency prices from TGJU.')
})

# مدل داده برای شاخص‌های بورس ایران
iran_indices_model = market_overview_ns.model('IranMarketIndices', {
    'Total_Index': fields.Raw(description='Overall Bourse Index'),
    'Equal_Weighted_Index': fields.Raw(description='Equal-weighted Bourse Index'),
    'Price_Equal_Weighted_Index': fields.Raw(description='Price Equal-weighted Bourse Index'),
    'Industry_Index': fields.Raw(description='Industry Bourse Index')
})

# مدل داده برای کالاهای جهانی
global_commodities_model = market_overview_ns.model('GlobalCommodities', {
    'gold': fields.Float(description='Price of Gold'),
    'silver': fields.Float(description='Price of Silver'),
    'platinum': fields.Float(description='Price of Platinum'),
    'copper': fields.Float(description='Price of Copper')
})

# مدل اصلی برای پاسخ API
market_overview_model = market_overview_ns.model('MarketOverview', {
    'date': fields.String(description='Current Persian date (YYYY/MM/DD)'),
    'tgju_data': fields.Nested(tgju_data_model, description='Data from TGJU proxy.'),
    'iran_market_indices': fields.Nested(iran_indices_model, description='Indices from Iran Bourse (TSETMC).'),
    'global_commodities': fields.Nested(global_commodities_model, description='Prices of global commodities.')
})

# --- منطق Endpoint ---
@market_overview_ns.route('/')
class MarketOverviewResource(Resource):
    @market_overview_ns.doc(security='Bearer Auth')
    @jwt_required()
    @market_overview_ns.marshal_with(market_overview_model)
    def get(self):
        """
        بازگرداندن داده‌های کلی بازار شامل TGJU، بورس و کالاهای جهانی.
        """
        overview_data = {
            "date": jdatetime.date.today().strftime("%Y/%m/%d"),
            "tgju_data": {
                "gold_prices": [],
                "currency_prices": []
            },
            "iran_market_indices": {},
            "global_commodities": {}
        }

        # تنظیم timeout و URLها از config (با fallback به مقدار پیش‌فرض)
        timeout = current_app.config.get("TGJU_TIMEOUT", 8)
        tgju_proxy_base = current_app.config.get("TGJU_PROXY_URL", "http://tgju_proxy:5001/api/price")
        tgju_fallback_base = current_app.config.get("TGJU_FALLBACK_URL", "https://call5.tgju.org")

        tgju_gold_url = f"{tgju_proxy_base}/gold"
        tgju_currency_url = f"{tgju_proxy_base}/currency"

        # 1. دریافت داده‌های TGJU از سرور پراکسی محلی
        tgju_data = {"gold_prices": [], "currency_prices": []}

        # Gold prices
        try:
            gold_response = requests.get(tgju_gold_url, timeout=timeout)
            gold_response.raise_for_status()
            tgju_data["gold_prices"] = gold_response.json()
        except Exception as e:
            logger.warning(f"عدم موفقیت در دریافت Gold از پراکسی، تلاش با fallback: {e}")
            try:
                fallback_url = f"{tgju_fallback_base}/ajax.json"
                fallback_resp = requests.get(fallback_url, timeout=timeout)
                fallback_resp.raise_for_status()
                raw = fallback_resp.json()
                # استخراج طلای 18 عیار از fallback
                gold_items = [i for i in raw.get("last", []) if "gold" in i.get("name", "")]
                tgju_data["gold_prices"] = gold_items
            except Exception as ee:
                logger.error(f"خطا در دریافت Gold حتی با fallback: {ee}", exc_info=True)

        # Currency prices
        try:
            currency_response = requests.get(tgju_currency_url, timeout=timeout)
            currency_response.raise_for_status()
            tgju_data["currency_prices"] = currency_response.json()
        except Exception as e:
            logger.warning(f"عدم موفقیت در دریافت Currency از پراکسی، تلاش با fallback: {e}")
            try:
                fallback_url = f"{tgju_fallback_base}/ajax.json"
                fallback_resp = requests.get(fallback_url, timeout=timeout)
                fallback_resp.raise_for_status()
                raw = fallback_resp.json()
                # استخراج ارزها از fallback
                currency_items = [i for i in raw.get("last", []) if "usd" in i.get("name", "").lower() or "eur" in i.get("name", "").lower()]
                tgju_data["currency_prices"] = currency_items
            except Exception as ee:
                logger.error(f"خطا در دریافت Currency حتی با fallback: {ee}", exc_info=True)

        overview_data["tgju_data"] = tgju_data

        # 2. دریافت داده‌های شاخص بورس ایران
        try:
            iran_indices = fetch_iran_market_indices()
            overview_data["iran_market_indices"] = iran_indices
        except Exception as e:
            logger.error(f"خطا در دریافت داده‌های شاخص بورس ایران: {e}", exc_info=True)
            overview_data["iran_market_indices"] = {"error": "Failed to fetch Iran market indices."}

        # 3. دریافت داده‌های کالاهای جهانی
        try:
            global_commodities = fetch_global_commodities()
            overview_data["global_commodities"] = global_commodities
        except Exception as e:
            logger.error(f"خطا در دریافت داده‌های کالاهای جهانی: {e}", exc_info=True)
            overview_data["global_commodities"] = {"error": "Failed to fetch global commodities data."}

        return overview_data, 200