# -*- coding: utf-8 -*-
# services/market_analysis_service.py

import logging
from datetime import datetime, timedelta, date
import jdatetime
from sqlalchemy.exc import SQLAlchemyError
import json
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np


from models import (
    ComprehensiveSymbolData,
    HistoricalData,
    TechnicalIndicatorData,
    GoldenKeyResult,
    AggregatedPerformance,
    WeeklyWatchlistResult,
)


# Import Jinja2 for templating
from jinja2 import Environment, FileSystemLoader, Template

# Import necessary modules and models from the Flask application structure
# این سرویس فرض می‌کند که نمونه 'db' از طریق ماژول extensions در دسترس است
from extensions import db
from services.iran_market_data import fetch_iran_market_indices
from services.utils import calculate_smart_money_flow

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# تنظیمات Jinja2: استفاده از قالب‌های جداگانه برای خوانایی بهتر
# -----------------------------------------------------------------------------

# تعریف متغیرهای قالب در سطح ماژول
daily_template = None
weekly_template = None

try:
    # مسیر 'templates' باید نسبت به ریشه پروژه یا محل اجرای اسکریپت تنظیم شود.
    # در یک برنامه Flask، این معمولاً به پوشه templates اصلی اشاره دارد.
    template_loader = FileSystemLoader('services/templates')
    template_env = Environment(loader=template_loader)
    daily_template = template_env.get_template('daily_summary.j2')
    weekly_template = template_env.get_template('weekly_summary.j2')
    logger.info("✅ قالب‌های Jinja2 با موفقیت از فایل‌ها بارگذاری شدند.")
except Exception as e:
    logger.error(f"❌ خطای بارگذاری قالب‌های Jinja2 از فایل: {e}. استفاده از قالب‌های درون‌حافظه‌ای.", exc_info=True)

    # Fallback to in-memory templates for robustness
    DAILY_TEMPLATE_STRING = """
**تحلیل روزانه بازار بورس تهران - {{ jdate }}**

## نمای کلی بازار
**شاخص کل:** با تغییر **{{ '%.2f'|format(indices_data.Total_Index.percent|default(0)) }}%**، امروز روندی {{ indices_data.Total_Index.status }} را تجربه کرد.
**شاخص هم‌وزن:** عملکرد {{ indices_data.Equal_Weighted_Index.status }} آن با تغییر **{{ '%.2f'|format(indices_data.Equal_Weighted_Index.percent|default(0)) }}%** نشان‌دهنده وضعیت سهام کوچک و متوسط بود.
{{ smart_money_flow_text }}

## تحلیل نمادهای منتخب
{% if all_symbols %}
در ادامه سیگنال‌های جدیدی که امروز شناسایی شده‌اند، آمده است:
{{ symbols_text }}
{% else %}
امروز نماد جدیدی در لیست‌های انتخابی سیگنال‌دهی نشده است.
{% endif %}
"""
    WEEKLY_TEMPLATE_STRING = """
**تحلیل هفتگی بازار بورس تهران - {{ jdate }}**

## نمای کلی بازار
{% if indices_data %}
**عملکرد شاخص‌ها:** شاخص کل در این هفته **{{ '%.2f'|format(indices_data.total_profit_percent|default(0)) }}%** و شاخص هم‌وزن (بر اساس عملکرد GoldenKey) **{{ '%.2f'|format(indices_data.win_rate|default(0)) }}%** نرخ برد داشته است.
{% else %}
خلاصه روند شاخص‌ها در دسترس نیست.
{% endif %}
{{ smart_money_flow_text }}

## عملکرد نمادهای منتخب هفته
{% if all_symbols %}
در ادامه عملکرد سیگنال‌های ارائه شده در طول هفته ارزیابی شده است:
{{ symbols_text }}
{% else %}
در این هفته هیچ نماد جدیدی در لیست‌های انتخابی سیگنال‌دهی نشده است.
{% endif %}
"""
    daily_template = Template(DAILY_TEMPLATE_STRING)
    weekly_template = Template(WEEKLY_TEMPLATE_STRING)
    logger.info("✅ قالب‌های Jinja2 با موفقیت از رشته‌های درون‌حافظه‌ای بارگذاری شدند.")

# -----------------------------------------------------------------------------
# توابع کمکی بهینه شده
# -----------------------------------------------------------------------------

def _get_day_type() -> str:
    """
    روز هفته را برای تعیین نوع تحلیل (روزانه، هفتگی یا بدون تحلیل) مشخص می‌کند.
    """
    today_jdate = jdatetime.date.today()
    weekday = today_jdate.weekday()
    
    if weekday in [0, 1, 2, 3, 4]:  # شنبه تا چهارشنبه
        return 'daily'
    elif weekday == 6:  # جمعه
        return 'weekly'
    else:  # پنجشنبه (5)
        return 'no_analysis_day'

def _calculate_pnl(entry_price: float, exit_price: Optional[float]) -> Optional[float]:
    """
    درصد سود یا زیان را محاسبه می‌کند.
    """
    if not entry_price or entry_price == 0 or exit_price is None:
        return None
    return round(((exit_price - entry_price) / entry_price) * 100, 2)

def _get_formatted_smart_money_flow_text(net_flow: float, is_weekly: bool) -> str:
    """متن فرمت‌شده برای نمایش وضعیت ورود و خروج پول هوشمند را تولید می‌کند."""
    period = "امروز" if not is_weekly else "در مجموع این هفته"
    if net_flow > 0:
        return f"{period} شاهد ورود پول حقیقی به ارزش تقریبی **{net_flow / 1e10:.2f} میلیارد تومان** به بازار بودیم."
    elif net_flow < 0:
        return f"{period} خروج پول حقیقی به ارزش تقریبی **{abs(net_flow) / 1e10:.2f} میلیارد تومان** از بازار صورت گرفت."
    else:
        return f"{period} جریان پول حقیقی در بازار تقریباً خنثی بود."

def _get_formatted_symbols_text(symbols: List[Any], is_weekly: bool) -> str:
    """متن فرمت‌شده برای نمایش تحلیل نمادهای منتخب را تولید می‌کند."""
    text_parts = []
    for symbol_data in symbols:
        symbol_name = symbol_data.symbol_name
        signal_source = getattr(symbol_data, 'signal_source', 'N/A').replace('Service', '')
        reasons = getattr(symbol_data, 'reasons', '{}')
        if not isinstance(reasons, str):
            reasons = json.dumps(reasons, ensure_ascii=False)
        entry_price = symbol_data.entry_price

        if not is_weekly:
            daily_change = getattr(symbol_data, 'daily_change_percent', None)
            
            status_text = ""
            if daily_change is not None:
                if daily_change > 0:
                    status_text = f"با رشد **{daily_change:.2f}%** همراه بود."
                elif daily_change < 0:
                    status_text = f"با کاهش **{abs(daily_change):.2f}%** همراه بود."
                else:
                    status_text = "بدون تغییر قیمت بسته شد."
            else:
                status_text = "تغییرات روزانه آن در دسترس نیست."

            text_parts.append(f"**- نماد {symbol_name} ({signal_source}):** {status_text} (دلیل سیگنال: {reasons})")
        else:
            pnl_percent = getattr(symbol_data, 'profit_loss_percentage', None)
            
            status_text = ""
            if pnl_percent is not None:
                if pnl_percent > 0:
                    status_text = f"این هفته **{pnl_percent:.2f}%** سوددهی داشته است."
                elif pnl_percent < 0:
                    status_text = f"این هفته با **{abs(pnl_percent):.2f}%** زیان بسته شد."
                else:
                    status_text = "این هفته بدون تغییر قیمت بسته شد."
            else:
                status_text = "هنوز در وضعیت فعال قرار دارد و ارزیابی نهایی نشده است."
            
            text_parts.append(f"**- نماد {symbol_name}:** {status_text} (دلیل سیگنال: {reasons})")
            
    return "\n".join(text_parts)

def _prepare_indices_data(indices_data: Dict) -> Dict:
    """داده‌های شاخص‌ها را برای استفاده در قالب آماده‌سازی می‌کند."""
    processed_data = {}
    for key, value in indices_data.items():
        percent = value.get('percent', 0) or 0
        status = 'صعودی' if percent > 0 else 'نزولی' if percent < 0 else 'بدون تغییر'
        processed_data[key] = {'percent': percent, 'status': status}
    return processed_data

# -----------------------------------------------------------------------------
# توابع اصلی تحلیل
# -----------------------------------------------------------------------------

def _generate_daily_summary() -> str:
    """
    یک تحلیل روزانه مختصر از بازار با استفاده از قالب تولید می‌کند.
    این تابع سیگنال‌های *جدید* همان روز را نمایش می‌دهد.
    """
    logger.info("شروع فرآیند تولید تحلیل روزانه بازار...")
    
    try:
        # 1. دریافت داده‌های شاخص از منبع آنلاین
        raw_indices_data = fetch_iran_market_indices()
        indices_data = _prepare_indices_data(raw_indices_data)
        
        # 2. دریافت داده‌های تاریخی مورد نیاز برای محاسبه جریان پول
        today_jdate_str = jdatetime.date.today().strftime('%Y-%m-%d')
        yesterday_jdate_str = (jdatetime.date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

        historical_data_for_df = HistoricalData.query.filter(
            HistoricalData.jdate == today_jdate_str,
            HistoricalData.symbol_name.isnot(None)
        ).all()
        
        # تبدیل داده‌ها به DataFrame و فراخوانی تابع با آرگومان صحیح
        df = pd.DataFrame([hd.__dict__ for hd in historical_data_for_df])
        smart_money_flow = calculate_smart_money_flow(df)
        
        # 3. دریافت سیگنال‌های جدید همان روز از پایگاه داده
        golden_key_results = GoldenKeyResult.query.filter(GoldenKeyResult.jdate == today_jdate_str).all()
        weekly_watchlist_results = WeeklyWatchlistResult.query.filter(WeeklyWatchlistResult.jentry_date == today_jdate_str).all()
        
        all_new_symbols = golden_key_results + weekly_watchlist_results

        # 4. محاسبه تغییرات روزانه برای هر نماد
        for symbol in all_new_symbols:
            today_data = HistoricalData.query.filter_by(symbol_id=symbol.symbol_id, jdate=today_jdate_str).first()
            yesterday_data = HistoricalData.query.filter_by(symbol_id=symbol.symbol_id, jdate=yesterday_jdate_str).first()

            if today_data and yesterday_data and yesterday_data.close_price != 0:
                daily_change = ((today_data.close_price - yesterday_data.close_price) / yesterday_data.close_price) * 100
                setattr(symbol, 'daily_change_percent', round(daily_change, 2))
            else:
                setattr(symbol, 'daily_change_percent', None)

        # 5. آماده‌سازی داده‌ها برای ارسال به قالب
        data_for_template = {
            'jdate': today_jdate_str,
            'indices_data': indices_data,
            'smart_money_flow_text': _get_formatted_smart_money_flow_text(smart_money_flow.get('net_real_money_flow', 0), is_weekly=False),
            'all_symbols': all_new_symbols,
            'symbols_text': _get_formatted_symbols_text(all_new_symbols, is_weekly=False)
        }
        
        return daily_template.render(data_for_template)

    except SQLAlchemyError as e:
        logger.error(f"❌ خطای پایگاه داده در تولید تحلیل روزانه: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای پایگاه داده، امکان تولید تحلیل روزانه وجود ندارد."
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در تولید تحلیل روزانه: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای فنی، امکان تولید تحلیل روزانه وجود ندارد."

def _generate_weekly_summary() -> str:
    """
    یک تحلیل هفتگی جامع از عملکرد بازار و نمادها تولید می‌کند.
    این تابع به داده‌های ارزیابی‌شده توسط فرآیندهای پس‌زمینه متکی است.
    """
    logger.info("شروع فرآیند تولید تحلیل هفتگی بازار...")
    
    try:
        week_ago_greg = datetime.now().date() - timedelta(days=7)
        week_ago_jdate_str = jdatetime.date.fromgregorian(date=week_ago_greg).strftime('%Y-%m-%d')
        
        # 1. دریافت داده‌های تجمیعی عملکرد که توسط یک جاب پس‌زمینه (update_and_save_performance_metrics) محاسبه شده است.
        aggregated_data = AggregatedPerformance.query.filter(
            AggregatedPerformance.period_type == 'weekly'
        ).order_by(AggregatedPerformance.created_at.desc()).first()
        
        # 2. دریافت داده‌های HistoricalData برای محاسبه جریان پول
        historical_data_for_df = HistoricalData.query.filter(
            HistoricalData.jdate >= week_ago_jdate_str,
            HistoricalData.symbol_name.isnot(None)
        ).all()
        
        # تبدیل داده‌ها به DataFrame و فراخوانی تابع با آرگومان صحیح
        df = pd.DataFrame([hd.__dict__ for hd in historical_data_for_df])
        weekly_smart_money_flow = calculate_smart_money_flow(df)
        
        # 3. دریافت رکوردهای سیگنال‌های هفته که توسط جاب پس‌زمینه (evaluate_weekly_watchlist_performance) ارزیابی شده‌اند.
        golden_key_records = GoldenKeyResult.query.filter(GoldenKeyResult.jdate >= week_ago_jdate_str).all()
        weekly_watchlist_records = WeeklyWatchlistResult.query.filter(WeeklyWatchlistResult.jentry_date >= week_ago_jdate_str).all()
        
        all_week_symbols = golden_key_records + weekly_watchlist_records
        
        # 4. آماده‌سازی داده‌ها برای ارسال به قالب
        data_for_template = {
            'jdate': jdatetime.date.today().strftime('%Y-%m-%d'),
            'indices_data': aggregated_data,
            'smart_money_flow_text': _get_formatted_smart_money_flow_text(weekly_smart_money_flow.get('net_real_money_flow', 0), is_weekly=True),
            'all_symbols': all_week_symbols,
            'symbols_text': _get_formatted_symbols_text(all_week_symbols, is_weekly=True)
        }
        
        return weekly_template.render(data_for_template)

    except SQLAlchemyError as e:
        logger.error(f"❌ خطای پایگاه داده در تولید تحلیل هفتگی: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای پایگاه داده، امکان تولید تحلیل هفتگی وجود ندارد."
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در تولید تحلیل هفتگی: {e}", exc_info=True)
        return "❌ متأسفانه به دلیل خطای فنی، امکان تولید تحلیل هفتگی وجود ندارد."

# -----------------------------------------------------------------------------
# تابع اصلی سرویس
# -----------------------------------------------------------------------------

def generate_market_summary() -> str:
    """
    تابع اصلی سرویس که بسته به روز هفته، تحلیل روزانه یا هفتگی را برمی‌گرداند.
    """
    logger.info("سرویس تحلیل بازار فراخوانی شد.")
    day_type = _get_day_type()
    
    if day_type == 'daily':
        return _generate_daily_summary()
    elif day_type == 'weekly':
        return _generate_weekly_summary()
    elif day_type == 'no_analysis_day':
        logger.info("امروز پنجشنبه است؛ تحلیل بازار منتشر نمی‌شود.")
        return "در روز پنجشنبه، بازار سرمایه فعال نیست و تحلیل روزانه منتشر نمی‌شود."
    
    # این حالت نباید رخ دهد
    return "نوع تحلیل برای روز جاری قابل تشخیص نیست."

# -----------------------------------------------------------------------------
# شبه‌کد برای فرآیند پس‌زمینه (جهت مستندسازی)
# -----------------------------------------------------------------------------

def update_evaluated_prices_job():
    """
    شبه‌کد: این تابع باید توسط یک زمان‌بند (Scheduler) مانند Celery یا Cron
    به صورت روزانه اجرا شود تا قیمت خروج و سود/زیان سیگنال‌های فعال را به‌روز کند.
    این فرآیند معادل منطق `evaluate_weekly_watchlist_performance` است.
    """
    logger.info("شروع جاب زمان‌بندی شده برای ارزیابی سیگنال‌های فعال...")
    try:
        # 1. بازیابی تمام سیگنال‌های فعال که هنوز قیمت خروج ندارند.
        active_signals = WeeklyWatchlistResult.query.filter(
            WeeklyWatchlistResult.status == 'active'
        ).all()
        
        # 2. برای هر سیگنال، آخرین قیمت را از دیتابیس لوکال دریافت و وضعیت آن را ارزیابی کن.
        for signal in active_signals:
            try:
                # اصلاحیه: قیمت از دیتابیس لوکال HistoricalData خوانده می‌شود
                latest_historical_data = HistoricalData.query.filter_by(
                    symbol_id=signal.symbol_id
                ).order_by(HistoricalData.jdate.desc()).first()

                if not latest_historical_data:
                    logger.warning(f"❌ آخرین داده تاریخی برای نماد {signal.symbol_name} یافت نشد. ارزیابی انجام نشد.")
                    continue

                latest_price = latest_historical_data.close_price

                # ... منطق ارزیابی (حد سود/ضرر) ...
                if latest_price: # Placeholder for evaluation logic
                    signal.exit_price = latest_price 
                    signal.status = 'evaluated' # یا closed_win, closed_loss
                    signal.profit_loss_percentage = _calculate_pnl(signal.entry_price, signal.exit_price)
                    db.session.add(signal)
            except Exception as e:
                logger.error(f"❌ خطا در ارزیابی نماد {signal.symbol_name}: {e}")
            
        db.session.commit()
        logger.info(f"ارزیابی {len(active_signals)} سیگنال فعال با موفقیت انجام شد.")
        
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"❌ خطای پایگاه داده در جاب ارزیابی: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در جاب ارزیابی: {e}", exc_info=True)