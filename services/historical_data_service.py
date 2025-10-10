# services/historical_data_service.py

from extensions import db
from models import HistoricalData, ComprehensiveSymbolData 
from sqlalchemy import or_, func # اضافه شدن func برای توابع تجمعی
from typing import List, Dict, Optional
from sqlalchemy.orm import sessionmaker
import logging
from datetime import date
from flask import current_app 
import datetime

logger = logging.getLogger(__name__)

# ----------------------------
# Session maker 
# ----------------------------
# این تابع بر اساس ساختار بک‌اند شما است.
def get_session_local():
    """ایجاد session local با application context"""
    try:
        with current_app.app_context():
            # فرض بر این است که SessionLocal در ماژول اصلی یا extensions تعریف شده
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
            return SessionLocal()
    except Exception:
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)
        return SessionLocal()


def get_historical_data_for_symbol(
    symbol_identifier: str, 
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None, 
    # ✨ اصلاح درخواست: تغییر پیش‌فرض از 10 به 21 روز
    days: int = 21 
) -> Optional[List[Dict]]:
    """
    بازیابی داده‌های تاریخی (HistoricalData) از دیتابیس برای یک نماد مشخص.
    
    اولویت فیلتر: (start_date و end_date) > days
    
    Returns:
        لیستی از دیکشنری‌های حاوی داده‌های تاریخی، مرتب‌شده بر اساس تاریخ صعودی (قدیم به جدید)
    """
    session = get_session_local()
    
    try:
        # ۱. پیدا کردن symbol_id داخلی (PK)
        sym_mapping_id = session.query(ComprehensiveSymbolData.symbol_id).filter(
            or_(
                ComprehensiveSymbolData.tse_index == symbol_identifier,
                ComprehensiveSymbolData.symbol_name == symbol_identifier
            )
        ).scalar()
        
        if not sym_mapping_id:
            logger.warning(f"⚠️ نماد '{symbol_identifier}' در دیتابیس ComprehensiveSymbolData یافت نشد.")
            return []

        # ۲. ساخت کوئری GROUP BY برای حذف سوابق تکراری روزانه (منطق قبلی که اضافه شد)
        # از func.max() برای قیمت‌ها (که انتظار می‌رود در رکوردهای تکراری یکسان باشند) و 
        # از func.sum() برای حجم/ارزش (که باید جمع شوند) استفاده می‌شود.
        query = session.query(
            HistoricalData.date, 
            func.max(HistoricalData.open).label('open'),
            func.max(HistoricalData.high).label('high'),
            func.max(HistoricalData.low).label('low'),
            func.max(HistoricalData.final).label('final'), 
            func.max(HistoricalData.close).label('close'), 
            func.max(HistoricalData.pcp).label('pcp'), 
            func.max(HistoricalData.plp).label('plp'), 
            func.sum(HistoricalData.volume).label('volume'), 
            func.sum(HistoricalData.value).label('value'),
            func.sum(HistoricalData.num_trades).label('num_trades'),
            func.sum(HistoricalData.buy_count_i).label('buy_count_i'),
        ).filter(
            # فیلترینگ با symbol_id در جدول HistoricalData
            HistoricalData.symbol_id == sym_mapping_id 
        ).group_by(
            # گروه‌بندی بر اساس تاریخ، که کلید حل مشکل تکرار است
            HistoricalData.date 
        )

        # ۳. اعمال فیلترهای زمانی و مرتب‌سازی
        use_date_range = start_date is not None and end_date is not None

        if use_date_range:
            # فیلتر بر اساس بازه دقیق
            query = query.filter(
                HistoricalData.date >= start_date, 
                HistoricalData.date <= end_date
            )
            # مرتب‌سازی برای بازه: از قدیم به جدید (asc)
            query = query.order_by(HistoricalData.date.asc())
        else:
            # فیلتر بر اساس تعداد روز اخیر
            # مرتب‌سازی: از جدید به قدیم (desc) برای استفاده از limit
            query = query.order_by(HistoricalData.date.desc()).limit(days)
            
        history_records = query.all()
        
        # اگر بر اساس days کوئری گرفتیم، لیست را معکوس می‌کنیم تا قدیمی‌ترین روز در ابتدا باشد.
        if not use_date_range:
            history_records.reverse()
        
        # ۴. تبدیل به فرمت Dict (با توجه به کوئری GROUP BY)
        result = []
        # چون از GROUP BY استفاده کردیم، رکوردهای ما آبجکت مدل نیستند، بلکه تاپل (Row objects) هستند.
        column_names = [
            'date', 'open', 'high', 'low', 'final', 'close', 
            'pcp', 'plp', 'volume', 'value', 'num_trades', 'buy_count_i'
        ]
        
        for record in history_records:
            # تبدیل Row object به Dict با استفاده از نام ستون‌های Label شده
            record_dict = dict(zip(column_names, record))
            record_date = record_dict.get('date')
            
            # مدیریت ایمن date برای تبدیل به isoformat
            formatted_date = record_date.isoformat() if record_date else None
            
            result.append({
                # تاریخ و قیمت‌های اصلی
                "date": formatted_date,
                "open": record_dict.get('open'),
                "high": record_dict.get('high'),
                "low": record_dict.get('low'),
                
                # تطبیق فیلدها با نیاز فرانت‌اند:
                "close": record_dict.get('final'),
                "last_price": record_dict.get('close'),
                
                # تغییرات قیمتی (از نام‌گذاری صحیح Label شده)
                "final_change_percent": record_dict.get('pcp'),
                "last_change_percent": record_dict.get('plp'),
                
                # حجم و معاملات
                "volume": record_dict.get('volume'),
                "value": record_dict.get('value'),
                "trades_count": record_dict.get('num_trades'), 
                
                # خریداران
                "buyers_count": record_dict.get('buy_count_i'),
            })
            
        return result
        
    except Exception:
        # مدیریت لاگ بهتر برای ثبت traceback
        logger.error(f"❌ خطا در بازیابی سابقه معاملات برای {symbol_identifier}", exc_info=True)
        return None 
    finally:
        session.close()

# ----------------------------
# Export functions 
# ----------------------------
__all__ = [
    'get_historical_data_for_symbol',
]