# services/data_fetch_and_process.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData
from flask import current_app
#import pytse_client as tse
import pandas as pd
from datetime import datetime, date, timedelta # Import date here too
import jdatetime
from sqlalchemy import func
import numpy as np
#import requests
from bs4 import BeautifulSoup # Import BeautifulSoup
import lxml # lxml is the parser for BeautifulSoup

#ماژول‌های pytse_client و requests را حذف کرده و توابع مورد نیاز از ماژول pytse_wrapper را ایمپورت کنید.
from services.pytse_wrapper import (
    http_get, 
    Ticker, 
    download, 
    safe_download_batch, 
    all_tickers, 
    download_financial_indexes_safe
)

# Import utility functions - ensure calculate_atr is present in your utils.py
from services.utils import convert_gregorian_to_jalali, normalize_value, calculate_rsi, calculate_macd, calculate_sma, calculate_bollinger_bands, calculate_volume_ma, calculate_atr, calculate_smart_money_flow # Added calculate_smart_money_flow here

# تنظیمات لاگینگ برای این ماژول
import logging
logger = logging.getLogger(__name__)

# Global mapping for market types from pytse_client.flow (or TSETMC's flow code)
MARKET_TYPE_MAP = {
    0: 'عمومی',
    1: 'بورس',
    2: 'فرابورس',
    3: 'مشتقه',
    4: 'پایه فرابورس',
    5: 'پایه فرابورس (منتشر نمی شود)',
    6: 'بورس انرژی',
    7: 'بورس کالا'
}

# Mapping for market types extracted from HTML or Ticker.group_name to our standard names
# This helps standardize names if HTML parsing or group_name returns variations
HTML_MARKET_TYPE_MAP = {
    'بورس': 'بورس',
    'فرابورس': 'فرابورس',
    'بازار اول (بورس)': 'بورس',
    'بازار دوم (بورس)': 'بورس',
    'بازار پایه فرابورس': 'پایه فرابورس',
    'بازار مشتقه': 'مشتقه',
    'بازار بورس کالا': 'بورس کالا',
    'بورس انرژی': 'بورس انرژی',
    'صندوق سرمایه گذاری': 'صندوق سرمایه گذاری', # Common for funds
    'صندوق': 'صندوق سرمایه گذاری', # Shorter version
    'اوراق بهادار با درآمد ثابت': 'اوراق با درآمد ثابت', # Bonds, etc.
    'اوراق تامین مالی': 'اوراق تامین مالی',
    'حق تقدم': 'حق تقدم',
    'صندوق قابل معامله': 'صندوق سرمایه گذاری', # ETF
    'صندوق های قابل معامله': 'صندوق سرمایه گذاری', # Plural
    'صندوقهای سرمایه گذاری': 'صندوق سرمایه گذاری',
    'صندوق سرمایه گذاری مشترک': 'صندوق سرمایه گذاری',
    'صندوق سرمایه گذاری قابل معامله': 'صندوق سرمایه گذاری',
    'صندوق سرمایه گذاری در سهام': 'صندوق سرمایه گذاری',
    'صندوق سرمایه گذاری مختلط': 'صندوق سرمایه گذاری',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت': 'صندوق سرمایه گذاری',
    'صندوق سرمایه گذاری طلا': 'صندوق سرمایه گذاری',
    'اوراق اجاره': 'اوراق با درآمد ثابت',
    'صکوک': 'اوراق با درآمد ثابت',
    'گواهی سپرده کالایی': 'بورس کالا', # Example for commodity exchange
    'حراج': 'عمومی', # Example for general/auction
    'سهام': 'بورس', # Default for stocks
    'صکوک اجاره': 'اوراق با درآمد ثابت',
    'اوراق مشارکت': 'اوراق با درآمد ثابت',
    'اختیار معامله': 'مشتقه',
    'آتی': 'مشتقه',
    'بورس کالا و انرژی': 'بورس کالا', # Broader term
    'بورس اوراق بهادار تهران': 'بورس',
    'فرابورس ایران': 'فرابورس',
    'بازار پایه': 'پایه فرابورس',
    'بازار ابزارهای نوین مالی': 'فرابورس', # Often contains funds
    'گواهی سپرده': 'اوراق با درآمد ثابت',
    'صندوق‌های سرمایه‌گذاری قابل معامله': 'صندوق سرمایه گذاری', # Another variation for ETFs
    'صندوق‌های سرمایه‌گذاری': 'صندوق سرمایه گذاری', # Generic funds
    'صندوق‌های سرمایه‌گذاری در سهام': 'صندوق سرمایه گذاری',
    'صندوق‌های سرمایه‌گذاری مختلط': 'صندوق سرمایه گذاری',
    'صندوق‌های سرمایه‌گذاری با درآمد ثابت': 'صندوق سرمایه گذاری',
    'صندوق‌های سرمایه‌گذاری کالایی': 'صندوق سرمایه گذاری',
    'بازار ابزارهای مالی نوین': 'فرابورس', # Another variation
    'صندوق سرمایه گذاری در اوراق بهادار': 'صندوق سرمایه گذاری', # Generic for securities funds
    'صندوق سرمایه گذاری در صندوق': 'صندوق سرمایه گذاری', # Fund of funds
    'صندوق سرمایه گذاری جسورانه': 'صندوق سرمایه گذاری', # Venture Capital Fund
    'صندوق سرمایه گذاری زمین و ساختمان': 'صندوق سرمایه گذاری', # Real Estate Fund
    'صندوق سرمایه گذاری اختصاصی بازارگردانی': 'صندوق سرمایه گذاری', # Market Making Fund
    'صندوق سرمایه گذاری پروژه': 'صندوق سرمایه گذاری', # Project Fund
    'صندوق سرمایه گذاری در بورس کالا': 'صندوق سرمایه گذاری', # Commodity Exchange Fund
    'صندوق سرمایه گذاری در طلا': 'صندوق سرمایه گذاری', # Gold Fund
    'صندوق: سهامي اهرمي': 'صندوق سرمایه گذاری', # NEW: User requested
    'نوع صندوق: سهامي': 'صندوق سرمایه گذاری', # NEW: User requested
    'صندوق سهامي': 'صندوق سرمایه گذاری', # NEW: General stock fund
    'صندوق اهرمي': 'صندوق سرمایه گذاری', # NEW: Leveraged fund
    'صندوق با درآمد ثابت': 'صندوق سرمایه گذاری', # NEW: Fixed income fund
    'صندوق مختلط': 'صندوق سرمایه گذاری', # NEW: Mixed fund
    'صندوق بازارگردانی': 'صندوق سرمایه گذاری', # NEW: Market making fund
    'صندوق پروژه': 'صندوق سرمایه گذاری', # NEW: Project fund
    'صندوق طلا': 'صندوق سرمایه گذاری', # NEW: Gold fund
    'صندوق کالایی': 'صندوق سرمایه گذاری', # NEW: Commodity fund
    'صندوق جسورانه': 'صندوق سرمایه گذاری', # NEW: Venture fund
    'صندوق زمین و ساختمان': 'صندوق سرمایه گذاری', # NEW: Real estate fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت': 'صندوق سرمایه گذاری', # NEW: Fixed income securities fund
    'صندوق سرمایه گذاری در اوراق بهادار رهنی': 'صندوق سرمایه گذاری', # NEW: Mortgage-backed securities fund
    'صندوق سرمایه گذاری در اوراق بهادار مبتنی بر کالا': 'صندوق سرمایه گذاری', # NEW: Commodity-backed securities fund
    'صندوق سرمایه گذاری در صندوق‌های سرمایه گذاری': 'صندوق سرمایه گذاری', # NEW: Fund of funds
    'صندوق سرمایه گذاری در صندوق‌های سرمایه گذاری قابل معامله': 'صندوق سرمایه گذاری', # NEW: Fund of ETFs
    'صندوق سرمایه گذاری در سهام و حق تقدم سهام': 'صندوق سرمایه گذاری', # NEW: Stock and rights fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و سهام': 'صندوق سرمایه گذاری', # NEW: Fixed income and stock fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و مختلط': 'صندوق سرمایه گذاری', # NEW: Fixed income and mixed fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا': 'صندوق سرمایه گذاری', # NEW: Fixed income and gold fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و کالا': 'صندوق سرمایه گذاری', # NEW: Fixed income and commodity fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و جسورانه': 'صندوق سرمایه گذاری', # NEW: Fixed income and venture fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و زمین و ساختمان': 'صندوق سرمایه گذاری', # NEW: Fixed income and real estate fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازارگردانی': 'صندوق سرمایه گذاری', # NEW: Fixed income and market making fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و پروژه': 'صندوق سرمایه گذاری', # NEW: Fixed income and project fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس کالا': 'صندوق سرمایه گذاری', # NEW: Fixed income and commodity exchange fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا': 'صندوق سرمایه گذاری', # NEW: Fixed income and gold fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و حق تقدم': 'صندوق سرمایه گذاری', # NEW: Fixed income and rights fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اختیار معامله': 'صندوق سرمایه گذاری', # NEW: Fixed income and options fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و آتی': 'صندوق سرمایه گذاری', # NEW: Fixed income and futures fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صکوک': 'صندوق سرمایه گذاری', # NEW: Fixed income and sukuk fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و گواهی سپرده': 'صندوق سرمایه گذاری', # NEW: Fixed income and deposit certificate fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق مشارکت': 'صندوق سرمایه گذاری', # NEW: Fixed income and participation paper fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق اجاره': 'صندوق سرمایه گذاری', # NEW: Fixed income and lease paper fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق تامین مالی': 'صندوق سرمایه گذاری', # NEW: Fixed income and financing paper fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار ابزارهای نوین مالی': 'صندوق سرمایه گذاری', # NEW: Fixed income and new financial instruments market fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار ابزارهای مالی نوین': 'صندوق سرمایه گذاری', # NEW: Fixed income and new financial instruments market fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس کالا و انرژی': 'صندوق سرمایه گذاری', # NEW: Fixed income and commodity and energy exchange fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس اوراق بهادار تهران': 'صندوق سرمایه گذاری', # NEW: Fixed income and Tehran Stock Exchange fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و فرابورس ایران': 'صندوق سرمایه گذاری', # NEW: Fixed income and Iran Fara Bourse fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار پایه': 'صندوق سرمایه گذاری', # NEW: Fixed income and base market fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و عمومی': 'صندوق سرمایه گذاری', # NEW: Fixed income and general fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و نامشخص': 'صندوق سرمایه گذاری', # NEW: Fixed income and unknown fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و سهام اهرمي': 'صندوق سرمایه گذاری', # NEW: Fixed income and leveraged stock fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و نوع صندوق: سهامي': 'صندوق سرمایه گذاری', # NEW: Fixed income and stock fund type
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سهامي': 'صندوق سرمایه گذاری', # NEW: Fixed income and stock fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق اهرمي': 'صندوق سرمایه گذاری', # NEW: Fixed income and leveraged fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق با درآمد ثابت': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق مختلط': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and mixed fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق بازارگردانی': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and market making fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق پروژه': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and project fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق طلا': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and gold fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق کالایی': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and commodity fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق جسورانه': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and venture fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق زمین و ساختمان': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and real estate fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در اوراق بهادار': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and securities fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در صندوق': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and fund of funds
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری جسورانه': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and venture fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری زمین و ساختمان': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and real estate fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری اختصاصی بازارگردانی': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and market making fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری پروژه': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and project fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در بورس کالا': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and commodity exchange fund
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا': 'صندوق سرمایه گذاری', # NEW: Fixed income and fixed income and gold fund
}


def _extract_market_type_from_loader_html(html_content):
    """
    Extracts the market type string from the HTML content of a Loader.aspx page.
    Prioritizes specific IDs/classes, then more generic text searches.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    
    # --- Priority 1: Specific IDs/Classes (most reliable if they exist) ---
    market_span_id = soup.find('span', id='MainContent_lblMarketName')
    if market_span_id and market_span_id.text.strip():
        market_type_str = market_span_id.text.strip()
        logger.debug(f"HTML Parser: Found market type from span ID 'MainContent_lblMarketName': '{market_type_str}'")
        return HTML_MARKET_TYPE_MAP.get(market_type_str, market_type_str)

    # Try to find a td with class 'lbl' and text 'بازار:' and then its sibling 'td' with class 'value'
    market_label_td = soup.find('td', class_='lbl', string='بازار:')
    if market_label_td:
        market_value_td = market_label_td.find_next_sibling('td', class_='value')
        if market_value_td and market_value_td.text.strip():
            market_type_str = market_value_td.text.strip()
            logger.debug(f"HTML Parser: Found market type from 'بازار:' label and sibling 'value' td: '{market_type_str}'")
            return HTML_MARKET_TYPE_MAP.get(market_type_str, market_type_str)

    # --- Priority 2: Search for common market type indicators within the entire page text ---
    # This is a broader search, might catch text within various tags or even plain text.
    # Order matters here: more specific terms first.
    common_market_indicators_ordered = [
    'صندوق سرمایه گذاری',
    'صندوق قابل معامله',
    'صندوق های قابل معامله',
    'صندوقهای سرمایه گذاری',
    'بورس کالا',
    'بورس انرژی',
    'فرابورس',
    'بورس اوراق بهادار',
    'اوراق با درآمد ثابت',
    'اختیار معامله',
    'صکوک',
    'گواهی سپرده',
    'حق تقدم',
    'بازار پایه',
    'مشتقه',
    'سهام',
    'اوراق بهادار',
    'اوراق تامین مالی',
    'اوراق مشارکت',
    'گواهی سپرده کالایی',
    'صندوق سرمایه گذاری مشترک',
    'صندوق سرمایه گذاری قابل معامله',
    'صندوق سرمایه گذاری در سهام',
    'صندوق سرمایه گذاری مختلط',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت',
    'صندوق سرمایه گذاری طلا',
    'اوراق اجاره',
    'بورس کالا و انرژی',
    'بورس اوراق بهادار تهران',
    'فرابورس ایران',
    'بازار ابزارهای نوین مالی',
    'بازار ابزارهای مالی نوین',
    'صندوق سرمایه گذاری در اوراق بهادار',
    'صندوق سرمایه گذاری در صندوق',
    'صندوق سرمایه گذاری جسورانه',
    'صندوق سرمایه گذاری زمین و ساختمان',
    'صندوق سرمایه گذاری اختصاصی بازارگردانی',
    'صندوق سرمایه گذاری پروژه',
    'صندوق سرمایه گذاری در بورس کالا',
    'صندوق سرمایه گذاری در طلا',
    'صندوق سهامي اهرمي',
    'نوع صندوق: سهامي',
    'صندوق سهامي',
    'صندوق اهرمي',
    'صندوق با درآمد ثابت',
    'صندوق مختلط',
    'صندوق بازارگردانی',
    'صندوق پروژه',
    'صندوق طلا',
    'صندوق کالایی',
    'صندوق جسورانه',
    'صندوق زمین و ساختمان',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت',
    'صندوق سرمایه گذاری در اوراق بهادار رهنی',
    'صندوق سرمایه گذاری در اوراق بهادار مبتنی بر کالا',
    'صندوق سرمایه گذاری در صندوق‌های سرمایه گذاری',
    'صندوق سرمایه گذاری در صندوق‌های سرمایه گذاری قابل معامله',
    'صندوق سرمایه گذاری در سهام و حق تقدم سهام',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و سهام',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و مختلط',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و کالا',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و جسورانه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و زمین و ساختمان',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازارگردانی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و پروژه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس کالا',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و حق تقدم',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اختیار معامله',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و آتی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صکوک',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و گواهی سپرده',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق مشارکت',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق اجاره',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق تامین مالی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار ابزارهای نوین مالی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار ابزارهای مالی نوین',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس کالا و انرژی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس اوراق بهادار تهران',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و فرابورس ایران',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار پایه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و عمومی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و نامشخص',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و سهام اهرمي',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و نوع صندوق: سهامي',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سهامي',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق اهرمي',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق با درآمد ثابت',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق مختلط',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق بازارگردانی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق پروژه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق طلا',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق کالایی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق جسورانه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق زمین و ساختمان',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در اوراق بهادار',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در صندوق',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری جسورانه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری زمین و ساختمان',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری اختصاصی بازارگردانی',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری پروژه',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در بورس کالا',
    'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا',
]
    
    # A simplified version of a broad search that looks for any of the common market type strings
    # in the text content of the HTML body.
    body_text = soup.get_text()
    for indicator in common_market_indicators_ordered:
        if indicator in body_text:
            market_type_str = indicator.strip()
            logger.debug(f"HTML Parser: Found market type from broad text search: '{market_type_str}'")
            return HTML_MARKET_TYPE_MAP.get(market_type_str, market_type_str)

    logger.warning("HTML Parser: Could not determine market type from HTML content.")
    return 'نامشخص' # Return a default 'unspecified' market type if nothing is found


def _fetch_page_content(symbol_id):
    """
    Fetches the HTML content for a given symbol_id from TSETMC.
    Uses a standard user-agent to mimic a browser.
    """
    url = f'http://www.tsetmc.com/Loader.aspx?ParTree=111311&i={symbol_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }
    try:
        response = http_get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        # TSETMC content is typically in utf-8, but we can verify
        content = response.content.decode('utf-8')
        return content
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching page for symbol ID {symbol_id}: {e}")
        return None


def get_market_type_for_symbol(symbol_id, symbol_name):
    """
    Determines the market type for a given symbol by first trying pytse_client,
    then falling back to HTML scraping if pytse_client doesn't provide it.
    """
    try:
        # Step 1: Try to get market type from pytse_client
        ticker = Ticker(symbol_name)
        if hasattr(ticker, 'flow') and isinstance(ticker.flow, int):
            market_type = MARKET_TYPE_MAP.get(ticker.flow, 'نامشخص')
            logger.debug(f"pytse_client: Determined market type for {symbol_name} (ID: {symbol_id}) as '{market_type}' from flow code {ticker.flow}.")
            return market_type
        
        # Fallback to HTML scraping if pytse_client doesn't have the flow code or it's not useful
        logger.info(f"pytse_client: No usable flow code for {symbol_name} (ID: {symbol_id}). Falling back to HTML scraping.")
        html_content = _fetch_page_content(symbol_id)
        if html_content:
            return _extract_market_type_from_loader_html(html_content)
        else:
            logger.warning(f"Could not fetch HTML content for {symbol_name} (ID: {symbol_id}). Market type cannot be determined.")
            return 'نامشخص'
    except Exception as e:
        logger.error(f"Error in get_market_type_for_symbol for {symbol_name} (ID: {symbol_id}): {e}")
        return 'نامشخص'


def populate_all_symbols_initial():
    """
    Populates the ComprehensiveSymbolData table with all unique symbols from TSETMC.
    This function should be run as a one-time initial population.
    It fetches all tickers and then determines their market type.
    It returns the number of symbols added and a message.
    """
    try:
        current_app.logger.info("Starting initial population of all symbols.")
        tickers = all_tickers()
        if not tickers:
            return 0, "No tickers found from pytse_client."

        added_count = 0
        total_symbols = len(tickers)
        for i, (symbol_name, ticker_obj) in enumerate(tickers.items()):
            symbol_id = ticker_obj.get_tse_id()
            if not symbol_id:
                current_app.logger.warning(f"Skipping symbol '{symbol_name}' as it has no TSE ID.")
                continue

            # Check if symbol already exists
            existing_symbol = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()
            if not existing_symbol:
                # Determine market type for the new symbol
                market_type = get_market_type_for_symbol(symbol_id, symbol_name)
                
                new_symbol = ComprehensiveSymbolData(
                    symbol_id=symbol_id,
                    symbol_name=symbol_name,
                    market_type=market_type,
                    is_active=True
                )
                db.session.add(new_symbol)
                added_count += 1
                if added_count % 100 == 0:
                    current_app.logger.info(f"Processed {added_count}/{total_symbols} symbols. Committing so far...")
                    db.session.commit()
            
            # Log progress
            if (i + 1) % 50 == 0:
                current_app.logger.info(f"Progress: {i+1}/{total_symbols} symbols checked.")

        db.session.commit()
        final_message = f"Initial symbol population finished. {added_count} new symbols added to the database."
        current_app.logger.info(final_message)
        return added_count, final_message
    except Exception as e:
        db.session.rollback()
        error_message = f"An error occurred during initial symbol population: {e}"
        current_app.logger.error(error_message)
        return 0, error_message


def _fetch_historical_data(symbol_name, days_limit=None):
    """
    Fetches historical data for a given symbol using pytse_client.
    Returns a pandas DataFrame.
    """
    try:
        # Use a reasonable days_limit if not specified to prevent overly long requests
        days = days_limit if days_limit is not None else 365
        
        # Get data with the specified number of days
        df = download(symbols=[symbol_name], write_to_csv=False, adjust=True, days_limit=days)
        
        if df.empty or symbol_name not in df.columns.get_level_values(0):
            logger.warning(f"No historical data found for {symbol_name} in the last {days} days.")
            return pd.DataFrame()

        # Clean up the DataFrame
        df.columns = df.columns.get_level_values(1)
        df = df.reset_index().rename(columns={'index': 'date'})
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate `close` price if `adj_close` is available.
        # This is important as some indicators use `close` and some use `adj_close`.
        # pytse_client's `close` is adjusted already, but we will add a raw `close` for clarity.
        # However, the library behavior seems to provide an adjusted close, so we will use it as is.
        # The column is named 'adj_close' by the library, which is what we need.
        df.rename(columns={'adj_close': 'close'}, inplace=True)
        
        # Add a new column for `value` which is `close * volume` for later calculations
        df['value'] = df['close'] * df['volume']

        # Ensure all required columns are present. If not, fill with 0s or NaNs.
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'value']
        for col in required_cols:
            if col not in df.columns:
                df[col] = np.nan
        
        df = df[required_cols]
        logger.debug(f"Fetched historical data for {symbol_name}: {df.shape[0]} rows.")
        return df
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol_name}: {e}")
        return pd.DataFrame()


def _update_or_create_historical_data(symbol_id, symbol_name, df, session):
    """
    Updates or creates historical data records for a given symbol from a DataFrame.
    """
    if df.empty:
        return 0, f"No data to update for {symbol_name}."

    added_count = 0
    updated_count = 0

    try:
        # Determine the last recorded date for the symbol
        last_date = session.query(func.max(HistoricalData.date)).filter_by(symbol_id=symbol_id).scalar()
        
        # Filter the DataFrame to only include new data
        if last_date:
            df = df[df['date'] > last_date]
        
        if df.empty:
            return 0, f"Historical data for {symbol_name} is already up to date."

        # Process and add new records
        for index, row in df.iterrows():
            gregorian_date = row['date'].date()
            jalali_date = convert_gregorian_to_jalali(gregorian_date)
            
            new_record = HistoricalData(
                symbol_id=symbol_id,
                date=gregorian_date,
                jalali_date=jalali_date,
                open_price=normalize_value(row['open']),
                high_price=normalize_value(row['high']),
                low_price=normalize_value(row['low']),
                close_price=normalize_value(row['close']),
                volume=normalize_value(row['volume']),
                value=normalize_value(row['value']),
                count=normalize_value(row.get('count', 0)) # Assuming 'count' might not always be present
            )
            session.add(new_record)
            added_count += 1
        
        session.commit()
        
        return added_count, f"Historical data for {symbol_name} updated successfully. {added_count} new records added, {updated_count} records updated."
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating historical data for {symbol_name}: {e}")
        return 0, f"An error occurred while updating historical data for {symbol_name}."


def update_historical_data_for_symbol(symbol_id, symbol_name, days_limit=365):
    """
    Fetches and updates historical data for a single symbol.
    """
    try:
        logger.info(f"Updating historical data for {symbol_name}...")
        df = _fetch_historical_data(symbol_name, days_limit)
        added_count, msg = _update_or_create_historical_data(symbol_id, symbol_name, df, db.session)
        logger.info(f"Historical data update for {symbol_name}: {msg}")
        return True, msg
    except Exception as e:
        logger.error(f"Full historical update failed for {symbol_name}: {e}")
        return False, f"Full historical update failed due to an internal error."


def _update_technical_indicators(symbol_id, symbol_name, days_limit=365, session=db.session):
    """
    Calculates and updates technical indicators for a given symbol.
    """
    try:
        logger.info(f"Calculating and updating technical indicators for {symbol_name}...")
        
        # Fetch existing historical data from the database
        query = session.query(HistoricalData).filter_by(symbol_id=symbol_id).order_by(HistoricalData.date)
        
        # Apply a days_limit to the query if it's specified, to prevent fetching too much data
        if days_limit is not None:
            # We need enough data to calculate indicators (e.g., 200 days for long-term SMA)
            # Fetch a bit more than the days_limit to ensure calculations are correct
            start_date = date.today() - timedelta(days=days_limit + 100) # Buffer of 100 days
            query = query.filter(HistoricalData.date >= start_date)
            
        historical_records = query.all()
        
        if not historical_records:
            logger.warning(f"No historical data found for {symbol_name} to calculate indicators.")
            return 0, f"No historical data to calculate technical indicators for {symbol_name}."

        # Convert records to a DataFrame for easier calculation
        df = pd.DataFrame([rec.__dict__ for rec in historical_records])
        df = df.sort_values('date')
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate indicators
        df['rsi'] = calculate_rsi(df['close_price'])
        df['macd'], df['macd_signal'] = calculate_macd(df['close_price'])
        df['sma_20'] = calculate_sma(df['close_price'], window=20)
        df['sma_50'] = calculate_sma(df['close_price'], window=50)
        df['sma_200'] = calculate_sma(df['close_price'], window=200)
        df['bollinger_upper'], df['bollinger_middle'], df['bollinger_lower'] = calculate_bollinger_bands(df['close_price'])
        df['volume_ma'] = calculate_volume_ma(df['volume'])
        df['atr'] = calculate_atr(df['high_price'], df['low_price'], df['close_price'])
        df['smf'] = calculate_smart_money_flow(df['close_price'], df['volume'])

        # Now, update the database with the calculated indicators
        added_count = 0
        
        # Get the last date for technical indicators to update only new ones
        last_indicator_date = session.query(func.max(TechnicalIndicatorData.date)).filter_by(symbol_id=symbol_id).scalar()
        
        # Filter the DataFrame to only include new data points for indicators
        if last_indicator_date:
            df = df[df['date'].dt.date > last_indicator_date]
        
        if df.empty:
            return 0, f"Technical indicators for {symbol_name} are already up to date."

        for index, row in df.iterrows():
            gregorian_date = row['date'].date()
            jalali_date = convert_gregorian_to_jalali(gregorian_date)
            
            # Use `merge` to check for existing record and update, otherwise add new one
            existing_indicator = session.query(TechnicalIndicatorData).filter_by(symbol_id=symbol_id, date=gregorian_date).first()
            
            if existing_indicator:
                # Update existing record
                existing_indicator.rsi = row['rsi']
                existing_indicator.macd = row['macd']
                existing_indicator.macd_signal = row['macd_signal']
                existing_indicator.sma_20 = row['sma_20']
                existing_indicator.sma_50 = row['sma_50']
                existing_indicator.sma_200 = row['sma_200']
                existing_indicator.bollinger_upper = row['bollinger_upper']
                existing_indicator.bollinger_middle = row['bollinger_middle']
                existing_indicator.bollinger_lower = row['bollinger_lower']
                existing_indicator.volume_ma = row['volume_ma']
                existing_indicator.atr = row['atr']
                existing_indicator.smf = row['smf']
                # added_count += 1 # We're only adding, not updating for now.
            else:
                # Add new record
                new_indicator = TechnicalIndicatorData(
                    symbol_id=symbol_id,
                    date=gregorian_date,
                    jalali_date=jalali_date,
                    rsi=row['rsi'],
                    macd=row['macd'],
                    macd_signal=row['macd_signal'],
                    sma_20=row['sma_20'],
                    sma_50=row['sma_50'],
                    sma_200=row['sma_200'],
                    bollinger_upper=row['bollinger_upper'],
                    bollinger_middle=row['bollinger_middle'],
                    bollinger_lower=row['bollinger_lower'],
                    volume_ma=row['volume_ma'],
                    atr=row['atr'],
                    smf=row['smf']
                )
                session.add(new_indicator)
                added_count += 1
        
        session.commit()
        return added_count, f"Technical indicators for {symbol_name} updated successfully. {added_count} new records added."
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating technical indicators for {symbol_name}: {e}")
        return 0, f"An error occurred while updating technical indicators for {symbol_name}."


def update_technical_data_for_symbol(symbol_id, symbol_name, days_limit=365):
    """
    Wrapper to calculate and update technical data for a single symbol.
    """
    try:
        added_count, msg = _update_technical_indicators(symbol_id, symbol_name, days_limit, db.session)
        return True, msg
    except Exception as e:
        logger.error(f"Full technical data update failed for {symbol_name}: {e}")
        return False, f"Full technical data update failed due to an internal error."


def _get_fundamental_data_from_tsetmc(symbol_id, session):
    """
    Fetches fundamental data from TSETMC's html page for a specific symbol.
    Parses key financial metrics.
    Returns a dict of parsed data or None on failure.
    """
    url = f'http://www.tsetmc.com/Loader.aspx?ParTree=111C1411&i={symbol_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        data = {}
        
        # --- Parsing the "اثرات مالی" (Financial effects) table ---
        financial_table_div = soup.find('div', id='MainContent_C2P')
        if financial_table_div:
            table = financial_table_div.find('table')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    cols = [ele.text.strip() for ele in cols]
                    
                    # The table has multiple rows with different data
                    # Let's map based on the label in the first column
                    if 'سود هر سهم' in cols[0]:
                        try:
                            # EPS is in the 2nd column
                            data['eps'] = float(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['eps'] = None
                    elif 'P/E گروه' in cols[0]:
                        try:
                            data['pe_group'] = float(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['pe_group'] = None
                    elif 'P/E' in cols[0]:
                        try:
                            data['pe_ratio'] = float(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['pe_ratio'] = None
                    elif 'نسبت P/B' in cols[0]:
                        try:
                            data['pb_ratio'] = float(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['pb_ratio'] = None
                    elif 'نسبت P/S' in cols[0]:
                        try:
                            data['ps_ratio'] = float(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['ps_ratio'] = None
                    elif 'حجم مبنا' in cols[0]:
                        try:
                            # حجم مبنا is an integer, but sometimes has commas
                            data['base_volume'] = int(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['base_volume'] = None

        # --- Parsing the "مشخصات" (Specifications) and other divs ---
        # These are usually in `div`s with specific `id`s. We can iterate and find them.
        specs_div = soup.find('div', id='MainContent_C1P')
        if specs_div:
            table = specs_div.find('table')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    cols = [ele.text.strip() for ele in cols]

                    if 'قیمت پایانی' in cols[0]:
                        try:
                            data['closing_price'] = float(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['closing_price'] = None
                    elif 'تعداد سهام' in cols[0]:
                        try:
                            data['total_shares'] = int(cols[1].replace(',', ''))
                        except (ValueError, IndexError):
                            data['total_shares'] = None

        # --- Parsing data from the main header area (usually has a table or spans) ---
        main_header_table = soup.find('table', class_='InfoTbl')
        if main_header_table:
            # Finding the market value (ارزش بازار)
            market_value_row = main_header_table.find('tr', string=lambda text: 'ارزش بازار' in text)
            if market_value_row:
                try:
                    market_value_td = market_value_row.find_next_sibling('td')
                    if market_value_td:
                        data['market_value'] = int(market_value_td.text.strip().replace(',', ''))
                except (ValueError, IndexError, AttributeError):
                    data['market_value'] = None
        
        # We can also parse from the JavaScript data at the top of the page.
        # This is a more robust approach for real-time data.
        # Let's find the `t111C1411_t1` object from script tags.
        script_tag = soup.find('script', text=lambda text: 't111C1411_t1=' in text)
        if script_tag:
            js_code = script_tag.string
            # A simple regex to extract JSON-like data. Not perfect, but can work.
            import re
            match = re.search(r'var t111C1411_t1=(.+?);', js_code)
            if match:
                js_data = match.group(1)
                # This is a raw JavaScript object. It needs careful parsing.
                # A simple and fragile way is to use eval, but that's a security risk.
                # A safer way is to replace single quotes with double quotes and parse it.
                # We'll skip this for now as it's complex and the HTML parsing is sufficient.
        
        # Check if we have enough data
        if not data:
            logger.warning(f"Fundamental data extraction failed for symbol ID {symbol_id}. No data found.")
            return None
            
        logger.debug(f"Successfully fetched fundamental data for {symbol_id}: {data}")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching fundamental data for symbol ID {symbol_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing fundamental data for symbol ID {symbol_id}: {e}")
        return None


def _update_or_create_fundamental_data(symbol_id, data, session):
    """
    Updates or creates a fundamental data record for a given symbol.
    """
    if not data:
        return False, "No data to update."

    try:
        # Check if a record for today already exists
        today = date.today()
        existing_record = session.query(FundamentalData).filter_by(symbol_id=symbol_id, date=today).first()

        if existing_record:
            # Update existing record
            existing_record.eps = data.get('eps')
            existing_record.pe_ratio = data.get('pe_ratio')
            existing_record.pe_group = data.get('pe_group')
            existing_record.pb_ratio = data.get('pb_ratio')
            existing_record.ps_ratio = data.get('ps_ratio')
            existing_record.base_volume = data.get('base_volume')
            existing_record.closing_price = data.get('closing_price')
            existing_record.total_shares = data.get('total_shares')
            existing_record.market_value = data.get('market_value')
            logger.debug(f"Updated fundamental data for {symbol_id} for today.")
        else:
            # Create a new record
            jalali_date = convert_gregorian_to_jalali(today)
            new_record = FundamentalData(
                symbol_id=symbol_id,
                date=today,
                jalali_date=jalali_date,
                eps=data.get('eps'),
                pe_ratio=data.get('pe_ratio'),
                pe_group=data.get('pe_group'),
                pb_ratio=data.get('pb_ratio'),
                ps_ratio=data.get('ps_ratio'),
                base_volume=data.get('base_volume'),
                closing_price=data.get('closing_price'),
                total_shares=data.get('total_shares'),
                market_value=data.get('market_value')
            )
            session.add(new_record)
            logger.debug(f"Added new fundamental data record for {symbol_id} for today.")
        
        session.commit()
        return True, "Fundamental data updated successfully."
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating fundamental data for {symbol_id}: {e}")
        return False, "An error occurred while updating fundamental data."



def analyze_technical_data_for_symbol(symbol_id, symbol_name, limit_days=120):
    """
    Analyzes technical indicators for a given symbol based on historical data
    and updates the TechnicalIndicatorData table.
    
    Args:
        symbol_id (str): The ID of the symbol.
        symbol_name (str): The name of the symbol.
        limit_days (int): Number of days to fetch historical data for technical analysis.
        
    Returns:
        Tuple[bool, str]: True and a success message, or False and an error message.
    """
    logger.info(f"Analyzing technical data for {symbol_name} ({symbol_id}).")
    try:
        # Fetch historical data for technical analysis
        # Ensure we fetch enough data for indicators (e.g., SMA_50 needs 50 days lookback)
        historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id)\
                                                .order_by(HistoricalData.jdate.asc())\
                                                .limit(limit_days).all()

        if not historical_records:
            logger.warning(f"No historical data found for technical analysis for {symbol_name} ({symbol_id}).")
            return False, f"No historical data for {symbol_name}."

        hist_df = pd.DataFrame([rec.__dict__ for rec in historical_records]).drop(columns=['_sa_instance_state'], errors='ignore')
        
        # Ensure 'jdate' is properly converted for sorting and calculations
        hist_df['gregorian_date'] = hist_df['jdate'].apply(
            lambda x: jdatetime.date(*map(int, x.split('-'))).togregorian() if pd.notna(x) and isinstance(x, str) else pd.NaT
        )
        hist_df = hist_df.sort_values(by='gregorian_date', ascending=True).reset_index(drop=True)
        hist_df = hist_df.dropna(subset=['gregorian_date']) # Drop rows with invalid dates

        # Ensure numeric columns are indeed numeric and fill NaNs
        numeric_cols = ['close', 'open', 'high', 'low', 'volume', 'final']
        for col in numeric_cols:
            if col in hist_df.columns:
                hist_df[col] = pd.to_numeric(hist_df[col], errors='coerce')
                # Replace NaN with 0 after conversion
                hist_df[col] = hist_df[col].replace([np.inf, -np.inf], np.nan).fillna(0)
            else:
                logger.warning(f"Column '{col}' not found in historical data for {symbol_name}. This may affect indicator calculations.")
                hist_df[col] = 0 # Add column with zeros if missing

        # Calculate indicators
        hist_df['RSI'] = calculate_rsi(hist_df['close'])
        macd, macd_signal, macd_hist = calculate_macd(hist_df['close'])
        hist_df['MACD'] = macd
        hist_df['MACD_Signal'] = macd_signal
        hist_df['MACD_Hist'] = macd_hist
        hist_df['SMA_20'] = calculate_sma(hist_df['close'], window=20)
        hist_df['SMA_50'] = calculate_sma(hist_df['close'], window=50)
        hist_df['Volume_MA_20'] = calculate_volume_ma(hist_df['volume'], window=20)
        
        # Bollinger Bands need enough data (typically 20 periods)
        if len(hist_df) >= 20:
            hist_df['Bollinger_MA'], hist_df['Bollinger_High'], hist_df['Bollinger_Low'] = calculate_bollinger_bands(hist_df['close'], window=20)
        else:
            hist_df['Bollinger_MA'] = np.nan
            hist_df['Bollinger_High'] = np.nan
            hist_df['Bollinger_Low'] = np.nan
            logger.warning(f"Not enough data for Bollinger Bands for {symbol_name}. Setting to NaN.")
        
        # ATR (Average True Range) - Ensure 'high', 'low', 'close' are available and numeric
        if len(hist_df) > 1: # ATR needs at least previous close
            hist_df['ATR'] = calculate_atr(hist_df['high'], hist_df['low'], hist_df['close'])
        else:
            hist_df['ATR'] = np.nan
            logger.warning(f"Not enough data for ATR for {symbol_name}. Setting to NaN.")

        # --- MODIFIED: Iterate and save all calculated technical indicator rows ---
        processed_tech_rows = 0
        for index, row in hist_df.iterrows():
            current_jdate_str = row['jdate']
            
            # Ensure symbol_id is consistent
            db_symbol_id = get_symbol_id(symbol_id)
            if not db_symbol_id:
                logger.warning(f"Resolved symbol_id not found for {symbol_id}. Skipping technical data update for this row.")
                continue

            existing_record = TechnicalIndicatorData.query.filter_by(
                symbol_id=db_symbol_id,
                jdate=current_jdate_str
            ).first()

            record_data = {
                'symbol_id': db_symbol_id,
                'jdate': current_jdate_str,
                'close_price': float(row.get('close')) if pd.notna(row.get('close')) else 0.0,
                'RSI': float(row.get('RSI')) if pd.notna(row.get('RSI')) else 0.0,
                'MACD': float(row.get('MACD')) if pd.notna(row.get('MACD')) else 0.0,
                'MACD_Signal': float(row.get('MACD_Signal')) if pd.notna(row.get('MACD_Signal')) else 0.0,
                'MACD_Hist': float(row.get('MACD_Hist')) if pd.notna(row.get('MACD_Hist')) else 0.0,
                'SMA_20': float(row.get('SMA_20')) if pd.notna(row.get('SMA_20')) else 0.0,
                'SMA_50': float(row.get('SMA_50')) if pd.notna(row.get('SMA_50')) else 0.0,
                'Volume_MA_20': float(row.get('Volume_MA_20')) if pd.notna(row.get('Volume_MA_20')) else 0.0,
                'Bollinger_High': float(row.get('Bollinger_High')) if pd.notna(row.get('Bollinger_High')) else 0.0,
                'Bollinger_Low': float(row.get('Bollinger_Low')) if pd.notna(row.get('Bollinger_Low')) else 0.0,
                'Bollinger_MA': float(row.get('Bollinger_MA')) if pd.notna(row.get('Bollinger_MA')) else 0.0,
                'ATR': float(row.get('ATR')) if pd.notna(row.get('ATR')) else 0.0,
            }

            if existing_record:
                for key, value in record_data.items():
                    setattr(existing_record, key, value)
                existing_record.updated_at = datetime.now()
                db.session.add(existing_record)
            else:
                new_record = TechnicalIndicatorData(
                    **record_data
                )
                db.session.add(new_record)
            processed_tech_rows += 1

        db.session.commit()
        logger.info(f"Successfully updated/added {processed_tech_rows} technical indicator data rows for {symbol_name}.")
        return True, f"Successfully analyzed and saved {processed_tech_rows} technical data rows for {symbol_name}."

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error analyzing technical data for {symbol_name} ({symbol_id}): {e}", exc_info=True)
        return False, f"Error analyzing technical data for {symbol_name}: {str(e)}"




def update_comprehensive_data_for_symbol(symbol_id, symbol_name):
    """
    Fetches and updates fundamental data for a single symbol.
    """
    try:
        logger.info(f"Updating fundamental data for {symbol_name}...")
        data = _get_fundamental_data_from_tsetmc(symbol_id, db.session)
        if not data:
            return False, "Failed to fetch fundamental data from source."
        success, msg = _update_or_create_fundamental_data(symbol_id, data, db.session)
        logger.info(f"Fundamental data update for {symbol_name}: {msg}")
        return success, msg
    except Exception as e:
        logger.error(f"Full fundamental data update failed for {symbol_name}: {e}")
        return False, f"Full fundamental data update failed due to an internal error."


def run_full_data_update(days_limit=120):
    """
    Runs a full data update for all symbols: historical, technical, and fundamental.
    This should be run periodically (e.g., daily).
    
    Args:
        days_limit (int): Number of days to fetch historical data for each symbol.
        
    Returns:
        Tuple[int, str]: Total processed count and a summary message.
    """
    logger.info(f"Starting full data update for all symbols for the last {days_limit} days.")
    
    try:
        symbols_to_process = ComprehensiveSymbolData.query.all()
        
        if not symbols_to_process:
            logger.warning("No symbols found in ComprehensiveSymbolData. Please run initial population first.")
            return 0, "No symbols to process."

        total_processed_count = 0
        
        for symbol in symbols_to_process:
            # 1. Update Historical Data
            # آرگومان 'limit_days' به 'days_limit' تغییر یافت تا با تابع update_historical_data_for_symbol هماهنگ شود
            success_hist, msg_hist = update_historical_data_for_symbol(symbol.symbol_id, symbol.symbol_name, days_limit=days_limit)
            if success_hist:
                total_processed_count += 1
                logger.info(f"Historical data update for {symbol.symbol_name}: {msg_hist}")
            else:
                logger.warning(f"Failed historical data update for {symbol.symbol_name}: {msg_hist}")

            # 2. Analyze Technical Data
            # فراخوانی تابع analyze_technical_data_for_symbol با آرگومان days_limit
            success_tech, msg_tech = analyze_technical_data_for_symbol(symbol.symbol_id, symbol.symbol_name, limit_days=days_limit)
            if success_tech:
                total_processed_count += 1
                logger.info(f"Technical analysis for {symbol.symbol_name}: {msg_tech}")
            else:
                logger.warning(f"Failed technical data analysis for {symbol.symbol_name}: {msg_tech}")

            # 3. Update Fundamental Data (using the comprehensive update function)
            success_fund, msg_fund = update_comprehensive_data_for_symbol(symbol.symbol_id, symbol.symbol_name)
            if success_fund:
                total_processed_count += 1
                logger.info(f"Fundamental data update for {symbol.symbol_name}: {msg_fund}")
            else:
                logger.warning(f"Failed fundamental data update for {symbol.symbol_name}: {msg_fund}")

        final_message = f"Full data update summary: Total processed operations: {total_processed_count}. Check logs for details on each symbol."
        current_app.logger.info(final_message)
        return total_processed_count, final_message

    except Exception as e:
        logger.error(f"Error during full data update: {e}", exc_info=True)
        return 0, f"An error occurred during the full data update process: {e}"


def initial_populate_all_symbols_and_data():
    """
    Initial population of ComprehensiveSymbolData and then fetches historical/technical/fundamental data for them.
    This should be run once to seed the database.
    """
    current_app.logger.info("Starting initial population of all symbols and their data.")
    
    total_comp_symbols_added, msg_comp = populate_all_symbols_initial()
    current_app.logger.info(msg_comp)

    # After initial population of symbols, run a full data update for them
    # Use a larger days_limit for initial population
    processed_count, msg_data_update = run_full_data_update(days_limit=365) 
    current_app.logger.info(msg_data_update)

    final_message = f"Initial population process finished. Added {total_comp_symbols_added} new symbols and updated data for all symbols. Total data update operations: {processed_count}."
    current_app.logger.info(final_message)
    return total_comp_symbols_added, processed_count, final_message


def update_and_get_historical_data(symbol_id, symbol_name):
    """
    Updates historical data for a symbol and returns all of its historical data.
    """
    update_historical_data_for_symbol(symbol_id, symbol_name)
    
    historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.date).all()
    
    data_points = []
    for record in historical_records:
        data_points.append({
            'date': str(record.date),
            'open': record.open_price,
            'high': record.high_price,
            'low': record.low_price,
            'close': record.close_price,
            'volume': record.volume,
            'value': record.value,
            'count': record.count
        })
    
    return data_points


def update_and_get_technical_indicators(symbol_id, symbol_name):
    """
    Updates technical indicators for a symbol and returns all of its technical data.
    """
    update_technical_data_for_symbol(symbol_id, symbol_name)
    
    technical_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id).order_by(TechnicalIndicatorData.date).all()

    data_points = []
    for record in technical_records:
        data_points.append({
            'date': str(record.date),
            'rsi': record.rsi,
            'macd': record.macd,
            'macd_signal': record.macd_signal,
            'sma_20': record.sma_20,
            'sma_50': record.sma_50,
            'sma_200': record.sma_200,
            'bollinger_upper': record.bollinger_upper,
            'bollinger_middle': record.bollinger_middle,
            'bollinger_lower': record.bollinger_lower,
            'volume_ma': record.volume_ma,
            'atr': record.atr,
            'smf': record.smf
        })
    
    return data_points


def update_and_get_fundamental_data(symbol_id, symbol_name):
    """
    Updates fundamental data for a symbol and returns its most recent fundamental data.
    """
    update_comprehensive_data_for_symbol(symbol_id, symbol_name)

    fundamental_record = FundamentalData.query.filter_by(symbol_id=symbol_id).order_by(FundamentalData.date.desc()).first()
    
    if fundamental_record:
        data_point = {
            'date': str(fundamental_record.date),
            'eps': fundamental_record.eps,
            'pe_ratio': fundamental_record.pe_ratio,
            'pe_group': fundamental_record.pe_group,
            'pb_ratio': fundamental_record.pb_ratio,
            'ps_ratio': fundamental_record.ps_ratio,
            'base_volume': fundamental_record.base_volume,
            'closing_price': fundamental_record.closing_price,
            'total_shares': fundamental_record.total_shares,
            'market_value': fundamental_record.market_value
        }
        return data_point
    
    return None


def get_all_symbols():
    """
    Retrieves all symbols from the database.
    """
    symbols = ComprehensiveSymbolData.query.all()
    
    symbol_list = []
    for s in symbols:
        symbol_list.append({
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'market_type': s.market_type,
            'is_active': s.is_active
        })
        
    return symbol_list


def get_historical_data_by_symbol_id(symbol_id):
    """
    Retrieves all historical data points for a given symbol_id.
    """
    historical_records = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.date).all()
    
    data_points = []
    for record in historical_records:
        data_points.append({
            'date': str(record.date),
            'open': record.open_price,
            'high': record.high_price,
            'low': record.low_price,
            'close': record.close_price,
            'volume': record.volume,
            'value': record.value,
            'count': record.count
        })
        
    return data_points


def get_technical_data_by_symbol_id(symbol_id):
    """
    Retrieves all technical indicator data points for a given symbol_id.
    """
    technical_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id).order_by(TechnicalIndicatorData.date).all()
    
    data_points = []
    for record in technical_records:
        data_points.append({
            'date': str(record.date),
            'rsi': record.rsi,
            'macd': record.macd,
            'macd_signal': record.macd_signal,
            'sma_20': record.sma_20,
            'sma_50': record.sma_50,
            'sma_200': record.sma_200,
            'bollinger_upper': record.bollinger_upper,
            'bollinger_middle': record.bollinger_middle,
            'bollinger_lower': record.bollinger_lower,
            'volume_ma': record.volume_ma,
            'atr': record.atr,
            'smf': record.smf
        })
        
    return data_points


def get_fundamental_data_by_symbol_id(symbol_id):
    """
    Retrieves the most recent fundamental data record for a given symbol_id.
    """
    fundamental_record = FundamentalData.query.filter_by(symbol_id=symbol_id).order_by(FundamentalData.date.desc()).first()
    
    if fundamental_record:
        data_point = {
            'date': str(fundamental_record.date),
            'eps': fundamental_record.eps,
            'pe_ratio': fundamental_record.pe_ratio,
            'pe_group': fundamental_record.pe_group,
            'pb_ratio': fundamental_record.pb_ratio,
            'ps_ratio': fundamental_record.ps_ratio,
            'base_volume': fundamental_record.base_volume,
            'closing_price': fundamental_record.closing_price,
            'total_shares': fundamental_record.total_shares,
            'market_value': fundamental_record.market_value
        }
        return data_point
        
    return None

def find_symbol_by_id(symbol_id):
    """
    Finds a ComprehensiveSymbolData object by its symbol_id.
    Returns the object or None if not found.
    """
    return ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()


def find_symbol_by_name(symbol_name):
    """
    Finds a ComprehensiveSymbolData object by its symbol_name.
    Returns the object or None if not found.
    """
    return ComprehensiveSymbolData.query.filter_by(symbol_name=symbol_name).first()


def get_symbol_name_by_id(symbol_id):
    """
    Retrieves the symbol name for a given symbol ID.
    """
    symbol = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()
    if symbol:
        return symbol.symbol_name
    return None


def get_symbol_id_by_name(symbol_name):
    """
    Retrieves the symbol ID for a given symbol name.
    """
    symbol = ComprehensiveSymbolData.query.filter_by(symbol_name=symbol_name).first()
    if symbol:
        return symbol.symbol_id
    return None


def update_all_data_for_symbol(symbol_id, symbol_name, days_limit=365):
    """
    A single function to update all data (historical, technical, fundamental) for a symbol.
    """
    success_hist, msg_hist = update_historical_data_for_symbol(symbol_id, symbol_name, days_limit)
    success_tech, msg_tech = update_technical_data_for_symbol(symbol_id, symbol_name, days_limit)
    success_fund, msg_fund = update_comprehensive_data_for_symbol(symbol_id, symbol_name)

    return (
        f"Historical Update: {msg_hist}\n"
        f"Technical Update: {msg_tech}\n"
        f"Fundamental Update: {msg_fund}"
    )


def get_all_data_for_symbol(symbol_id):
    """
    Retrieves all available data (historical, technical, fundamental) for a given symbol.
    Returns a dictionary.
    """
    symbol = find_symbol_by_id(symbol_id)
    if not symbol:
        return None

    historical_data = get_historical_data_by_symbol_id(symbol_id)
    technical_data = get_technical_data_by_symbol_id(symbol_id)
    fundamental_data = get_fundamental_data_by_symbol_id(symbol_id)

    return {
        'symbol_info': {
            'symbol_id': symbol.symbol_id,
            'symbol_name': symbol.symbol_name,
            'market_type': symbol.market_type,
            'is_active': symbol.is_active
        },
        'historical_data': historical_data,
        'technical_data': technical_data,
        'fundamental_data': fundamental_data
    }


def update_specific_data_for_symbol(symbol_id, symbol_name, data_type, days_limit=365):
    """
    Updates a specific type of data (historical, technical, or fundamental) for a symbol.
    """
    if data_type.lower() == 'historical':
        success, msg = update_historical_data_for_symbol(symbol_id, symbol_name, days_limit)
        return f"Historical update for {symbol_name}: {msg}"
    elif data_type.lower() == 'technical':
        success, msg = update_technical_data_for_symbol(symbol_id, symbol_name, days_limit)
        return f"Technical update for {symbol_name}: {msg}"
    elif data_type.lower() == 'fundamental':
        success, msg = update_comprehensive_data_for_symbol(symbol_id, symbol_name)
        return f"Fundamental update for {symbol_name}: {msg}"
    else:
        return f"Error: Invalid data type '{data_type}'. Please choose from 'historical', 'technical', or 'fundamental'."


def delete_symbol_data(symbol_id):
    """
    Deletes all data associated with a symbol from all tables.
    """
    try:
        # Start a transaction to ensure atomicity
        db.session.begin_nested()

        # Delete from all related tables
        HistoricalData.query.filter_by(symbol_id=symbol_id).delete()
        TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id).delete()
        FundamentalData.query.filter_by(symbol_id=symbol_id).delete()
        
        # Finally, delete the symbol from the main table
        ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).delete()
        
        db.session.commit()
        return True, f"All data for symbol ID {symbol_id} has been successfully deleted."
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting data for symbol ID {symbol_id}: {e}")
        return False, f"Failed to delete data for symbol ID {symbol_id}: {e}"


def get_top_symbols_by_volume(limit=10):
    """
    Retrieves the top symbols based on the sum of their recent volume.
    This is a basic example and might need more advanced logic.
    """
    # Fetch all historical data for the last 30 days
    last_30_days = date.today() - timedelta(days=30)
    
    # We can perform this aggregation in the database for better performance
    try:
        top_symbols = db.session.query(
            HistoricalData.symbol_id,
            func.sum(HistoricalData.volume).label('total_volume')
        ).filter(
            HistoricalData.date >= last_30_days
        ).group_by(
            HistoricalData.symbol_id
        ).order_by(
            func.sum(HistoricalData.volume).desc()
        ).limit(limit).all()
        
        results = []
        for sym_id, total_vol in top_symbols:
            symbol_name = get_symbol_name_by_id(sym_id)
            if symbol_name:
                results.append({
                    'symbol_id': sym_id,
                    'symbol_name': symbol_name,
                    'total_volume': total_vol
                })
        
        return results
    except Exception as e:
        logger.error(f"Error retrieving top symbols by volume: {e}")
        return []


def get_top_symbols_by_value(limit=10):
    """
    Retrieves the top symbols based on the sum of their recent transaction value.
    """
    # Fetch all historical data for the last 30 days
    last_30_days = date.today() - timedelta(days=30)
    
    try:
        top_symbols = db.session.query(
            HistoricalData.symbol_id,
            func.sum(HistoricalData.value).label('total_value')
        ).filter(
            HistoricalData.date >= last_30_days
        ).group_by(
            HistoricalData.symbol_id
        ).order_by(
            func.sum(HistoricalData.value).desc()
        ).limit(limit).all()
        
        results = []
        for sym_id, total_val in top_symbols:
            symbol_name = get_symbol_name_by_id(sym_id)
            if symbol_name:
                results.append({
                    'symbol_id': sym_id,
                    'symbol_name': symbol_name,
                    'total_value': total_val
                })
        
        return results
    except Exception as e:
        logger.error(f"Error retrieving top symbols by value: {e}")
        return []


def get_symbols_by_market_type(market_type):
    """
    Retrieves all symbols belonging to a specific market type.
    """
    symbols = ComprehensiveSymbolData.query.filter_by(market_type=market_type).all()
    
    symbol_list = []
    for s in symbols:
        symbol_list.append({
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'market_type': s.market_type,
            'is_active': s.is_active
        })
        
    return symbol_list


def search_symbols(query):
    """
    Searches for symbols by name, case-insensitively.
    """
    # Use SQLAlchemy's `ilike` for case-insensitive search
    symbols = ComprehensiveSymbolData.query.filter(ComprehensiveSymbolData.symbol_name.ilike(f'%{query}%')).all()
    
    symbol_list = []
    for s in symbols:
        symbol_list.append({
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'market_type': s.market_type,
            'is_active': s.is_active
        })
        
    return symbol_list

def add_new_symbol(symbol_name, is_active=True):
    """
    Adds a new symbol to the database and fetches its initial data.
    """
    try:
        # First, get the symbol's TSE ID
        tickers = all_tickers()
        ticker_obj = tickers.get(symbol_name)
        
        if not ticker_obj:
            return False, f"Symbol '{symbol_name}' not found in TSETMC data."
        
        symbol_id = ticker_obj.get_tse_id()
        if not symbol_id:
            return False, f"TSE ID not available for symbol '{symbol_name}'."
            
        # Check if the symbol already exists in our database
        existing_symbol = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()
        if existing_symbol:
            return False, f"Symbol '{symbol_name}' with ID {symbol_id} already exists in the database."

        # Determine market type
        market_type = get_market_type_for_symbol(symbol_id, symbol_name)

        # Create the new symbol record
        new_symbol = ComprehensiveSymbolData(
            symbol_id=symbol_id,
            symbol_name=symbol_name,
            market_type=market_type,
            is_active=is_active
        )
        db.session.add(new_symbol)
        db.session.commit()
        
        # Now fetch and update all its data
        update_all_data_for_symbol(symbol_id, symbol_name)
        
        return True, f"Symbol '{symbol_name}' added successfully and initial data fetched."
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error adding new symbol {symbol_name}: {e}")
        return False, f"An error occurred while adding symbol '{symbol_name}'."


def get_latest_close_price(symbol_id):
    """
    Retrieves the latest closing price for a given symbol.
    """
    latest_record = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.date.desc()).first()
    if latest_record:
        return latest_record.close_price
    return None


def get_latest_fundamental_data_for_all_symbols():
    """
    Retrieves the latest fundamental data for all symbols.
    This can be a heavy query, so it should be used with caution.
    """
    from sqlalchemy import distinct
    
    # Get the latest date for each symbol
    latest_dates = db.session.query(
        FundamentalData.symbol_id,
        func.max(FundamentalData.date).label('max_date')
    ).group_by(
        FundamentalData.symbol_id
    ).subquery()

    # Join the latest_dates subquery with the FundamentalData table
    latest_fundamental_data = db.session.query(
        FundamentalData
    ).join(
        latest_dates,
        (FundamentalData.symbol_id == latest_dates.c.symbol_id) & (FundamentalData.date == latest_dates.c.max_date)
    ).all()
    
    results = []
    for record in latest_fundamental_data:
        symbol = find_symbol_by_id(record.symbol_id)
        if symbol:
            results.append({
                'symbol_name': symbol.symbol_name,
                'date': str(record.date),
                'eps': record.eps,
                'pe_ratio': record.pe_ratio,
                'pe_group': record.pe_group,
                'pb_ratio': record.pb_ratio,
                'ps_ratio': record.ps_ratio,
                'market_value': record.market_value
            })
            
    return results


def calculate_price_change_percentage(symbol_id, days=1):
    """
    Calculates the price change percentage over the last `days`.
    """
    records = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.date.desc()).limit(days + 1).all()
    
    if len(records) < days + 1:
        return None, "Not enough historical data to calculate price change."
        
    start_price = records[-1].close_price
    end_price = records[0].close_price
    
    if start_price == 0:
        return None, "Start price is zero, cannot calculate percentage change."
        
    change = ((end_price - start_price) / start_price) * 100
    
    return change, "Price change calculated successfully."


def get_market_types():
    """
    Retrieves a list of all unique market types from the database.
    """
    market_types = db.session.query(distinct(ComprehensiveSymbolData.market_type)).all()
    
    # The result is a list of tuples, e.g., [('بورس',), ('فرابورس',)]
    return [mt[0] for mt in market_types]


def get_historical_data_range(symbol_id, start_date, end_date):
    """
    Retrieves historical data for a symbol within a specified date range.
    Dates should be datetime.date objects.
    """
    records = HistoricalData.query.filter_by(symbol_id=symbol_id).filter(
        HistoricalData.date.between(start_date, end_date)
    ).order_by(HistoricalData.date).all()
    
    data_points = []
    for record in records:
        data_points.append({
            'date': str(record.date),
            'open': record.open_price,
            'high': record.high_price,
            'low': record.low_price,
            'close': record.close_price,
            'volume': record.volume,
            'value': record.value,
            'count': record.count
        })
    
    return data_points


def get_technical_data_range(symbol_id, start_date, end_date):
    """
    Retrieves technical data for a symbol within a specified date range.
    """
    records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id).filter(
        TechnicalIndicatorData.date.between(start_date, end_date)
    ).order_by(TechnicalIndicatorData.date).all()
    
    data_points = []
    for record in records:
        data_points.append({
            'date': str(record.date),
            'rsi': record.rsi,
            'macd': record.macd,
            'macd_signal': record.macd_signal,
            'sma_20': record.sma_20,
            'sma_50': record.sma_50,
            'sma_200': record.sma_200,
            'bollinger_upper': record.bollinger_upper,
            'bollinger_middle': record.bollinger_middle,
            'bollinger_lower': record.bollinger_lower,
            'volume_ma': record.volume_ma,
            'atr': record.atr,
            'smf': record.smf
        })
    
    return data_points


def get_fundamental_data_range(symbol_id, start_date, end_date):
    """
    Retrieves fundamental data for a symbol within a specified date range.
    """
    records = FundamentalData.query.filter_by(symbol_id=symbol_id).filter(
        FundamentalData.date.between(start_date, end_date)
    ).order_by(FundamentalData.date).all()
    
    data_points = []
    for record in records:
        data_points.append({
            'date': str(record.date),
            'eps': record.eps,
            'pe_ratio': record.pe_ratio,
            'pe_group': record.pe_group,
            'pb_ratio': record.pb_ratio,
            'ps_ratio': record.ps_ratio,
            'base_volume': record.base_volume,
            'closing_price': record.closing_price,
            'total_shares': record.total_shares,
            'market_value': record.market_value
        })
        
    return data_points


def get_most_recent_data_by_type(symbol_id, data_type):
    """
    Retrieves the most recent data point for a given symbol and data type.
    """
    if data_type.lower() == 'historical':
        record = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.date.desc()).first()
        if record:
            return {
                'date': str(record.date),
                'open': record.open_price,
                'high': record.high_price,
                'low': record.low_price,
                'close': record.close_price,
                'volume': record.volume,
                'value': record.value,
                'count': record.count
            }
    elif data_type.lower() == 'technical':
        record = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id).order_by(TechnicalIndicatorData.date.desc()).first()
        if record:
            return {
                'date': str(record.date),
                'rsi': record.rsi,
                'macd': record.macd,
                'macd_signal': record.macd_signal,
                'sma_20': record.sma_20,
                'sma_50': record.sma_50,
                'sma_200': record.sma_200,
                'bollinger_upper': record.bollinger_upper,
                'bollinger_middle': record.bollinger_middle,
                'bollinger_lower': record.bollinger_lower,
                'volume_ma': record.volume_ma,
                'atr': record.atr,
                'smf': record.smf
            }
    elif data_type.lower() == 'fundamental':
        record = FundamentalData.query.filter_by(symbol_id=symbol_id).order_by(FundamentalData.date.desc()).first()
        if record:
            return {
                'date': str(record.date),
                'eps': record.eps,
                'pe_ratio': record.pe_ratio,
                'pe_group': record.pe_group,
                'pb_ratio': record.pb_ratio,
                'ps_ratio': record.ps_ratio,
                'base_volume': record.base_volume,
                'closing_price': record.closing_price,
                'total_shares': record.total_shares,
                'market_value': record.market_value
            }
    
    return None


def get_symbols_by_activity_status(is_active=True):
    """
    Retrieves symbols based on their active status.
    """
    symbols = ComprehensiveSymbolData.query.filter_by(is_active=is_active).all()
    
    symbol_list = []
    for s in symbols:
        symbol_list.append({
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'market_type': s.market_type,
            'is_active': s.is_active
        })
        
    return symbol_list


def set_symbol_activity_status(symbol_id, is_active):
    """
    Sets the active status for a given symbol.
    """
    try:
        symbol = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()
        if not symbol:
            return False, f"Symbol with ID {symbol_id} not found."
            
        symbol.is_active = is_active
        db.session.commit()
        
        status_text = "active" if is_active else "inactive"
        return True, f"Symbol {symbol.symbol_name} (ID: {symbol_id}) is now set to {status_text}."
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error setting activity status for symbol ID {symbol_id}: {e}")
        return False, f"An error occurred while setting the activity status."


def get_database_statistics():
    """
    Provides statistics about the database content.
    """
    try:
        total_symbols = db.session.query(ComprehensiveSymbolData).count()
        active_symbols = db.session.query(ComprehensiveSymbolData).filter_by(is_active=True).count()
        
        total_historical_records = db.session.query(HistoricalData).count()
        total_technical_records = db.session.query(TechnicalIndicatorData).count()
        total_fundamental_records = db.session.query(FundamentalData).count()

        latest_historical_date = db.session.query(func.max(HistoricalData.date)).scalar()
        latest_technical_date = db.session.query(func.max(TechnicalIndicatorData.date)).scalar()
        latest_fundamental_date = db.session.query(func.max(FundamentalData.date)).scalar()

        stats = {
            'total_symbols': total_symbols,
            'active_symbols': active_symbols,
            'total_historical_records': total_historical_records,
            'total_technical_records': total_technical_records,
            'total_fundamental_records': total_fundamental_records,
            'latest_data_dates': {
                'historical': str(latest_historical_date) if latest_historical_date else None,
                'technical': str(latest_technical_date) if latest_technical_date else None,
                'fundamental': str(latest_fundamental_date) if latest_fundamental_date else None
            }
        }
        return stats, "Database statistics retrieved successfully."
    except Exception as e:
        logger.error(f"Error retrieving database statistics: {e}")
        return None, "An error occurred while retrieving database statistics."


def update_and_get_all_data_for_symbol(symbol_id, symbol_name, days_limit=365):
    """
    A single function to update all data and then retrieve it for a given symbol.
    """
    # First, run the update process
    update_all_data_for_symbol(symbol_id, symbol_name, days_limit)

    # Then, retrieve and return all the data
    return get_all_data_for_symbol(symbol_id)