# services/data_fetch_and_process.py
from extensions import db
from models import HistoricalData, ComprehensiveSymbolData, TechnicalIndicatorData, FundamentalData
from flask import current_app
import pytse_client as tse
import pandas as pd
from datetime import datetime, date, timedelta # Import date here too
import jdatetime
from sqlalchemy import func
import numpy as np
import requests
from bs4 import BeautifulSoup # Import BeautifulSoup
import lxml # lxml is the parser for BeautifulSoup

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
        'صندوق سرمایه گذاری', 'صندوق قابل معامله', 'صندوق های قابل معامله', 'صندوقهای سرمایه گذاری',
        'بورس کالا', 'بورس انرژی', 'فرابورس', 'بورس اوراق بهادار', 'اوراق با درآمد ثابت',
        'اختیار معامله', 'صکوک', 'گواهی سپرده', 'حق تقدم', 'بازار پایه', 'مشتقه', 'سهام',
        'اوراق بهادار', 'اوراق تامین مالی', 'اوراق مشارکت', 'گواهی سپرده کالایی',
        'صندوق سرمایه گذاری مشترک', 'صندوق سرمایه گذاری قابل معامله', 'صندوق سرمایه گذاری در سهام',
        'صندوق سرمایه گذاری مختلط', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت',
        'صندوق سرمایه گذاری طلا', 'اوراق اجاره', 'بورس کالا و انرژی', 'بورس اوراق بهادار تهران',
        'فرابورس ایران', 'بازار ابزارهای نوین مالی', 'بازار ابزارهای مالی نوین',
        'صندوق سرمایه گذاری در اوراق بهادار', 'صندوق سرمایه گذاری در صندوق',
        'صندوق سرمایه گذاری جسورانه', 'صندوق سرمایه گذاری زمین و ساختمان',
        'صندوق سرمایه گذاری اختصاصی بازارگردانی', 'صندوق سرمایه گذاری پروژه',
        'صندوق سرمایه گذاری در بورس کالا', 'صندوق سرمایه گذاری در طلا',
        'صندوق سهامي اهرمي', 'نوع صندوق: سهامي', 'صندوق سهامي', 'صندوق اهرمي', # NEW: User requested and derived
        'صندوق با درآمد ثابت', 'صندوق مختلط', 'صندوق بازارگردانی', 'صندوق پروژه',
        'صندوق طلا', 'صندوق کالایی', 'صندوق جسورانه', 'صندوق زمین و ساختمان',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت', 'صندوق سرمایه گذاری در اوراق بهادار رهنی',
        'صندوق سرمایه گذاری در اوراق بهادار مبتنی بر کالا', 'صندوق سرمایه گذاری در صندوق‌های سرمایه گذاری',
        'صندوق سرمایه گذاری در صندوق‌های سرمایه گذاری قابل معامله', 'صندوق سرمایه گذاری در سهام و حق تقدم سهام',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و سهام', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و مختلط',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و کالا',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و جسورانه', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و زمین و ساختمان',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازارگردانی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و پروژه',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس کالا', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و حق تقدم', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اختیار معامله',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و آتی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صکوک',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و گواهی سپرده', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق مشارکت',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق اجاره', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و اوراق تامین مالی',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار ابزارهای نوین مالی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار ابزارهای مالی نوین',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس کالا و انرژی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بورس اوراق بهادار تهران',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و فرابورس ایران', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و بازار پایه',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و عمومی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و نامشخص',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و سهام اهرمي', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و نوع صندوق: سهامي',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سهامي', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق اهرمي',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق با درآمد ثابت', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق مختلط',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق بازارگردانی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق پروژه',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق طلا', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق کالایی',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق جسورانه', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق زمین و ساختمان',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در اوراق بهادار', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در صندوق',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری جسورانه', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری زمین و ساختمان',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری اختصاصی بازارگردانی', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری پروژه',
        'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در بورس کالا', 'صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و صندوق سرمایه گذاری در اوراق بهادار با درآمد ثابت و طلا',
    ]

    for indicator in common_market_indicators_ordered:
        found_string = soup.find(string=lambda text: text and indicator in text)
        if found_string:
            logger.debug(f"HTML Parser: Keyword search: Found '{indicator}' in page text.")
            return HTML_MARKET_TYPE_MAP.get(indicator, indicator) # Map if possible, otherwise return raw

    # --- Priority 3: Generic Text Search for "بازار:" (as in previous version) ---
    # This is still useful if the market type is directly next to "بازار:"
    for tag in soup.find_all(lambda t: t.name in ['span', 'div', 'td'] and 'بازار:' in t.text):
        next_sibling = tag.find_next_sibling()
        if next_sibling and next_sibling.name in ['span', 'div', 'td'] and next_sibling.text.strip():
            market_type_str = next_sibling.text.strip()
            logger.debug(f"HTML Parser: Generic 'بازار:' sibling search: Found '{market_type_str}'.")
            return HTML_MARKET_TYPE_MAP.get(market_type_str, market_type_str)
        
        parts = tag.text.split('بازار:')
        if len(parts) > 1 and parts[1].strip():
            market_type_str = parts[1].strip()
            logger.debug(f"HTML Parser: Generic 'بازار:' embedded search: Found '{market_type_str}'.")
            return HTML_MARKET_TYPE_MAP.get(market_type_str, market_type_str)
    
    logger.warning("HTML Parser: Could not find market type in Loader.aspx HTML using any known pattern.")
    return None

def get_symbol_id(symbol_name_or_id):
    """
    Resolves a symbol name or ID to a consistent symbol_id from ComprehensiveSymbolData.
    """
    # Import here to avoid circular dependency with models, as models might import utils
    from models import ComprehensiveSymbolData 
    symbol = ComprehensiveSymbolData.query.filter(
        (ComprehensiveSymbolData.symbol_id == symbol_name_or_id) |
        (ComprehensiveSymbolData.symbol_name == symbol_name_or_id)
    ).first()
    if symbol:
        return symbol.symbol_id
    return None

def fetch_all_symbols_info_from_pytse():
    """
    Fetches symbols using tse.all_symbols(), then enriches them with Ticker-specific info.
    Filters symbols based on market type after attempting to get Ticker info.
    Uses BeautifulSoup as a fallback for market type if Ticker.flow is 'نامشخص' or unmapped.
    Also uses Ticker.group_name as an intermediate fallback.
    Returns:
        List[Dict]: A list of dictionaries, each representing a symbol with enriched data.
    """
    logger.info("Fetching all symbols using tse.all_symbols() and enriching with Ticker info (with HTML fallback).")
    
    all_symbol_ids_from_pytse = []
    try:
        all_symbol_ids_from_pytse = tse.all_symbols()
        logger.info(f"Successfully fetched {len(all_symbol_ids_from_pytse)} raw symbols from pytse_client.all_symbols().")
    except Exception as e:
        logger.error(f"Error fetching all symbols from pytse_client.all_symbols(): {e}", exc_info=True)
        return []

    if not all_symbol_ids_from_pytse:
        logger.warning("No symbols returned from pytse_client.all_symbols(). Cannot proceed with enrichment.")
        return []

    temp_enriched_symbols = []
    for symbol_id_str in all_symbol_ids_from_pytse:
        if not symbol_id_str:
            logger.warning("Skipping empty symbol ID from tse.all_symbols().")
            continue
        
        market_type_name = 'نامشخص' # Default fallback
        base_volume = 0
        actual_symbol_id = symbol_id_str
        actual_symbol_name = symbol_id_str

        try:
            # Instantiate Ticker to get detailed info like flow (market_type) and base_volume
            ticker = tse.Ticker(symbol=symbol_id_str)
            
            actual_symbol_id = getattr(ticker, 'symbol', symbol_id_str)
            actual_symbol_name = getattr(ticker, 'title', symbol_id_str)

            # --- Primary Market Type Detection: Ticker.flow ---
            market_type_code = getattr(ticker, 'flow', None)
            market_type_name = MARKET_TYPE_MAP.get(market_type_code, 'نامشخص')
            
            base_volume = getattr(ticker, 'base_volume', 0)

            logger.debug(f"Initial Ticker info for {actual_symbol_name}: Market: {market_type_name}, Base Volume: {base_volume}).")

            # --- Secondary Market Type Detection: Ticker.group_name (if flow is 'نامشخص') ---
            if market_type_name == 'نامشخص' and hasattr(ticker, 'group_name') and getattr(ticker, 'group_name', None):
                group_name_str = getattr(ticker, 'group_name').strip()
                mapped_group_name = HTML_MARKET_TYPE_MAP.get(group_name_str, group_name_str)
                if mapped_group_name != group_name_str: # Only update if a mapping was found
                    market_type_name = mapped_group_name
                    logger.info(f"Market type for {actual_symbol_name} updated from Ticker.group_name: '{group_name_str}' mapped to '{market_type_name}'.")
                else:
                    logger.debug(f"Ticker.group_name '{group_name_str}' for {actual_symbol_name} did not provide a known market type mapping.")

            # --- Tertiary Market Type Detection: HTML Fallback (if still 'نامشخص') ---
            if market_type_name == 'نامشخص' and hasattr(ticker, 'url') and getattr(ticker, 'url', None) and 'Loader.aspx' in getattr(ticker, 'url'):
                logger.info(f"Market type for {actual_symbol_name} is still 'نامشخص'. Attempting HTML fallback from {getattr(ticker, 'url')}.")
                try:
                    loader_url = getattr(ticker, 'url')
                    response_html = requests.get(loader_url, timeout=10)
                    response_html.raise_for_status()
                    html_market_type = _extract_market_type_from_loader_html(response_html.text)
                    if html_market_type:
                        market_type_name = HTML_MARKET_TYPE_MAP.get(html_market_type, html_market_type)
                        logger.info(f"Successfully extracted market type '{html_market_type}' (mapped to '{market_type_name}') from Loader.aspx for {actual_symbol_name}.")
                    else:
                        logger.warning(f"Could not extract market type from Loader.aspx for {actual_symbol_name}. Keeping as 'نامشخص'.")
                except requests.exceptions.RequestException as req_e:
                    logger.warning(f"Error fetching Loader.aspx for {actual_symbol_name}: {req_e}. Keeping market type as 'نامشخص'.", exc_info=True)
                except Exception as html_e:
                    logger.warning(f"Error parsing Loader.aspx HTML for {actual_symbol_name}: {html_e}. Keeping market type as 'نامشخص'.", exc_info=True)

        except (IndexError, RuntimeError, Exception) as e:
            logger.warning(f"Could not instantiate Ticker or fetch basic info for {symbol_id_str}: {e}. Adding basic info with 'نامشخص' market type.", exc_info=True)
            # Fallback values are already set at the beginning of the loop
            
        temp_enriched_symbols.append({
            'symbol_id': actual_symbol_id,
            'symbol_name': actual_symbol_name,
            'market_type': market_type_name,
            'base_volume': base_volume
        })


    if not temp_enriched_symbols:
        logger.warning("No symbols were successfully enriched with any info. Cannot populate ComprehensiveSymbolData.")
        return []

    # Filter for desired market types after all symbols have been processed
    final_filtered_symbols = []
    # MODIFIED: Removed 'حق تقدم' from allowed_market_types as per user's request
    allowed_market_types = ['بورس', 'فرابورس', 'بورس کالا', 'صندوق سرمایه گذاری', 'اوراق با درآمد ثابت', 'مشتقه', 'عمومی', 'پایه فرابورس', 'بورس انرژی', 'اوراق تامین مالی', 'اوراق با درآمد ثابت']
    
    for symbol_data in temp_enriched_symbols:
        if symbol_data['market_type'] in allowed_market_types:
            final_filtered_symbols.append(symbol_data)
        else:
            logger.debug(f"Skipping symbol {symbol_data['symbol_name']} due to market type: {symbol_data['market_type']}.")

    logger.info(f"Filtered down to {len(final_filtered_symbols)} symbols based on allowed market types: {allowed_market_types}.")
    
    return final_filtered_symbols


def populate_all_symbols_initial():
    """
    Populates the ComprehensiveSymbolData table with basic symbol information from pytse_client.
    This should be run once to seed the database.
    Now uses Ticker object to get market_type and base_volume.
    """
    logger.info("Starting initial population of ComprehensiveSymbolData.")
    symbols_data = fetch_all_symbols_info_from_pytse() # Use the new enriched fetch function
    
    if not symbols_data:
        logger.warning("No symbols fetched or enriched from pytse_client. Cannot populate ComprehensiveSymbolData.")
        return 0, "No symbols fetched."

    added_count = 0
    updated_count = 0
    for symbol_item in symbols_data:
        symbol_id = symbol_item.get('symbol_id')
        symbol_name = symbol_item.get('symbol_name')
        market_type = symbol_item.get('market_type')
        base_volume = symbol_item.get('base_volume')
        
        if not symbol_id:
            logger.warning(f"Skipping symbol with missing 'symbol' ID in enriched data: {symbol_item}")
            continue

        existing_symbol = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()

        if existing_symbol:
            # Update existing symbol data
            changed = False
            if existing_symbol.symbol_name != symbol_name:
                existing_symbol.symbol_name = symbol_name
                changed = True
            if existing_symbol.market_type != market_type:
                existing_symbol.market_type = market_type
                changed = True
            if existing_symbol.base_volume != base_volume:
                existing_symbol.base_volume = base_volume
                changed = True

            if changed:
                existing_symbol.updated_at = datetime.now()
                db.session.add(existing_symbol)
                updated_count += 1
                logger.debug(f"Updated existing symbol: {symbol_name} ({symbol_id}) with market type: {market_type}, base volume: {base_volume}.")
            else:
                logger.debug(f"Symbol {symbol_name} ({symbol_id}) already up-to-date in ComprehensiveSymbolData.")
        else:
            # Add new symbol
            new_symbol = ComprehensiveSymbolData(
                symbol_id=symbol_id,
                symbol_name=symbol_name,
                market_type=market_type,
                base_volume=base_volume,
                # created_at and updated_at are typically handled by SQLAlchemy defaults
            )
            db.session.add(new_symbol)
            added_count += 1
            logger.debug(f"Added new symbol: {symbol_name} ({symbol_id}) with market type: {market_type}, base volume: {base_volume}.")
            
    try:
        db.session.commit()
        message = f"ComprehensiveSymbolData population completed. Added {added_count} new symbols, updated {updated_count} existing symbols."
        logger.info(message)
        return added_count + updated_count, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error populating ComprehensiveSymbolData: {e}"
        logger.error(error_message, exc_info=True)
        return 0, error_message


def update_historical_data_for_symbol(symbol_id, symbol_name, limit_days=120):
    """
    Fetches historical data and client types data for a given symbol using Ticker object
    and updates the HistoricalData table.
    Ensures data consistency and handles missing values.
    
    Args:
        symbol_id (str): The ID of the symbol.
        symbol_name (str): The name of the symbol.
        limit_days (int): Number of days to fetch historical data for.
        
    Returns:
        Tuple[bool, str]: True and a success message, or False and an error message.
    """
    logger.info(f"Fetching historical and client types data for {symbol_name} ({symbol_id}) for last {limit_days} days.")
    try:
        ticker = tse.Ticker(symbol=symbol_id, adjust=True) # Use adjust=True for adjusted prices
        
        # Fetch historical OHLCV data
        hist_df = ticker.history
        if hist_df.empty:
            logger.warning(f"No historical OHLCV data returned from pytse_client for {symbol_name} ({symbol_id}).")
            return False, f"No historical OHLCV data for {symbol_name}."

        # Fetch client types data
        client_types_df = ticker.client_types
        if client_types_df.empty:
            logger.warning(f"No client types data returned from pytse_client for {symbol_name} ({symbol_id}).")
            # Proceed, but smart money filters might be affected
            client_types_df = pd.DataFrame(columns=['date', 'individual_buy_vol', 'individual_sell_vol', 
                                                    'individual_buy_count', 'individual_sell_count'])

        # Convert 'date' columns to datetime objects for merging
        hist_df['date'] = pd.to_datetime(hist_df['date'])
        client_types_df['date'] = pd.to_datetime(client_types_df['date'])

        # Filter for the last `limit_days`
        start_date = datetime.now() - timedelta(days=limit_days)
        hist_df = hist_df[hist_df['date'] >= start_date].copy()
        client_types_df = client_types_df[client_types_df['date'] >= start_date].copy()

        if hist_df.empty:
            logger.warning(f"No recent historical OHLCV data for the last {limit_days} days for {symbol_name} ({symbol_id}).")
            return False, f"No recent historical OHLCV data for {symbol_name}."

        # Merge historical and client types data on 'date'
        # Use 'outer' merge to keep all dates from both, then fill missing values
        merged_df = pd.merge(hist_df, client_types_df, on='date', how='outer', suffixes=('_hist', '_client'))
        merged_df = merged_df.sort_values(by='date', ascending=True).reset_index(drop=True)
        
        # Fill NaN values for columns that might be missing after merge
        # For numeric columns, fill with 0
        numeric_cols_to_fill = [
            'open', 'high', 'low', 'close', 'final', 'volume', 'value', 'count', # From history
            'individual_buy_vol', 'individual_sell_vol', 'individual_buy_count', 'individual_sell_count', # From client_types
            'yesterday_price', 'pcp', 'plc', 'plp', 'num_trades', 'buy_count_n', 'sell_count_n',
            'buy_n_volume', 'sell_n_volume', 'mv', 'po1', 'po2', 'po3', 'po4', 'po5',
            'pd1', 'pd2', 'pd3', 'pd4', 'pd5', 'qo1', 'qo2', 'qo3', 'qo4', 'qo5', # Fixed qo1 repetition
            'qd1', 'qd2', 'qd3', 'qd4', 'qd5',
            'zo1', 'zo2', 'zo3', 'zo4', 'zo5',
            'zd1', 'zd2', 'zd3', 'zd4', 'zd5'
        ]
        for col in numeric_cols_to_fill:
            if col in merged_df.columns:
                # Ensure numeric conversion and then fillna for the Series
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)
            else:
                merged_df[col] = 0 # Add missing columns and fill with 0

        processed_rows = 0
        for index, row in merged_df.iterrows():
            greg_date = row['date'].date() # Extract date part
            jdate_str = convert_gregorian_to_jalali(greg_date)

            # Ensure symbol_id is consistent
            db_symbol_id = get_symbol_id(symbol_id)
            if not db_symbol_id:
                logger.warning(f"Resolved symbol_id not found for {symbol_id}. Skipping historical data update for this row.")
                continue

            # Check if record already exists
            existing_record = HistoricalData.query.filter_by(
                symbol_id=db_symbol_id,
                jdate=jdate_str
            ).first()


            # --- NEW LOGIC FOR FINAL PRICE (from user's suggestion) ---
            current_final_price = safe_float(row.get('final'))
            if current_final_price <= 0: # If final price is missing or zero, use close price
                current_final_price = safe_float(row.get('close'))
            # --- END NEW LOGIC ---

            # Construct the record_data dictionary
            record_data = {
                'symbol_id': symbol_id,
                'symbol_name': symbol_name,
                'jdate': jdate_str,
                'date': greg_date_index, # Store Gregorian date object
                'close': safe_float(row.get('close')),
                'open': safe_float(row.get('open')),
                'high': safe_float(row.get('high')),
                'low': safe_float(row.get('low')),
                'volume': safe_float(row.get('volume')),
                'value': safe_float(row.get('value')),
                'final': current_final_price, # Use the determined final price
                
                # Client type data - use .get() and safe_float for robustness
                'buy_i_volume': safe_float(client_row.get('individual_buy_vol')),
                'sell_i_volume': safe_float(client_row.get('individual_sell_vol')),
                'buy_count_i': safe_float(client_row.get('individual_buy_count')),
                'sell_count_i': safe_float(client_row.get('individual_sell_count')),
                
                # Other fields from ticker.history or client_types that match your model
                # Use .get() for safety as columns might not always be present or have None
                'yesterday_price': safe_float(row.get('yesterday_price')), 
                'pcp': safe_float(row.get('pcp')),
                'plc': safe_float(row.get('plc')),
                'plp': safe_float(row.get('plp')),
                'num_trades': safe_float(row.get('count')), # 'count' from ticker.history
                
                # Corporate data - use .get() and safe_float for robustness
                'buy_count_n': safe_float(client_row.get('corporate_buy_count')),
                'sell_count_n': safe_float(client_row.get('corporate_sell_count')),
                'buy_n_volume': safe_float(client_row.get('corporate_buy_vol')),
                'sell_n_volume': safe_float(client_row.get('corporate_sell_vol')),
                
                'mv': safe_float(row.get('value')), # Assuming 'mv' is equivalent to 'value' for now
                
                # Orderbook/real-time fields - these are usually for current day,
                # so they might not be present in historical 'row'. Use .get() and safe_float.
                'po1': safe_float(row.get('po1')), 'po2': safe_float(row.get('po2')), 'po3': safe_float(row.get('po3')), 'po4': safe_float(row.get('po4')), 'po5': safe_float(row.get('po5')),
                'pd1': safe_float(row.get('pd1')), 'pd2': safe_float(row.get('pd2')), 'pd3': safe_float(row.get('pd3')), 'pd4': safe_float(row.get('pd4')), 'pd5': safe_float(row.get('pd5')),
                'qo1': safe_float(row.get('qo1')), 'qo2': safe_float(row.get('qo2')), 'qo3': safe_float(row.get('qo3')), 'qo4': safe_float(row.get('qo4')), 'qo5': safe_float(row.get('qo5')),
                'qd1': safe_float(row.get('qd1')), 'qd2': safe_float(row.get('qd2')), 'qd3': safe_float(row.get('qd3')), 'qd4': safe_float(row.get('qd4')), 'qd5': safe_float(row.get('qd5')),
                'zo1': safe_float(row.get('zo1')), 'zo2': safe_float(row.get('zo2')), 'zo3': safe_float(row.get('zo3')), 'zo4': safe_float(row.get('zo4')), 'zo5': safe_float(row.get('zo5')),
                'zd1': safe_float(row.get('zd1')), 'zd2': safe_float(row.get('zd2')), 'zd3': safe_float(row.get('zd3')), 'zd4': safe_float(row.get('zd4')), 'zd5': safe_float(row.get('zd5')),
            }

            if existing_record:
                for key, value in record_data.items():
                    setattr(existing_record, key, value)
                existing_record.updated_at = datetime.now()
                db.session.add(existing_record)
            else:
                new_record = HistoricalData(
                    **record_data
                    # created_at and updated_at are typically handled by SQLAlchemy defaults
                )
                db.session.add(new_record)
            processed_rows += 1

        db.session.commit()
        logger.info(f"Successfully updated/added {processed_rows} historical data rows for {symbol_name}.")
        return True, f"Successfully processed {processed_rows} historical data rows."

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating historical data for {symbol_name} ({symbol_id}): {e}", exc_info=True)
        return False, f"Error updating historical data for {symbol_name}: {str(e)}"


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
    Fetches and updates comprehensive symbol data (including fundamental data) for a given symbol.
    This function leverages pytse_client's Ticker object.
    """
    logger.info(f"Updating comprehensive and fundamental data for {symbol_name} ({symbol_id}).")
    try:
        ticker = tse.Ticker(symbol=symbol_id)
        
        # Update ComprehensiveSymbolData
        existing_comp_symbol = ComprehensiveSymbolData.query.filter_by(symbol_id=symbol_id).first()
        if existing_comp_symbol:
            # Use getattr for safer access to potentially missing attributes
            existing_comp_symbol.symbol_name = getattr(ticker, 'title', existing_comp_symbol.symbol_name)
            existing_comp_symbol.company_name = getattr(ticker, 'company_name', existing_comp_symbol.company_name)
            existing_comp_symbol.isin = getattr(ticker, 'isin', existing_comp_symbol.isin)
            
            # Use the robust market type extraction
            market_type_code = getattr(ticker, 'flow', None)
            market_type_name = MARKET_TYPE_MAP.get(market_type_code, 'نامشخص')
            if market_type_name == 'نامشخص' and hasattr(ticker, 'group_name') and getattr(ticker, 'group_name', None):
                group_name_str = getattr(ticker, 'group_name').strip()
                mapped_group_name = HTML_MARKET_TYPE_MAP.get(group_name_str, group_name_str)
                if mapped_group_name != group_name_str:
                    market_type_name = mapped_group_name
            if market_type_name == 'نامشخص' and hasattr(ticker, 'url') and getattr(ticker, 'url', None) and 'Loader.aspx' in getattr(ticker, 'url'):
                loader_url = getattr(ticker, 'url')
                response_html = requests.get(loader_url, timeout=10)
                response_html.raise_for_status()
                html_market_type = _extract_market_type_from_loader_html(response_html.text)
                if html_market_type:
                    market_type_name = HTML_MARKET_TYPE_MAP.get(html_market_type, html_market_type)

            existing_comp_symbol.market_type = market_type_name
            existing_comp_symbol.flow = str(getattr(ticker, 'flow', None)) if getattr(ticker, 'flow', None) is not None else None
            existing_comp_symbol.industry = getattr(ticker, 'industry_name', existing_comp_symbol.industry)
            existing_comp_symbol.capital = str(getattr(ticker, 'capital', None)) if getattr(ticker, 'capital', None) is not None else existing_comp_symbol.capital
            existing_comp_symbol.legal_shareholder_percentage = getattr(ticker, 'legal_shareholder_percentage', existing_comp_symbol.legal_shareholder_percentage)
            existing_comp_symbol.real_shareholder_percentage = getattr(ticker, 'real_shareholder_percentage', existing_comp_symbol.real_shareholder_percentage)
            existing_comp_symbol.float_shares = getattr(ticker, 'float_shares', existing_comp_symbol.float_shares)
            existing_comp_symbol.base_volume = getattr(ticker, 'base_volume', existing_comp_symbol.base_volume)
            existing_comp_symbol.group_name = getattr(ticker, 'group_name', existing_comp_symbol.group_name)
            existing_comp_symbol.description = getattr(ticker, 'description', existing_comp_symbol.description)
            existing_comp_symbol.last_historical_update_date = date.today() 
            existing_comp_symbol.updated_at = datetime.now()
            db.session.add(existing_comp_symbol)
            logger.debug(f"Updated ComprehensiveSymbolData for {symbol_name}.")
        else:
            logger.warning(f"ComprehensiveSymbolData for {symbol_name} not found. Skipping update. Please run initial population.")
            return False, f"ComprehensiveSymbolData for {symbol_name} not found. Please run initial population."

        # Update FundamentalData
        existing_fund_data = FundamentalData.query.filter_by(symbol_id=symbol_id).first()
        
        # Ensure 'eps' and 'pe' are fetched and handled with getattr
        eps = getattr(ticker, 'eps', None)
        pe_ratio = getattr(ticker, 'pe_ratio', None)
        group_pe_ratio = getattr(ticker, 'group_pe_ratio', None)
        psr = getattr(ticker, 'psr', None)
        
        # Handle p_s_ratio and market_cap carefully due to potential internal TypeError in pytse_client
        p_s_ratio_val = None
        try:
            p_s_ratio_val = getattr(ticker, 'p_s_ratio', None)
        except TypeError:
            logger.warning(f"TypeError when getting p_s_ratio for {symbol_name}. Setting to None.")
            p_s_ratio_val = None

        market_cap_val = None
        try:
            market_cap_val = getattr(ticker, 'market_value', None) # Assuming market_value is market_cap
        except TypeError:
            logger.warning(f"TypeError when getting market_value for {symbol_name}. Setting to None.")
            market_cap_val = None
        
        if existing_fund_data:
            existing_fund_data.eps = eps
            existing_fund_data.pe_ratio = pe_ratio
            existing_fund_data.group_pe_ratio = group_pe_ratio
            existing_fund_data.psr = psr
            existing_fund_data.p_s_ratio = p_s_ratio_val # Use the safely retrieved value
            existing_fund_data.market_cap = market_cap_val # Use the safely retrieved value
            existing_fund_data.base_volume = getattr(ticker, 'base_volume', existing_fund_data.base_volume)
            existing_fund_data.float_shares = getattr(ticker, 'float_shares', existing_fund_data.float_shares)
            existing_fund_data.last_updated = datetime.now()
            db.session.add(existing_fund_data)
            logger.debug(f"Updated FundamentalData for {symbol_name}.")
        else:
            new_fund_data = FundamentalData(
                symbol_id=symbol_id,
                eps=eps,
                pe_ratio=pe_ratio,
                group_pe_ratio=group_pe_ratio,
                psr=psr,
                p_s_ratio=p_s_ratio_val, # Use the safely retrieved value
                market_cap=market_cap_val, # Use the safely retrieved value
                base_volume=getattr(ticker, 'base_volume', 0),
                float_shares=getattr(ticker, 'float_shares', 0),
                last_updated=datetime.now()
            )
            db.session.add(new_fund_data)
            logger.debug(f"Added new FundamentalData for {symbol_name}.")

        db.session.commit()
        logger.info(f"Successfully updated comprehensive and fundamental data for {symbol_name}.")
        return True, f"Successfully updated comprehensive and fundamental data for {symbol_name}."

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating comprehensive/fundamental data for {symbol_name} ({symbol_id}): {e}", exc_info=True)
        return False, f"Error updating comprehensive/fundamental data for {symbol_name}: {str(e)}"


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
    
    symbols_to_process = ComprehensiveSymbolData.query.all()
    if not symbols_to_process:
        logger.warning("No symbols found in ComprehensiveSymbolData. Please run initial population first.")
        return 0, "No symbols to process."

    total_processed_count = 0
    
    for symbol in symbols_to_process:
        # Update Historical Data (including client types)
        success_hist, msg_hist = update_historical_data_for_symbol(symbol.symbol_id, symbol.symbol_name, limit_days=days_limit)
        if success_hist:
            total_processed_count += 1
            logger.info(f"Historical data update for {symbol.symbol_name}: {msg_hist}")
        else:
            logger.warning(f"Failed historical data update for {symbol.symbol_name}: {msg_hist}")

        # Analyze Technical Data
        # Pass the same days_limit to ensure enough technical data is saved
        success_tech, msg_tech = analyze_technical_data_for_symbol(symbol.symbol_id, symbol.symbol_name, limit_days=days_limit)
        if success_tech:
            total_processed_count += 1
            logger.info(f"Technical analysis for {symbol.symbol_name}: {msg_tech}")
        else:
            logger.warning(f"Failed technical data analysis for {symbol.symbol_name}: {msg_tech}")

        # Update Fundamental Data (using the comprehensive update function)
        success_fund, msg_fund = update_comprehensive_data_for_symbol(symbol.symbol_id, symbol.symbol_name)
        if success_fund:
            total_processed_count += 1
            logger.info(f"Fundamental data update for {symbol.symbol_name}: {msg_fund}")
        else:
            logger.warning(f"Failed fundamental data update for {symbol.symbol_name}: {msg_fund}")

    final_message = f"Full data update summary: Total processed operations: {total_processed_count}. Check logs for details on each symbol."
    current_app.logger.info(final_message)
    return total_processed_count, final_message


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
    
    return processed_count, f"Initial data population completed. {total_comp_symbols_added} symbols populated. {msg_data_update}"
