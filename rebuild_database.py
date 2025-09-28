#!/usr/bin/env python3
# rebuild_database.py - اسکریپت بازسازی کامل دیتابیس

import os
import sys
import time
import requests
import logging
from pathlib import Path
from datetime import datetime

# تنظیم encoding
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

# تنظیمات logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_rebuild.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

BASE_URL = "http://127.0.0.1:5000/api"
HEADERS = {"Content-Type": "application/json"}

def initialize_database():
    """ایجاد tables و کاربر اولیه"""
    try:
        from flask import Flask
        from config import Config
        from models import db, User
        from flask_bcrypt import Bcrypt  # 🔽 استفاده از bcrypt به جای werkzeug
        
        app = Flask(__name__)
        app.config.from_object(Config)
        db.init_app(app)
        bcrypt = Bcrypt(app)
        
        with app.app_context():
            # ایجاد تمام tables
            db.create_all()
            logger.info("✅ Tables دیتابیس ایجاد شدند")
            
            # ایجاد کاربر اولیه اگر وجود ندارد
            if not User.query.filter_by(username='admin').first():
                user = User(
                    username='admin', 
                    email='admin@example.com',
                    hashed_password=bcrypt.generate_password_hash('admin123').decode('utf-8')  # 🔽 استفاده از bcrypt
                )
                db.session.add(user)
                db.session.commit()
                logger.info("✅ کاربر admin ایجاد شد")
            
            return True
            
    except Exception as e:
        logger.error("❌ خطا در initialize دیتابیس: %s", str(e))
        return False

def check_api_available():
    """بررسی در دسترس بودن API"""
    try:
        response = requests.get(f"{BASE_URL}/market-overview/", timeout=5)
        return response.status_code == 200 or response.status_code == 401
    except:
        return False

def wait_for_api(max_retries=30, delay=5):
    """منتظر ماندن برای راه‌اندازی API"""
    logger.info("منتظر راه اندازی API...")
    
    for i in range(max_retries):
        if check_api_available():
            logger.info("API آماده است")
            return True
        
        if i < max_retries - 1:
            logger.info("تلاش %d/%d - منتظر %d ثانیه...", i+1, max_retries, delay)
            time.sleep(delay)
    
    logger.error("API راه اندازی نشد")
    return False

def login_to_api(username, password):
    """ورود به API و دریافت token"""
    try:
        response = requests.post(
            f"{BASE_URL}/auth/login",
            json={"username": username, "password": password},
            headers=HEADERS,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        token = data.get('access_token') or data.get('token')
        if token:
            logger.info("ورود موفقیت آمیز بود")
            return token
        else:
            logger.error("توکن در پاسخ دریافت نشد")
            return None
    except Exception as e:
        logger.error("خطا در ورود به API: %s", str(e))
        return None

def make_api_request(method, endpoint, data=None, token=None, timeout=300):
    """درخواست به API با احراز هویت"""
    url = f"{BASE_URL}{endpoint}"
    headers = HEADERS.copy()
    
    if token:
        headers["Authorization"] = f"Bearer {token}"
    
    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            timeout=timeout
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("خطا در %s %s: %s", method, endpoint, str(e))
        return None

def cleanup_database():
    """پاک کردن داده های دیتابیس"""
    try:
        db_path = os.path.join(project_root, 'app.db')
        if os.path.exists(db_path):
            os.remove(db_path)
            logger.info("فایل دیتابیس پاک شد")
        return True
    except Exception as e:
        logger.error("خطا در پاک کردن دیتابیس: %s", str(e))
        return False

def run_rebuild_steps(token):
    """اجرای مراحل بازسازی"""
    steps = [
        ("پر کردن نمادها", "POST", "/analysis/initial-populate", None)
    ]
    
    for step_name, method, endpoint, data in steps:
        logger.info("در حال اجرای: %s", step_name)
        result = make_api_request(method, endpoint, data, token)
        
        if result:
            logger.info("انجام شد: %s", step_name)
        else:
            logger.warning("خطا در: %s (ادامه می دهیم)", step_name)
        
        time.sleep(3)
    
    return True

def main():
    """تابع اصلی"""
    print("=" * 70)
    print("اسکریپت بازسازی کامل دیتابیس")
    print("=" * 70)
    
    # 1. پاک کردن دیتابیس
    if not cleanup_database():
        return
    
    # 2. ایجاد tables و کاربر اولیه
    if not initialize_database():
        return
    
    # 3. منتظر API
    if not wait_for_api():
        return
    
    # 4. ورود با کاربر پیش‌فرض
    token = login_to_api('admin', 'admin123')
    if not token:
        print("❌ ورود ناموفق بود")
        return
    
    # 5. اجرای مراحل بازسازی
    start_time = datetime.now()
    success = run_rebuild_steps(token)
    end_time = datetime.now()
    
    if success:
        print("✅ بازسازی کامل شد! مدت زمان: ", end_time - start_time)
    else:
        print("❌ خطا در بازسازی!")

if __name__ == "__main__":
    main()