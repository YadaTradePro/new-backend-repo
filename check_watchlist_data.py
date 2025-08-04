# check_watchlist_data.py
import os
import sys

# افزودن مسیر پروژه به sys.path تا ماژول‌های داخلی پیدا شوند
# این خط فرض می‌کند که شما این اسکریپت را در ریشه پروژه خود اجرا می‌کنید
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# مطمئن شوید که Flask-App-Context به درستی تنظیم شده است.
# برای اجرای اسکریپت‌های خارج از محیط Flask CLI
from flask import Flask
from extensions import db
from models import WeeklyWatchlistResult, HistoricalData 

# یک نمونه کوچک از اپلیکیشن Flask ایجاد می‌کنیم تا context فعال شود
# این فقط برای دسترسی به دیتابیس است و سرور را اجرا نمی‌کند
def create_minimal_app():
    app = Flask(__name__)
    # مسیر دیتابیس را اینجا تنظیم کنید. باید با config.Config شما مطابقت داشته باشد.
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///E:/BourseAnalysisFlask/app.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    return app

app = create_minimal_app()

with app.app_context():
    print("--- Fetching data from WeeklyWatchlistResult table ---")
    results = WeeklyWatchlistResult.query.all()
    
    if results:
        for r in results:
            print(f"ID: {r.id}, Signal Unique ID: {r.signal_unique_id}, Symbol: {r.symbol}, Name: {r.symbol_name}, Entry Price: {r.entry_price}, JEntry Date: {r.jentry_date}, Status: {r.status}, Exit Price: {r.exit_price}, JExit Date: {r.jexit_date}, P/L %: {r.profit_loss_percentage}")
            
            # همچنین داده‌های تاریخی آخرین قیمت را برای این نماد بررسی می‌کنیم
            latest_historical_data = HistoricalData.query.filter_by(symbol_id=r.symbol)\
                                                        .order_by(HistoricalData.jdate.desc())\
                                                        .first()
            if latest_historical_data:
                print(f"  -> Latest Historical Data for {r.symbol_name} (Symbol ID: {r.symbol}): JDate={latest_historical_data.jdate}, Final Price={latest_historical_data.final}, Close Price={latest_historical_data.close}")
            else:
                print(f"  -> NO Historical Data found for {r.symbol_name} (Symbol ID: {r.symbol})")
            print("-" * 50) # جداکننده
    else:
        print("No records found in WeeklyWatchlistResult table.")

print("\n--- Data fetching complete ---")
