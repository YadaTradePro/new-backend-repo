# update_golden_key_data.py
import os
from datetime import datetime, date
import jdatetime # برای کار با تاریخ شمسی

# تنظیم مسیر فایل دیتابیس
# مطمئن شوید که این مسیر با مسیر واقعی فایل دیتابیس شما مطابقت دارد
# اگر فایل app.db مستقیماً در ریشه پروژه است، از 'app.db' استفاده کنید
# اگر در پوشه 'instance' است، از 'instance/app.db' استفاده کنید
DATABASE_PATH = 'app.db' # مسیر فایل دیتابیس خود را اینجا تنظیم کنید

# تنظیم متغیر محیطی FLASK_APP برای Flask-SQLAlchemy
os.environ['FLASK_APP'] = 'main.py' # نام فایل اصلی اپلیکیشن Flask شما

from main import create_app
from extensions import db
from models import GoldenKeyResult # اطمینان حاصل کنید که مدل GoldenKeyResult وارد شده است

def update_golden_key_statuses():
    """
    Updates the 'status' and 'probability_percent' for existing GoldenKeyResult records.
    Sets status to 'active' for today's records (2025-07-25) and 'closed_neutral' for older records.
    Sets probability_percent to 0.0 if it's NULL.
    """
    app = create_app()
    with app.app_context():
        print("Connecting to database and updating GoldenKeyResult records...")
        
        today_gregorian = datetime(2025, 7, 25).date() # تاریخ امروز را به صورت میلادی تنظیم کنید
        today_jdate_str = jdatetime.date.fromgregorian(date=today_gregorian).strftime('%Y-%m-%d')
        print(f"Today's Jalali date string: {today_jdate_str}")

        try:
            # Update records older than today
            # Assuming jdate is stored as 'YYYY-MM-DD' string
            older_records = GoldenKeyResult.query.filter(GoldenKeyResult.jdate < today_jdate_str).all()
            updated_older_count = 0
            for record in older_records:
                if record.status is None:
                    record.status = 'closed_neutral' # یا 'closed_profit'/'closed_loss' اگر منطق خاصی دارید
                    updated_older_count += 1
                if record.probability_percent is None:
                    record.probability_percent = 0.0
                db.session.add(record)
            print(f"Updated {updated_older_count} older GoldenKeyResult records to 'closed_neutral'.")

            # Update records for today
            today_records = GoldenKeyResult.query.filter(GoldenKeyResult.jdate == today_jdate_str).all()
            updated_today_count = 0
            for record in today_records:
                if record.status is None or record.status != 'active': # اگر null است یا active نیست
                    record.status = 'active'
                    updated_today_count += 1
                if record.probability_percent is None:
                    record.probability_percent = 0.0
                db.session.add(record)
            print(f"Updated {updated_today_count} today's GoldenKeyResult records to 'active'.")

            db.session.commit()
            print("GoldenKeyResult records updated successfully.")

        except Exception as e:
            db.session.rollback()
            print(f"An error occurred during database update: {e}")

if __name__ == "__main__":
    update_golden_key_statuses()
