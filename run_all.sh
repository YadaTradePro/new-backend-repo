#!/bin/bash
# شروع کردن سرویس زمان‌بندی (Scheduler) در پس زمینه
echo "Starting scheduler service..."
python scheduler.py &


# شروع کردن اسکریپت اسکرپینگ TGJU در پس زمینه
echo "Starting TGJU scraper..."
python services/tgju.py &
sleep 3  # کمی صبر می‌کنیم تا سرور بالا بیاید

# شروع کردن Flask app در پس زمینه
echo "Starting Flask web server..."
python main.py &


# این دستور تضمین می‌کند که اسکریپت Shell فعال بماند تا زمانی که همه فرآیندها در حال اجرا هستند.
# این کار مانع از بسته شدن Replit می‌شود.
wait