import os
import time
import logging
from functools import wraps
from main import create_app
from extensions import scheduler

# Import all service functions needed by the scheduler
from services.data_fetch_and_process import run_full_data_update
from services.data_fetch_and_process import run_daily_update
from services.weekly_watchlist_service import run_weekly_watchlist_selection, evaluate_weekly_watchlist_performance
from services.golden_key_service import run_golden_key_analysis_and_save, calculate_golden_key_win_rate
from services.potential_buy_queues_service import run_potential_buy_queue_analysis_and_save
from services.ml_prediction_service import generate_and_save_predictions_for_watchlist, update_ml_prediction_outcomes
from services import market_analysis_service

# ----------------- Logging Setup -----------------
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# Handler برای لاگ کردن در فایل
file_handler = logging.FileHandler("scheduler.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# Handler برای لاگ کردن در کنسول
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

logger = logging.getLogger(__name__)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ----------------- App Context and Error Handling Decorator -----------------
app = create_app()

def with_context_and_error_handling(func):
    """
    Decorator to run a function inside a Flask app context and handle potential exceptions.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with app.app_context():
            logger.info(f"✅ Executing job '{func.__name__}' inside Flask app context.")
            try:
                result = func(*args, **kwargs)
                logger.info(f"✅ Job '{func.__name__}' completed successfully.")
                return result
            except Exception as e:
                logger.error(f"❌ An error occurred while running job '{func.__name__}': {e}", exc_info=True)
    return wrapper

# ----------------- Job Definitions -----------------
JOBS = [
    # 🟢 وظایف روزانه
    {"id": "daily_light_update_job", "func": with_context_and_error_handling(run_daily_update), "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 17, "minute": 0, "coalesce": True, "max_instances": 1}, # ✅ تنها این Job سنگین در نظر گرفته شد.
    {"id": "generate_daily_summary_job", "func": with_context_and_error_handling(market_analysis_service.generate_market_summary), "trigger": "cron", "day_of_week": "sat, sun, mon, tue, wed", "hour": 23, "minute": 15},
    
    # 🟡 وظایف هفتگی
    {"id": "update_exit_prices_job", "func": with_context_and_error_handling(market_analysis_service.update_evaluated_prices_job), "trigger": "cron", "day_of_week": "thu", "hour": 4, "minute": 0},
    {"id": "weekly_watchlist_selection_job", "func": with_context_and_error_handling(run_weekly_watchlist_selection), "trigger": "cron", "day_of_week": "wed", "hour": 21, "minute": 30},
    {"id": "run_golden_key_filters_job", "func": with_context_and_error_handling(run_golden_key_analysis_and_save), "trigger": "cron", "day_of_week": "wed", "hour": 20, "minute": 30},
    
    # ⚪️ سایر وظایف
    {"id": "calculate_golden_key_win_rate_job", "func": with_context_and_error_handling(calculate_golden_key_win_rate), "trigger": "cron", "day_of_week": "wed", "hour": 20, "minute": 0},
    {"id": "weekly_watchlist_performance_job", "func": with_context_and_error_handling(evaluate_weekly_watchlist_performance), "trigger": "cron", "day_of_week": "wed", "hour": 21, "minute": 0},
    {"id": "potential_buy_queues_job", "func": with_context_and_error_handling(run_potential_buy_queue_analysis_and_save), "trigger": "cron", "hour": 7, "minute": 30},
    {"id": "generate_ml_predictions_job", "func": with_context_and_error_handling(generate_and_save_predictions_for_watchlist), "trigger": "cron", "day_of_week": "thu", "hour": 3, "minute": 0, "coalesce": True, "max_instances": 1}, # ✅ همچنین برای Job های سنگین ML
    {"id": "run-maintenance-update", "func": with_context_and_error_handling(run_full_data_update), "trigger": "cron", "day": 1, "hour": 16, "minute": 50, "coalesce": True, "max_instances": 1},
    {"id": "update_ml_outcomes_job", "func": with_context_and_error_handling(update_ml_prediction_outcomes), "trigger": "cron", "hour": 8, "minute": 0},
]

TIMEZONE = "Asia/Tehran"

# ----------------- Scheduler Runner -----------------
def run_scheduler_app():
    """Runs the APScheduler in a standalone process."""
    app.config["SCHEDULER_RUN"] = True
    
    scheduler.init_app(app)

    for job in JOBS:
        try:
            scheduler.add_job(
                id=job["id"],
                func=job["func"],
                trigger=job["trigger"],
                replace_existing=True,
                timezone=TIMEZONE,
                **{k: v for k, v in job.items() if k not in ["id", "func", "trigger"]}
            )
            logger.info(f"✅ Job registered: {job['id']}")
        except Exception as e:
            logger.error(f"❌ Failed to add job {job['id']}: {e}")

    scheduler.start()
    logger.info("🚀 APScheduler started in a separate process.")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("🛑 Scheduler has been shut down.")

if __name__ == "__main__":
    run_scheduler_app()