# scheduler.py

import os
import logging
from main import create_app
from extensions import scheduler

# Import all service functions needed by the scheduler
from services.data_fetch_and_process import initial_populate_all_symbols_and_data
from services.weekly_watchlist_service import run_weekly_watchlist_selection, evaluate_weekly_watchlist_performance 
from services.golden_key_service import run_golden_key_analysis_and_save, calculate_golden_key_win_rate
from services.potential_buy_queues_service import run_potential_buy_queue_analysis_and_save
from services.ml_prediction_service import generate_and_save_predictions_for_watchlist, update_ml_prediction_outcomes

# Setup logging for the scheduler process
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_scheduler_app():
    """Runs the APScheduler in a standalone process."""
    app = create_app()
    
    # This config signals to main.py to NOT start the scheduler
    app.config['SCHEDULER_RUN'] = True
    
    with app.app_context():
        # Initialize and add jobs
        scheduler.init_app(app)
        
        # Add scheduled jobs (identical to the jobs in main.py)
        scheduler.add_job(id='full_data_update_job', func=initial_populate_all_symbols_and_data, trigger='cron', hour=3, minute=0, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='weekly_watchlist_selection_job', func=run_weekly_watchlist_selection, trigger='cron', day_of_week='thu', hour=18, minute=0, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='run_golden_key_filters_job', func=run_golden_key_analysis_and_save, trigger='cron', day_of_week='thu', hour=19, minute=0, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='calculate_golden_key_win_rate_job', func=calculate_golden_key_win_rate, trigger='cron', day_of_week='thu', hour=1, minute=0, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='weekly_watchlist_performance_job', func=evaluate_weekly_watchlist_performance, trigger='cron', day_of_week='thu', hour=2, minute=00, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='potential_buy_queues_job', func=run_potential_buy_queue_analysis_and_save, trigger='cron', hour=7, minute=30, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='generate_ml_predictions_job', func=generate_and_save_predictions_for_watchlist, trigger='cron', day_of_week='thu', hour=3, minute=0, timezone='Asia/Tehran', replace_existing=True)
        scheduler.add_job(id='update_ml_outcomes_job', func=update_ml_prediction_outcomes, trigger='cron', hour=8, minute=0, timezone='Asia/Tehran', replace_existing=True)
        
        scheduler.start()
        logger.info("APScheduler has started in a separate process.")
        
    try:
        # Keep the process alive
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler has been shut down.")

if __name__ == '__main__':
    run_scheduler_app()