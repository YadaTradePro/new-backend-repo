# -*- coding: utf-8 -*-
import os
import sys
import logging
from flask import Flask, jsonify, request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity 
from extensions import db, bcrypt, jwt 
from flask_restx import Api, Namespace, Resource, fields # ADDED: Namespace, Resource, fields for settings_ns
from flask_cors import CORS
from flask_apscheduler import APScheduler 
from flask_migrate import Migrate 
from datetime import datetime, date
import jdatetime
import pytz
import models
import click # --- این خط جدید است ---
from services.ml_prediction_service import generate_and_save_predictions_for_watchlist 

logger = logging.getLogger(__name__)
scheduler = APScheduler() 

def create_app():
    app = Flask(__name__)

    # پیکربندی لاگینگ
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    
    logging.getLogger('services.golden_key_service').setLevel(logging.DEBUG)
    logging.getLogger('services.data_fetch_and_process').setLevel(logging.DEBUG)
    logging.getLogger('services.potential_buy_queues_service').setLevel(logging.DEBUG)
    logging.getLogger('services.performance_service').setLevel(logging.DEBUG) 
    logging.getLogger('services.ml_prediction_service').setLevel(logging.DEBUG) 

    app.logger.setLevel(logging.DEBUG)
    
    app.config.from_object('config.Config')

    cors_origins = [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://127.0.0.1:5000",
        "http://192.168.1.6:8000",  # Frontend on your phone (optional)
        "http://192.168.1.6:5000",  # Backend address for external access
        "http://10.0.2.2:5000",
    ]
    CORS(app, resources={r"/api/*": {"origins": cors_origins}}, supports_credentials=True)

    db.init_app(app)
    jwt.init_app(app)
    bcrypt.init_app(app)

    migrate = Migrate(app, db) 

    authorizations = {
        'Bearer Auth': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'Authorization',
            'description': "JWT Authorization header using the Bearer scheme. Example: \"Authorization: Bearer {token}\""
        }
    }

    main_api = Api(
        app,
        version='1.0',
        title='Bourse Analysis API',
        description='API for Bourse Analysis with Technical, Fundamental data and User Management',
        doc='/api/swagger-ui/',
        prefix='/api',
        security='Bearer Auth',
        authorizations=authorizations
    )

    with app.app_context():
        db.create_all() 

        try:
            import pytse_client as tse_check
            current_app.logger.info("ماژول pytse-client با موفقیت وارد شد و در دسترس است.")
            app.config['PYTSE_CLIENT_AVAILABLE'] = True
        except ImportError:
            current_app.logger.error("خطا: ماژول pytse-client پیدا نشد. لطفا آن را با 'pip install pytse-client' نصب کنید. برنامه بدون دسترسی به داده‌های واقعی بازار ادامه خواهد یافت.")
            app.config['PYTSE_CLIENT_AVAILABLE'] = False
        except Exception as e:
            current_app.logger.error(f"خطای ناشناخته در وارد کردن pytse-client: {e}. برنامه بدون دسترسی به داده‌های واقعی بازار ادامه خواهد یافت.")
            app.config['PYTSE_CLIENT_AVAILABLE'] = False

    # Import ALL Namespaces from their respective routes files
    from routes.auth import auth_ns
    from routes.analysis import analysis_ns 
    from routes.market_data import market_overview_ns 
    from routes.golden_key import golden_key_ns       
    from routes.weekly_watchlist import weekly_watchlist_ns 
    from routes.potential_queues import potential_queues_ns 
    from routes.performance import performance_ns     
    # from routes.settings import settings_ns # REMOVED: Since settings_ns is now defined directly here

    # Settings Namespace (Defined directly in main.py, as no service file exists yet)
    settings_ns = Namespace('settings', description='User settings operations')
    @settings_ns.route('/')
    class SettingsResource(Resource):
        @settings_ns.doc(security='Bearer Auth')
        @jwt_required() 
        def get(self):
            return {"message": "Settings endpoint. Not yet implemented."}, 200
    # The add_namespace call for settings_ns must come AFTER its definition
    main_api.add_namespace(settings_ns, path='/settings') # MOVED: This line is now correctly placed


    # Import services for scheduler jobs (these are still needed here for func=lambda)
    from services.data_fetch_and_process import initial_populate_all_symbols_and_data
    from services.weekly_watchlist_service import run_weekly_watchlist_selection, evaluate_weekly_watchlist_performance 
    from services.golden_key_service import run_golden_key_analysis_and_save, calculate_golden_key_win_rate
    from services.potential_buy_queues_service import run_potential_buy_queue_analysis_and_save
    from services.ml_prediction_service import generate_and_save_predictions_for_watchlist, update_ml_prediction_outcomes


    # Add ALL Namespaces to the main API object (ensure settings_ns is added here too)
    main_api.add_namespace(auth_ns, path='/auth')
    main_api.add_namespace(analysis_ns, path='/analysis') 
    main_api.add_namespace(market_overview_ns, path='/market_overview') 
    main_api.add_namespace(golden_key_ns, path='/golden_key')       
    main_api.add_namespace(weekly_watchlist_ns, path='/weekly_watchlist') 
    main_api.add_namespace(potential_queues_ns, path='/potential_queues') 
    main_api.add_namespace(performance_ns, path='/performance')     
    # settings_ns is already added above, no need to add again here.


    # Add scheduled jobs
    scheduler.init_app(app) 

    with app.app_context():
        # Define and add jobs only if not already running
        scheduler.add_job(
            id='full_data_update_job',
            func=lambda: app.app_context().push() or initial_populate_all_symbols_and_data(),
            trigger='cron', hour=3, minute=0, timezone='Asia/Tehran', replace_existing=True
        )
        logger.info("Added job 'full_data_update_job' to job store 'default'")

        scheduler.add_job(
            id='weekly_watchlist_selection_job',
            func=lambda: app.app_context().push() or run_weekly_watchlist_selection(), 
            trigger='cron', day_of_week='thu', hour=18, minute=0, timezone='Asia/Tehran', replace_existing=True
        )
        logger.info("Added job 'weekly_watchlist_selection_job' to job store 'default'")

        scheduler.add_job(
            id='run_golden_key_filters_job',
            func=lambda: app.app_context().push() or run_golden_key_analysis_and_save(),
            trigger='cron', day_of_week='thu', hour=19, minute=0, timezone='Asia/Tehran', replace_existing=True
        )
        logger.info("Added job 'run_golden_key_filters_job' to job store 'default'")

        scheduler.add_job(
            id='calculate_golden_key_win_rate_job',
            func=lambda: app.app_context().push() or calculate_golden_key_win_rate(),
            trigger='cron', day_of_week='thu', hour=1, minute=0, timezone='Asia/Tehran', replace_existing=True
        )
        logger.info("Added job 'calculate_golden_key_win_rate_job' to job store 'default'")

        scheduler.add_job(
            id='weekly_watchlist_performance_job',
            func=lambda: app.app_context().push() or evaluate_weekly_watchlist_performance(), 
            trigger='cron', day_of_week='thu', hour=2, minute=00, timezone='Asia/Tehran', replace_existing=True
        )
        logger.info("Added job 'weekly_watchlist_performance_job' to job store 'default'")
        
        scheduler.add_job(
            id='potential_buy_queues_job',
            func=lambda: app.app_context().push() or run_potential_buy_queue_analysis_and_save(), 
            trigger='cron', hour=7, minute=30, timezone='Asia/Tehran', replace_existing=True
        )
        logger.info("Added job 'potential_buy_queues_job' to job store 'default'")

        # --- بازبینی Job تولید پیش‌بینی‌های ML ---
        scheduler.add_job(
            id='generate_ml_predictions_job', 
            # ارسال آرگومان‌ها به تابع generate_and_save_predictions_for_watchlist
            func=lambda: app.app_context().push() or generate_and_save_predictions_for_watchlist(None, 7), 
            trigger='cron', day_of_week='thu', hour=3, minute=0, timezone='Asia/Tehran', replace_existing=True
        )
        app.logger.info("Added job 'generate_ml_predictions_job' to job store 'default'")
        
        # --- بازبینی Job به‌روزرسانی نتایج ML ---
        scheduler.add_job(
            id='update_ml_outcomes_job', 
            func=lambda: app.app_context().push() or update_ml_prediction_outcomes(), 
            trigger='cron', hour=8, minute=0, timezone='Asia/Tehran', replace_existing=True
        )
        app.logger.info("Added job 'update_ml_outcomes_job' to job store 'default'")
        
    app.logger.info("زمان‌بندی‌کننده APScheduler راه‌اندازی و وظایف اضافه شدند.")

    migrate = Migrate(app, db)


    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        if not scheduler.running:
            scheduler.start()
            logger.info("Scheduler با موفقیت شروع به کار کرد (درون create_app، فرآیند اصلی).")
        else:
            logger.info("Scheduler از قبل در حال اجرا بود (درون create_app، فرآیند اصلی).")
    else:
        logger.info("فرآیند ریلودر Werkzeug: Scheduler شروع نمی‌شود (درون create_app، فرآیند والد).")


    @jwt.unauthorized_loader
    def unauthorized_response(callback):
        return jsonify({"message": "توکن احراز هویت موجود نیست یا نامعتبر است."}), 401

    @jwt.invalid_token_loader
    def invalid_token_response(callback):
        app.logger.error(f"خطای توکن نامعتبر: {callback}")
        return jsonify({"message": "اعتبار سنجی امضای توکن انجام نشد."}), 403

    @jwt.expired_token_loader
    def expired_token_response(jwt_header, jwt_data):
        current_app.logger.warning(f"Expired token detected. Header: {jwt_header}, Data: {jwt_data}")
        return jsonify({"message": "Your session has expired. Please log in again.", "code": "token_expired"}), 401

    @app.route('/')
    def home():
        return jsonify({
            "message": "به API تحلیل بورس Flask خوش آمدید! مستندات API در /api/swagger-ui/ در دسترس است."
        })


    # --- اضافه کردن دستورات CLI ---
    @app.cli.command('generate-ml-predictions')
    @click.option('--date', default=None, help='تاریخ پیش‌بینی به فرمت YYYY-MM-DD (اختیاری، پیش‌فرض: امروز).')
    @click.option('--period', default=7, type=int, help='افق پیش‌بینی بر حسب روز (پیش‌فرض: 7).')
    def generate_predictions_command(date, period):
        """تولید و ذخیره پیش‌بینی‌های ML برای نمادها."""
        if date:
            try:
                prediction_date = datetime.strptime(date, '%Y-%m-%d').date()
            except ValueError:
                click.echo("خطا: فرمت تاریخ نامعتبر است. لطفاً از YYYY-MM-DD استفاده کنید.")
                return
        else:
            prediction_date = None # استفاده از تاریخ امروز در سرویس
        
        with app.app_context():
            success, message = generate_and_save_predictions_for_watchlist(
                prediction_date_greg=prediction_date, 
                prediction_period_days=period
            )
            if success:
                click.echo(f"موفقیت: {message}")
            else:
                click.echo(f"خطا: {message}")


    return app

if __name__ == '__main__':
    app = create_app()
    port = int(os.environ.get('PORT', 5000))
    print(f"برنامه Flask روی http://0.0.0.0:{port} در حال اجرا است...")
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)

