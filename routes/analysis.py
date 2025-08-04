# routes/analysis.py
from flask_restx import Namespace, Resource, fields, reqparse
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask import request, current_app
from flask_cors import cross_origin

# از db و سایر مدل‌ها از extensions و models وارد کنید
from extensions import db
from models import (
    User, HistoricalData, ComprehensiveSymbolData, SignalsPerformance, FundamentalData,
    TechnicalIndicatorData, MLPrediction # Removed models now in other route files
)

# Import services relevant to analysis_ns only
from services import data_fetch_and_process
from services.ml_prediction_service import get_ml_predictions_for_symbol, get_all_ml_predictions, generate_and_save_predictions_for_watchlist

# Import func from sqlalchemy for database operations
from sqlalchemy import func 
from sqlalchemy import or_ 

analysis_ns = Namespace('analysis', description='Stock data analysis and fetching operations')

# --- API Models for Flask-RESTX Documentation ---
# این مدل‌ها برای مستندسازی Swagger UI استفاده می‌شوند.
# مطمئن شوید که فیلدهای این مدل‌ها با فیلدهای خروجی توابع سرویس شما مطابقت دارند.

historical_data_model = analysis_ns.model('HistoricalData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'symbol_name': fields.String(description='Stock symbol name (Persian short name)'),
    'jdate': fields.String(description='Persian date (YYYY-MM-DD)'),
    'date': fields.String(description='Gregorian date (YYYY-MM-DD)'),
    'open': fields.Float(description='Opening price'),
    'high': fields.Float(description='Highest price'),
    'low': fields.Float(description='Lowest price'),
    'close': fields.Float(description='Closing price'),
    'final': fields.Float(description='Final price'),
    'yesterday_price': fields.Float(description='Yesterday\'s closing price'),
    'volume': fields.Integer(description='Trading volume'),
    'value': fields.Float(description='Trading value'),
    'num_trades': fields.Integer(description='Number of trades'),
    'plc': fields.Float(description='Price change (last closing)'),
    'plp': fields.Float(description='Price change percentage (last closing)'),
    'pcc': fields.Float(description='Price change (final closing)'),
    'pcp': fields.Float(description='Price change percentage (final closing)'),
    'mv': fields.Float(description='Market Value'),
    'eps': fields.Float(description='Earnings Per Share'),
    'pe': fields.Float(description='Price to Earnings Ratio'),
    'buy_count_i': fields.Integer(description='Number of real buyer accounts'),
    'buy_count_n': fields.Integer(description='Number of legal buyer accounts'),
    'sell_count_i': fields.Integer(description='Number of real seller accounts'),
    'sell_count_n': fields.Integer(description='Number of legal seller accounts'),
    'buy_i_volume': fields.Integer(description='Real buyer volume'),
    'buy_n_volume': fields.Integer(description='Legal buyer volume'),
    'sell_i_volume': fields.Integer(description='Real seller volume'),
    'sell_n_volume': fields.Integer(description='Legal seller volume'),
    'zd1': fields.Integer(description='Demand count 1'),
    'qd1': fields.Integer(description='Demand volume 1'),
    'pd1': fields.Float(description='Demand price 1'),
    'zo1': fields.Integer(description='Supply count 1'),
    'qo1': fields.Integer(description='Supply volume 1'),
    'po1': fields.Float(description='Supply price 1'),
    'zd2': fields.Integer(description='Demand count 2'),
    'qd2': fields.Integer(description='Demand volume 2'),
    'pd2': fields.Float(description='Demand price 2'),
    'zo2': fields.Integer(description='Supply count 2'),
    'qo2': fields.Integer(description='Supply volume 2'),
    'po2': fields.Float(description='Supply price 2'),
    'zd3': fields.Integer(description='Demand count 3'),
    'qd3': fields.Integer(description='Demand volume 3'),
    'pd3': fields.Float(description='Demand price 3'),
    'zo3': fields.Integer(description='Supply count 3'),
    'qo3': fields.Integer(description='Supply volume 3'),
    'po3': fields.Float(description='Supply price 3'),
    'zd4': fields.Integer(description='Demand count 4'),
    'qd4': fields.Integer(description='Demand volume 4'),
    'pd4': fields.Float(description='Demand price 4'),
    'zo4': fields.Integer(description='Supply count 4'),
    'qo4': fields.Integer(description='Supply volume 4'),
    'po4': fields.Float(description='Supply price 4'),
    'zd5': fields.Integer(description='Demand count 5'),
    'qd5': fields.Integer(description='Demand volume 5'),
    'pd5': fields.Float(description='Demand price 5'),
    'zo5': fields.Integer(description='Supply count 5'),
    'qo5': fields.Integer(description='Supply volume 5'),
    'po5': fields.Float(description='Supply price 5')
})

comprehensive_symbol_data_model = analysis_ns.model('ComprehensiveSymbolData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'symbol_name': fields.String(required=True, description='Stock symbol name (Persian short name)'),
    'company_name': fields.String(description='Company name'),
    'isin': fields.String(description='ISIN code'),
    'market_type': fields.String(description='Market type'),
    'flow': fields.String(description='Flow (e.g., 1 for main market, 2 for secondary)'),
    'industry': fields.String(description='Industry name'),
    'capital': fields.String(description='Company capital'),
    'legal_shareholder_percentage': fields.Float(description='Legal Shareholder Percentage'),
    'real_shareholder_percentage': fields.Float(description='Real Shareholder Percentage'),
    'float_shares': fields.Float(description='Float shares'),
    'base_volume': fields.Float(description='Base volume'),
    'group_name': fields.String(description='Group name'),
    'description': fields.String(description='Symbol description'),
    'last_historical_update_date': fields.String(description='Last historical update date (YYYY-MM-DD)')
})

# Model for TechnicalIndicatorData
technical_indicator_model = analysis_ns.model('TechnicalIndicatorData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'jdate': fields.String(required=True, description='Persian date (YYYY-MM-DD)'),
    'close_price': fields.Float(description='Closing price'),
    'RSI': fields.Float(description='Relative Strength Index'),
    'MACD': fields.Float(description='Moving Average Convergence Divergence'),
    'MACD_Signal': fields.Float(description='MACD Signal Line'),
    'MACD_Hist': fields.Float(description='MACD Histogram'),
    'SMA_20': fields.Float(description='20-day Simple Moving Average'),
    'SMA_50': fields.Float(description='50-day Simple Moving Average'),
    'Bollinger_High': fields.Float(description='Bollinger Band Upper'),
    'Bollinger_Low': fields.Float(description='Bollinger Band Lower'),
    'Bollinger_MA': fields.Float(description='Bollinger Band Middle (20-day MA)'),
    'Volume_MA_20': fields.Float(description='20-day Moving Average of Volume'),
    'ATR': fields.Float(description='Average True Range') # Added ATR to the model
})

# Model for FundamentalData
fundamental_data_model = analysis_ns.model('FundamentalData', {
    'symbol_id': fields.String(required=True, description='Stock symbol ID (Persian short name)'),
    'last_updated': fields.DateTime(description='Last update timestamp'),
    'eps': fields.Float(description='Earnings Per Share'),
    'pe_ratio': fields.Float(description='Price-to-Earnings Ratio'),
    'group_pe_ratio': fields.Float(description='Group Price-to-Earnings Ratio'),
    'psr': fields.Float(description='Price-to-Sales Ratio (PSR)'),
    'p_s_ratio': fields.Float(description='Price-to-Sales Ratio (P/S)'),
    'market_cap': fields.Float(description='Market Capitalization'),
    'base_volume': fields.Float(description='Base Volume'),
    'float_shares': fields.Float(description='Float Shares')
})

# NEW: Model for ML Predictions (ADDED)
ml_prediction_model = analysis_ns.model('MLPredictionModel', {
    'id': fields.Integer(readOnly=True, description='The unique identifier of the prediction'),
    'symbol_id': fields.String(required=True, description='The ID of the stock symbol'),
    'symbol_name': fields.String(required=True, description='The name of the stock symbol'),
    'prediction_date': fields.String(required=True, description='Gregorian date when the prediction was made (YYYY-MM-DD)'),
    'jprediction_date': fields.String(required=True, description='Jalali date when the prediction was made (YYYY-MM-DD)'),
    'prediction_period_days': fields.Integer(description='Number of days for the prediction horizon'),
    'predicted_trend': fields.String(required=True, description='Predicted trend: UP, DOWN, or NEUTRAL'),
    'prediction_probability': fields.Float(required=True, description='Probability/confidence of the predicted trend (0.0 to 1.0)'),
    'predicted_price_at_period_end': fields.Float(description='Optional: Predicted price at the end of the period'),
    'actual_price_at_period_end': fields.Float(description='Actual price at the end of the prediction period'),
    'actual_trend_outcome': fields.String(description='Actual trend outcome: UP, DOWN, or NEUTRAL'),
    'is_prediction_accurate': fields.Boolean(description='True if predicted_trend matches actual_trend_outcome'),
    'signal_source': fields.String(description='Source of the signal, e.g., ML-Trend'),
    'model_version': fields.String(description='Version of the ML model used for prediction'),
    'created_at': fields.String(description='Timestamp of creation'),
    'updated_at': fields.String(description='Timestamp of last update'),
})


# --- Parsers for API Endpoints ---

# Parser for data update endpoint (for update_historical_data_limited)
data_update_parser = reqparse.RequestParser()
data_update_parser.add_argument('days_limit', type=int, default=200, help='Number of historical days to fetch for each symbol.')
data_update_parser.add_argument('limit_per_run', type=int, help='Maximum number of symbols to process in this run.')
data_update_parser.add_argument('specific_symbols_list', type=str, action='append', help='List of specific symbol IDs to update.')

# NEW PARSER: For run_full_data_update
full_data_update_parser = reqparse.RequestParser()
full_data_update_parser.add_argument('days_limit', type=int, default=365, help='Number of historical days to fetch for each symbol for full update.')


# --- API Resources ---

@analysis_ns.route('/initial-populate-all-symbols')
class InitialSymbolPopulationResource(Resource):
    @analysis_ns.doc(security='Bearer Auth')
    @jwt_required()
    @analysis_ns.response(200, 'Initial symbol population process initiated.')
    @analysis_ns.response(500, 'Error during initial population.')
    def post(self):
        """
        Triggers the initial population of ComprehensiveSymbolData from pytse-client
        and fetches historical/technical data for them.
        This should typically be run once to seed the database.
        """
        current_app.logger.info("API call: Initiating initial symbol population and data fetch.")
        try:
            total_processed_count, msg_text = data_fetch_and_process.initial_populate_all_symbols_and_data()
            return {"message": msg_text, "processed_symbols_count": total_processed_count}, 200
        except Exception as e:
            current_app.logger.error(f"Error during initial population: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500

# NEW ENDPOINT: Full Data Update
@analysis_ns.route('/run-full-data-update')
class FullDataUpdateResource(Resource):
    @analysis_ns.doc(security='Bearer Auth')
    @jwt_required()
    @analysis_ns.expect(full_data_update_parser)
    @analysis_ns.response(200, 'Full data update process initiated.')
    @analysis_ns.response(500, 'Error during full data update.')
    def post(self):
        """
        Triggers a full data update for all symbols (historical, technical, fundamental).
        This should be run periodically (e.g., daily).
        """
        args = full_data_update_parser.parse_args()
        days_limit = args['days_limit']
        
        current_app.logger.info(f"API call: Initiating full data update for all symbols for the last {days_limit} days.")
        try:
            processed_count, message = data_fetch_and_process.run_full_data_update(days_limit=days_limit)
            return {"message": message, "processed_count": processed_count}, 200
        except Exception as e:
            current_app.logger.error(f"Error during full data update: {e}", exc_info=True)
            return {"message": f"An error occurred: {str(e)}"}, 500


@analysis_ns.route('/update-historical-data')
class UpdateHistoricalDataResource(Resource):
    @jwt_required()
    @analysis_ns.doc(security='Bearer Auth')
    @analysis_ns.param('limit_per_run', 'Limit the number of symbols to update in this run (default: 100)', type=int, _in='query', default=100)
    @analysis_ns.param('specific_symbols', 'Comma-separated list of specific symbol names (Persian) or IDs (ISIN) to update (e.g., خودرو,فملی,IRO1KHODRO0001)', type=str, _in='query')
    @analysis_ns.response(200, 'Historical data update triggered successfully')
    @analysis_ns.response(500, 'Error during historical data update')
    def post(self):
        """
        Triggers an update for historical data for a limited number of symbols
        (or specific symbols) from pytse-client.
        """
        parser = reqparse.RequestParser()
        parser.add_argument('limit_per_run', type=int, default=100, help='Limit the number of symbols to update in this run (default: 100)')
        parser.add_argument('specific_symbols', type=str, help='Comma-separated list of specific symbol names (Persian) or IDs (ISIN) to update (e.g., خودرو,فملی,IRO1KHODRO0001)', location='args')
        args = parser.parse_args()

        limit_per_run = args['limit_per_run']
        specific_symbols_str = args['specific_symbols']
        specific_symbols_list = [s.strip() for s in specific_symbols_str.split(',') if s.strip()] if specific_symbols_str else None

        try:
            # Corrected function call to data_fetch_and_process.run_full_data_update
            # Assuming update_historical_data_limited is now part of run_full_data_update
            total_rows, msg_text = data_fetch_and_process.run_full_data_update(
                limit_per_run=limit_per_run, specific_symbols_list=specific_symbols_list
            )
            response_data = {"message": msg_text, "updated_or_inserted_rows": total_rows}
            return response_data, 200
        except Exception as e:
            current_app.logger.error(f"Error in UpdateHistoricalDataResource.post: {e}", exc_info=True)
            error_response_data = {
                "message": f"An internal server error occurred: {str(e)}",
                "error_type": e.__class__.__name__
            }
            return error_response_data, 500


@analysis_ns.route('/historical_data/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name, e.g., خودرو) or ISIN (e.g., IRO1KHODRO0001)')
class HistoricalDataResource(Resource):
    @jwt_required()
    @analysis_ns.doc(security='Bearer Auth')
    @analysis_ns.param('limit', 'Limit the number of historical records returned (e.g., 10, 100). Default returns all.', type=int, _in='query')
    @analysis_ns.marshal_list_with(historical_data_model)
    @analysis_ns.response(200, 'Historical data fetched successfully')
    @analysis_ns.response(404, 'No historical data found for the symbol')
    def get(self, symbol_input):
        """Fetches historical data for a given stock symbol from the database, with optional limit."""
        symbol_id = data_fetch_and_process.get_symbol_id(symbol_input)
        if not symbol_id:
            analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

        parser = reqparse.RequestParser()
        parser.add_argument('limit', type=int, help='Limit the number of historical records returned')
        args = parser.parse_args()
        limit = args['limit']

        query = HistoricalData.query.filter_by(symbol_id=symbol_id).order_by(HistoricalData.date.desc())
        if limit:
            historical_records = query.limit(limit).all()
        else:
            historical_records = query.all()

        if not historical_records:
            current_app.logger.warning(f"No historical data found for symbol_id: {symbol_id}")
            analysis_ns.abort(404, f"No historical data found for symbol_id: {symbol_id}")
        return historical_records


@analysis_ns.route('/fundamental_data/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name) or ISIN')
class FundamentalDataResource(Resource):
    @jwt_required()
    @analysis_ns.marshal_with(fundamental_data_model)
    @analysis_ns.response(200, 'Fundamental data fetched successfully')
    @analysis_ns.response(404, 'No fundamental data found for the symbol')
    @analysis_ns.doc(security='Bearer Auth')
    def get(self, symbol_input):
        """Fetches fundamental data for a given stock symbol."""
        symbol_id = data_fetch_and_process.get_symbol_id(symbol_input)
        if not symbol_id:
            analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

        fundamental_data = FundamentalData.query.filter_by(symbol_id=symbol_id).first()
        if not fundamental_data:
            # Attempt to fetch and save fundamental data if not found in DB
            # This now calls the specific update_fundamental_data function
            success, msg = data_fetch_and_process.update_fundamental_data(symbol_id, symbol_id) 
            if success:
                fundamental_data = FundamentalData.query.filter_by(symbol_id=symbol_id).first()
                if fundamental_data:
                    return fundamental_data, 200
            analysis_ns.abort(404, f"No fundamental data found for symbol_id: {symbol_id} after attempted fetch.")
        return fundamental_data

@analysis_ns.route('/trigger_fundamental_update/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name) or ISIN')
class TriggerFundamentalUpdate(Resource):
    @jwt_required()
    @analysis_ns.response(200, 'Fundamental data update triggered successfully')
    @analysis_ns.response(500, 'Error during fundamental data update')
    @analysis_ns.doc(security='Bearer Auth')
    def post(self, symbol_input):
        """Trigger update for fundamental data for a symbol."""
        symbol_id = data_fetch_and_process.get_symbol_id(symbol_input)
        if not symbol_id:
            analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

        current_app.logger.info(f"Triggered fundamental data update for {symbol_id}.")
        # Call the specific service function for fundamental data update
        success, message = data_fetch_and_process.update_fundamental_data(symbol_id, symbol_id) 
        
        if success:
            return {"message": message}, 200
        else:
            return {"message": message}, 500

@analysis_ns.route('/analyze_technical_indicators/<string:symbol_input>')
@analysis_ns.param('symbol_input', 'The stock symbol ID (Persian short name) or ISIN')
@analysis_ns.param('days', 'Number of recent days to fetch and analyze (default: 365)')
class TechnicalIndicatorsResource(Resource):
    @jwt_required()
    @analysis_ns.marshal_list_with(technical_indicator_model)
    @analysis_ns.response(200, 'Technical indicators calculated successfully')
    @analysis_ns.response(404, 'No historical data found for the symbol')
    @analysis_ns.doc(security='Bearer Auth')
    def get(self, symbol_input):
        """
        Fetches historical data, calculates various technical indicators,
        saves them to the database, and returns the recent results.
        """
        symbol_id = data_fetch_and_process.get_symbol_id(symbol_input)
        if not symbol_id:
            analysis_ns.abort(404, f"Invalid symbol ID or name: {symbol_input}")

        parser = reqparse.RequestParser()
        parser.add_argument('days', type=int, default=365, help='Number of recent days to fetch and analyze')
        args = parser.parse_args()
        days = args['days']

        # Call the service function to analyze and save technical data
        success, msg = data_fetch_and_process.analyze_technical_data_for_symbol(symbol_id, symbol_id, limit_days=days)
        if not success:
            analysis_ns.abort(404, f"Failed to analyze technical data for symbol_id: {symbol_id}. Reason: {msg}")

        # Fetch the newly saved technical data from the database
        technical_data_records = TechnicalIndicatorData.query.filter_by(symbol_id=symbol_id)\
                                                        .order_by(TechnicalIndicatorData.jdate.desc())\
                                                        .limit(days).all()
        if not technical_data_records:
            analysis_ns.abort(404, f"No technical indicator data found for symbol_id: {symbol_id} after calculation.")

        # Convert records to a list of dictionaries for marshalling
        return [rec.__dict__ for rec in technical_data_records]


# --- NEW API Resource for ML Predictions ---
@analysis_ns.route('/ml-predictions')
class MLPredictionListResource(Resource):
    @analysis_ns.doc(security='Bearer Auth', params={'symbol_id': 'Optional: Filter predictions by symbol ID'})
    @jwt_required()
    @analysis_ns.marshal_list_with(ml_prediction_model)
    @analysis_ns.response(200, 'ML predictions retrieved successfully.')
    @analysis_ns.response(404, 'No ML prediction found for the symbol (if symbol_id provided).')
    @analysis_ns.response(500, 'Error retrieving ML predictions.')
    def get(self):
        """
        Retrieves ML predictions. Can be filtered by symbol_id.
        If no symbol_id is provided, returns all predictions.
        """
        symbol_id = request.args.get('symbol_id')
        if symbol_id:
            current_app.logger.info(f"API request for ML prediction for symbol: {symbol_id}")
            prediction = get_ml_predictions_for_symbol(symbol_id)
            if prediction:
                # get_ml_predictions_for_symbol returns a single dict, marshal_list_with expects a list
                return [prediction], 200 
            else:
                return {'message': f'No ML prediction found for symbol_id: {symbol_id}'}, 404
        else:
            current_app.logger.info("API request for all ML predictions.")
            predictions = get_all_ml_predictions()
            return predictions, 200

