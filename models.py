# -*- coding: utf-8 -*-
# models.py
from extensions import db
from datetime import datetime, date # Import date as well
import uuid # برای تولید Unique ID برای سیگنال‌ها
import json # For storing satisfied_filters as JSON string
from sqlalchemy import UniqueConstraint # Import UniqueConstraint if still needed for other models

# --- این خط را حذف کنید. این باعث خطای ModuleNotFoundError می‌شود. ---
# import models # این خط تضمین می‌کند که تمام کلاس‌های مدل در models.py بارگذاری شوند
# --- پایان حذف شده ---

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    hashed_password = db.Column(db.String(128), nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'

class HistoricalData(db.Model):
    __tablename__ = 'stock_data'
    symbol_id = db.Column(db.String(50), primary_key=True) # Changed to composite primary key
    symbol_name = db.Column(db.String(255), nullable=False) # Added length for string
    date = db.Column(db.Date, primary_key=True) # Gregorian date as Date object, part of composite primary key
    jdate = db.Column(db.String(10), nullable=False) # Jdate (Persian date) as YYYY-MM-DD string

    # Prices
    open = db.Column(db.Float) # pf
    high = db.Column(db.Float) # pmax
    low = db.Column(db.Float)   # pmin
    close = db.Column(db.Float) # pc (final closing price)
    final = db.Column(db.Float) # pl (last traded price)
    yesterday_price = db.Column(db.Float) # py

    # Volume and Value
    volume = db.Column(db.BigInteger) # tvol (Changed to BigInteger for larger values)
    value = db.Column(db.BigInteger)    # tval (Changed to BigInteger for larger values)
    num_trades = db.Column(db.Integer) # tno

    # Price Changes (calculated or from API)
    plc = db.Column(db.Float) # change in last price
    plp = db.Column(db.Float) # percentage change in last price
    pcc = db.Column(db.Float) # change in final price
    pcp = db.Column(db.Float) # percentage change in final price

    # Market Value (often included in daily history for convenience, but EPS/PE are in FundamentalData)
    mv = db.Column(db.BigInteger)       # Market Value (Changed to BigInteger)

    # Real/Legal Shareholder Data (Individual/Institutional)
    buy_count_i = db.Column(db.Integer) # Buy_CountI
    buy_count_n = db.Column(db.Integer) # Buy_CountN
    sell_count_i = db.Column(db.Integer) # Sell_CountI
    sell_count_n = db.Column(db.Integer) # Sell_CountN
    buy_i_volume = db.Column(db.BigInteger) # Buy_I_Volume (Changed to BigInteger)
    buy_n_volume = db.Column(db.BigInteger) # Buy_N_Volume (Changed to BigInteger)
    sell_i_volume = db.Column(db.BigInteger) # Sell_I_Volume (Changed to BigInteger)
    sell_n_volume = db.Column(db.BigInteger) # Sell_N_Volume (Changed to BigInteger)

    # Order Book Data (Up to 5 levels) - zdX: demand count, qdX: demand volume, pdX: demand price, zoX: offer count, qoX: offer volume, poX: offer price
    zd1 = db.Column(db.Integer)
    qd1 = db.Column(db.BigInteger) # Changed to BigInteger
    pd1 = db.Column(db.Float)
    zo1 = db.Column(db.Integer)
    qo1 = db.Column(db.BigInteger) # Changed to BigInteger
    po1 = db.Column(db.Float)

    zd2 = db.Column(db.Integer)
    qd2 = db.Column(db.BigInteger) # Changed to BigInteger
    pd2 = db.Column(db.Float)
    zo2 = db.Column(db.Integer)
    qo2 = db.Column(db.BigInteger) # Changed to BigInteger
    po2 = db.Column(db.Float)

    zd3 = db.Column(db.Integer)
    qd3 = db.Column(db.BigInteger) # Changed to BigInteger
    pd3 = db.Column(db.Float)
    zo3 = db.Column(db.Integer)
    qo3 = db.Column(db.BigInteger) # Changed to BigInteger
    po3 = db.Column(db.Float)

    zd4 = db.Column(db.Integer)
    qd4 = db.Column(db.BigInteger) # Changed to BigInteger
    pd4 = db.Column(db.Float)
    zo4 = db.Column(db.Integer)
    qo4 = db.Column(db.BigInteger) # Changed to BigInteger
    po4 = db.Column(db.Float)

    zd5 = db.Column(db.Integer)
    qd5 = db.Column(db.BigInteger) # Changed to BigInteger
    pd5 = db.Column(db.Float)
    zo5 = db.Column(db.Integer)
    qo5 = db.Column(db.BigInteger) # Changed to BigInteger
    po5 = db.Column(db.Float)
    
    # Timestamps for tracking
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'<HistoricalData {self.symbol_name} - {self.date}>'

class ComprehensiveSymbolData(db.Model):
    __tablename__ = 'comprehensive_symbol_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), unique=True) # l18 - Added length
    symbol_name = db.Column(db.String(100)) # name (from AllSymbols) - Added length
    company_name = db.Column(db.String(255)) # l30 (from AllSymbols or ComprehensiveSymbol) - Added length
    isin = db.Column(db.String(12)) # isin (from AllSymbols) - Added length

    # Fields from ComprehensiveSymbol.php (if fetched later)
    market_type = db.Column(db.String(50)) # Added length
    flow = db.Column(db.String(50)) # Added length
    industry = db.Column(db.String(100)) # Added length
    capital = db.Column(db.Float) # CHANGED: From db.String to db.Float
    legal_shareholder_percentage = db.Column(db.Float) # l_ps_p
    real_shareholder_percentage = db.Column(db.Float)       # r_ps_p
    float_shares = db.Column(db.Float) # float_shares
    base_volume = db.Column(db.Float)       # base_volume
    group_name = db.Column(db.String(100)) # cs - Added length
    description = db.Column(db.Text) # Changed to Text for potentially long descriptions

    last_historical_update_date = db.Column(db.Date) # To track when historical data was last updated
    
    # Added created_at and updated_at for better tracking
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'<ComprehensiveSymbolData {self.symbol_name} ({self.symbol_id})>'

class SignalsPerformance(db.Model):
    """
    Tracks the performance of individual signals generated by different sources.
    """
    __tablename__ = 'signals_performance'
    id = db.Column(db.Integer, primary_key=True)
    
    # MODIFIED: Changed from signal_unique_id to signal_id for consistency
    signal_id = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()), comment='Unique identifier for each signal') 
    
    # ADDED: symbol_id and symbol_name to link to ComprehensiveSymbolData
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False, comment='Stock symbol ID (Persian short name)')
    symbol_name = db.Column(db.String(255), nullable=False, comment='Stock symbol name')
    
    signal_source = db.Column(db.String(50), nullable=False, comment='Source of the signal (e.g., Weekly Watchlist, Golden Key, Potential Buy Queue)') 
    
    entry_date = db.Column(db.Date, nullable=False, comment='Gregorian entry date') 
    jentry_date = db.Column(db.String(10), nullable=False, comment='Jalali entry date (YYYY-MM-DD)') 
    entry_price = db.Column(db.Float, nullable=False, comment='Price at signal entry')
    
    outlook = db.Column(db.String(50), nullable=True, comment='Outlook of the signal (e.g., Bullish, Neutral)') 
    reason = db.Column(db.Text, nullable=True, comment='Explanation for the signal') # Changed to Text for longer explanations
    probability_percent = db.Column(db.Float, nullable=True, comment='Estimated probability of success')

    exit_date = db.Column(db.Date, nullable=True, comment='Gregorian exit date') 
    jexit_date = db.Column(db.String(10), nullable=True, comment='Jalali exit date (YYYY-MM-DD)') 
    exit_price = db.Column(db.Float, nullable=True, comment='Price at signal exit')
    profit_loss_percent = db.Column(db.Float, nullable=True, comment='Calculated profit/loss percentage')
    status = db.Column(db.String(50), default='active', nullable=True, comment='Status of the signal (active, closed_profit, closed_loss, closed_neutral)') 
    
    created_at = db.Column(db.DateTime, default=datetime.now, comment='When the signal was first recorded') 
    evaluated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='When performance was last evaluated')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='زمان آخرین به‌روزرسانی رکورد') 

    # Relationship to ComprehensiveSymbolData (for symbol_id foreign key)
    symbol_data = db.relationship('ComprehensiveSymbolData', backref=db.backref('signals_performance_records', lazy=True))

    __table_args__ = (db.UniqueConstraint('signal_id', name='_signal_id_uc'),) # Ensure signal_id is unique

    def __repr__(self):
        return f'<SignalsPerformance {self.symbol_name} - {self.signal_source} - {self.jentry_date}>'

class AggregatedPerformance(db.Model):
    __tablename__ = 'aggregated_performance'
    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.String(10), nullable=False, comment='تاریخ گزارش به شمسی (YYYY-MM-DD)')
    period_type = db.Column(db.String(20), nullable=False, comment='نوع دوره (daily, weekly, monthly, annual)')
    signal_source = db.Column(db.String(50), nullable=False, comment='منبع سیگنال (Golden Key, Weekly Watchlist, overall)')
    total_signals = db.Column(db.Integer, nullable=False, default=0, comment='تعداد کل سیگنال‌ها در دوره')
    successful_signals = db.Column(db.Integer, nullable=False, default=0, comment='تعداد سیگنال‌های موفق در دوره')
    win_rate = db.Column(db.Float, nullable=False, default=0.0, comment='درصد برد سیگنال‌ها')
    total_profit_percent = db.Column(db.Float, nullable=False, default=0.0, comment='درصد سود کل سیگنال‌های موفق')
    total_loss_percent = db.Column(db.Float, nullable=False, default=0.0, comment='درصد زیان کل سیگنال‌های ناموفق')
    
    # NEWLY ADDED COLUMNS (Ensure these are present)
    average_profit_per_win = db.Column(db.Float, nullable=False, default=0.0, comment='میانگین سود هر سیگنال موفق')
    average_loss_per_loss = db.Column(db.Float, nullable=False, default=0.0, comment='میانگین زیان هر سیگنال ناموفق')
    net_profit_percent = db.Column(db.Float, nullable=False, default=0.0, comment='درصد سود/زیان خالص (سود کل + زیان کل)')

    created_at = db.Column(db.DateTime, default=datetime.now, comment='زمان ایجاد رکورد')
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now, comment='زمان آخرین به‌روزرسانی رکورد')

    __table_args__ = (db.UniqueConstraint('report_date', 'period_type', 'signal_source', name='_report_period_source_uc'),)

    def __repr__(self):
        return f"<AggregatedPerformance {self.signal_source} {self.period_type} {self.report_date}>"


# --- Placeholder Models for Fundamental and Sentiment Data ---
class FundamentalData(db.Model):
    __tablename__ = 'fundamental_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), unique=True, nullable=False) # Added length, Foreign Key
    # Add fundamental data fields here.
    eps = db.Column(db.Float)
    pe = db.Column(db.Float) # Changed from pe_ratio to pe for consistency with data_fetch_and_process.py
    group_pe_ratio = db.Column(db.Float)
    psr = db.Column(db.Float)
    p_s_ratio = db.Column(db.Float)
    market_cap = db.Column(db.BigInteger) # Changed to BigInteger
    base_volume = db.Column(db.BigInteger) # Changed to BigInteger
    float_shares = db.Column(db.Float) # Float is fine here
    created_at = db.Column(db.DateTime, default=datetime.now) # Added
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now) # Added onupdate

    def __repr__(self):
        return f'<FundamentalData {self.symbol_id}>'

class SentimentData(db.Model):
    __tablename__ = 'sentiment_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), nullable=False) # Added length
    date = db.Column(db.String(10), nullable=False) # YYYY-MM-DD - Added length
    # Add sentiment data fields here.
    sentiment_score = db.Column(db.Float) # Example field
    news_count = db.Column(db.Integer) # Example field

    __table_args__ = (db.UniqueConstraint('symbol_id', 'date', name='_symbol_date_sentiment_uc'),)

    def __repr__(self):
        return f'<SentimentData {self.symbol_id} - {self.date}>'

class TechnicalIndicatorData(db.Model):
    __tablename__ = 'technical_indicator_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False) # Added Foreign Key
    jdate = db.Column(db.String(10), nullable=False) # Added length
    close_price = db.Column(db.Float) # To store the closing price for the indicator date
    RSI = db.Column(db.Float)
    MACD = db.Column(db.Float)
    MACD_Signal = db.Column(db.Float)
    MACD_Hist = db.Column(db.Float)
    SMA_20 = db.Column(db.Float)
    SMA_50 = db.Column(db.Float)
    Bollinger_High = db.Column(db.Float)
    Bollinger_Low = db.Column(db.Float)
    Bollinger_MA = db.Column(db.Float)
    Volume_MA_20 = db.Column(db.Float)
    ATR = db.Column(db.Float) # NEWLY ADDED: Average True Range
    # Add other technical indicators as needed
    created_at = db.Column(db.DateTime, default=datetime.now) # Added
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now) # Added

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', name='_symbol_jdate_tech_uc'),)

    def __repr__(self):
        return f'<TechnicalIndicatorData {self.symbol_id} - {self.jdate}>'

class CandlestickPatternDetection(db.Model):
    __tablename__ = 'candlestick_pattern_detection'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False) # Added Foreign Key
    jdate = db.Column(db.String(10), nullable=False) # Added length
    pattern_name = db.Column(db.String(100), nullable=False) # Added length
    # Add other pattern details
    created_at = db.Column(db.DateTime, default=datetime.now) # Added
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now) # Added

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', 'pattern_name', name='_symbol_jdate_pattern_uc'),)

    def __repr__(self):
        return f'<CandlestickPatternDetection {self.symbol_id} - {self.jdate} - {self.pattern_name}>'

class TSETMCFilterResult(db.Model):
    __tablename__ = 'tsetmc_filter_result'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False) # Added Foreign Key
    jdate = db.Column(db.String(10), nullable=False) # Added length
    filter_name = db.Column(db.String(100), nullable=False) # Added length
    # Add other filter details
    created_at = db.Column(db.DateTime, default=datetime.now) # Added
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now) # Added

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', 'filter_name', name='_symbol_jdate_filter_uc'),)

    def __repr__(self):
        return f'<TSETMCFilterResult {self.symbol_id} - {self.jdate} - {self.filter_name}>'

class FinancialRatiosData(db.Model):
    __tablename__ = 'financial_ratios_data'
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False) # Added Foreign Key
    fiscal_year = db.Column(db.String(10), nullable=False) # Added length
    ratio_name = db.Column(db.String(100), nullable=False) # Added length
    ratio_value = db.Column(db.Float)
    # Add other financial ratio details
    created_at = db.Column(db.DateTime, default=datetime.now) # Added
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now) # Added

    __table_args__ = (db.UniqueConstraint('symbol_id', 'fiscal_year', 'ratio_name', name='_symbol_fiscal_ratio_uc'),)

    def __repr__(self):
        return f'<FinancialRatiosData {self.symbol_id} - {self.fiscal_year} - {self.ratio_name}>'

    # Add this new model for ML Predictions
class MLPrediction(db.Model):
    __tablename__ = 'ml_predictions'

    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False)
    symbol_name = db.Column(db.String(255), nullable=False)
    
    prediction_date = db.Column(db.Date, nullable=False, comment="Gregorian date when the prediction was made")
    jprediction_date = db.Column(db.String(10), nullable=False, comment="Jalali date when the prediction was made (YYYY-MM-DD)")
    
    prediction_period_days = db.Column(db.Integer, default=7, comment="Number of days for the prediction horizon (e.g., 7 days)")
    
    predicted_trend = db.Column(db.String(50), nullable=False, comment="Predicted trend: 'UP', 'DOWN', 'NEUTRAL'")
    prediction_probability = db.Column(db.Float, nullable=False, comment="Probability/confidence of the predicted trend (0.0 to 1.0)")
    
    # Optional: Predicted price at the end of the period (for reference)
    predicted_price_at_period_end = db.Column(db.Float, nullable=True)
    
    # Fields to store actual outcome (updated after the prediction period)
    actual_price_at_period_end = db.Column(db.Float, nullable=True)
    actual_trend_outcome = db.Column(db.String(50), nullable=True, comment="Actual trend outcome: 'UP', 'DOWN', 'NEUTRAL'")
    is_prediction_accurate = db.Column(db.Boolean, nullable=True, comment="True if predicted_trend matches actual_trend_outcome")
    
    signal_source = db.Column(db.String(50), default='ML-Trend', comment="Source of the signal, e.g., 'ML-Trend'")
    model_version = db.Column(db.String(50), nullable=True, comment="Version of the ML model used for prediction")
    
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<MLPrediction {self.symbol_id} - {self.jprediction_date} - {self.predicted_trend}>"

    def to_dict(self):
        """Converts the MLPrediction object to a dictionary for API response."""
        return {
            'id': self.id,
            'symbol_id': self.symbol_id,
            'symbol_name': self.symbol_name,
            'prediction_date': self.prediction_date.strftime('%Y-%m-%d') if self.prediction_date else None,
            'jprediction_date': self.jprediction_date,
            'prediction_period_days': self.prediction_period_days,
            'predicted_trend': self.predicted_trend,
            'prediction_probability': self.prediction_probability,
            'predicted_price_at_period_end': self.predicted_price_at_period_end,
            'actual_price_at_period_end': self.actual_price_at_period_end,
            'actual_trend_outcome': self.actual_trend_outcome,
            'is_prediction_accurate': self.is_prediction_accurate,
            'signal_source': self.signal_source,
            'model_version': self.model_version,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


class GoldenKeyResult(db.Model):
    """
    Stores the weekly Golden Key selected symbols based on combined technical filters.
    """
    __tablename__ = 'golden_key_results' # Keep the existing table name for clarity
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), db.ForeignKey('comprehensive_symbol_data.symbol_id'), nullable=False) # Added length
    symbol_name = db.Column(db.String(100), nullable=False) # Added length
    jdate = db.Column(db.String(10), nullable=False) # Jalali date of the recommendation (Wednesday 18:00) - Added length
    is_golden_key = db.Column(db.Boolean, default=False)
    score = db.Column(db.Integer, default=0) # Score based on how many filters passed
    # matched_filters = db.Column(db.Integer, default=0) # Removed as satisfied_filters stores the actual list
    reason = db.Column(db.Text) # Human-readable reasons (e.g., "RSI oversold, MACD cross")
    timestamp = db.Column(db.DateTime, default=datetime.now) # Timestamp of when the result was generated

    # NEW: Store which specific filters were satisfied (for frontend filtering)
    satisfied_filters = db.Column(db.Text) # Storing JSON string of filter names

    # NEW: Fields for Win-Rate calculation
    recommendation_price = db.Column(db.Float) # Price at Saturday 5:00 AM (or closest available)
    recommendation_jdate = db.Column(db.String(10)) # Jalali date of recommendation price - Added length
    final_price = db.Column(db.Float) # Final price at Wednesday 8:00 PM (or closest available)
    profit_loss_percentage = db.Column(db.Float) # (final_price - recommendation_price) / recommendation_price * 100
    
    # ADDED: weekly_growth field (This was the missing field causing the TypeError)
    # NOTE: In services/golden_key_service.py, profit_loss_percentage is mapped to weekly_growth in JSON output.
    # It's recommended to use one consistent field name for clarity.
    # If weekly_growth is meant to be distinct from profit_loss_percentage, keep it.
    # Otherwise, consider removing weekly_growth here and just using profit_loss_percentage.
    weekly_growth = db.Column(db.Float, nullable=True, comment='Weekly growth percentage of the symbol') 

    # --- ADDED THESE TWO CRUCIAL FIELDS ---
    status = db.Column(db.String(50), default='active', nullable=True, comment='Status of the signal (active, closed_profit, closed_loss, closed_neutral)')
    probability_percent = db.Column(db.Float, nullable=True, comment='Estimated probability of success for this signal')
    # --- END ADDED FIELDS ---

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', name='_symbol_jdate_golden_key_uc'),)

    def __repr__(self):
        return f'<GoldenKeyResult {self.symbol_name} {self.jdate} (Score: {self.score})>'

class WeeklyWatchlistResult(db.Model):
    __tablename__ = 'weekly_watchlist_results' # Added tablename for clarity
    id = db.Column(db.Integer, primary_key=True)
    signal_unique_id = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    symbol = db.Column(db.String(50), nullable=False) # This is the symbol_id in your service logic
    symbol_name = db.Column(db.String(100), nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    entry_date = db.Column(db.Date, nullable=False) # Gregorian date
    jentry_date = db.Column(db.String(10), nullable=False) # Jalali date 'YYYY-MM-DD'
    outlook = db.Column(db.String(255)) 
    reason = db.Column(db.Text) 
    probability_percent = db.Column(db.Float) 
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now) 

    # --- ADDED: Fields for WeeklyWatchlistResult performance tracking ---
    status = db.Column(db.String(50), default='active', nullable=False) # 'active', 'closed_win', 'closed_loss', 'closed_neutral'
    exit_price = db.Column(db.Float, nullable=True)
    exit_date = db.Column(db.Date, nullable=True) # Gregorian exit date
    jexit_date = db.Column(db.String(10), nullable=True) # Jalali exit date
    profit_loss_percentage = db.Column(db.Float, nullable=True)

    def __repr__(self):
        return f"<WeeklyWatchlistResult {self.symbol} on {self.jentry_date}>"

class PotentialBuyQueueResult(db.Model):
    __tablename__ = 'potential_buy_queue_results' # Optional: specify table name
    id = db.Column(db.Integer, primary_key=True)
    symbol_id = db.Column(db.String(50), nullable=False)
    symbol_name = db.Column(db.String(255), nullable=False)
    reason = db.Column(db.Text, nullable=True)
    jdate = db.Column(db.String(10), nullable=False) # 'YYYY-MM-DD'
    current_price = db.Column(db.Float, nullable=True)
    volume_change_percent = db.Column(db.Float, nullable=True)
    real_buyer_power_ratio = db.Column(db.Float, nullable=True)
    matched_filters = db.Column(db.Text, nullable=True) # Stored as JSON string
    group_type = db.Column(db.String(50), nullable=True) # 'general' or 'fund'
    timestamp = db.Column(db.DateTime, default=datetime.now)
    
    # NEWLY ADDED: Add the probability_percent column
    probability_percent = db.Column(db.Float, nullable=True)

    __table_args__ = (db.UniqueConstraint('symbol_id', 'jdate', name='_symbol_jdate_potential_queue_uc'),)

    def __repr__(self):
        return f'<PotentialBuyQueueResult {self.symbol_name} {self.jdate}>'
