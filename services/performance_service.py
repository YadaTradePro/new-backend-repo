# services/performance_service.py
from extensions import db
from models import SignalsPerformance, AggregatedPerformance, GoldenKeyResult, WeeklyWatchlistResult # Import all necessary models
from datetime import datetime, timedelta, date # Import date as well for type checking
import jdatetime
import pandas as pd
import logging
from sqlalchemy import func, cast, Date, and_

# Import utility functions
from services.utils import get_today_jdate_str, convert_gregorian_to_jalali # ADDED: Import these utility functions

logger = logging.getLogger(__name__)

# --- Helper functions for safe date/datetime formatting ---
def safe_date_format(date_obj, fmt='%Y-%m-%d'):
    """
    Safely formats a date or datetime object to a string.
    Returns None if the object is not a valid date/datetime.
    """
    if isinstance(date_obj, (datetime, date)):
        return date_obj.strftime(fmt)
    return None

def safe_isoformat(datetime_obj):
    """
    Safely converts a datetime object to an ISO 8601 string.
    Returns None if the object is not a valid datetime.
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.isoformat()
    return None
# --- End of Helper functions ---


def calculate_and_save_aggregated_performance(period_type='weekly', signal_source='overall'):
    """
    Calculates and saves aggregated performance reports (e.g., weekly, annual).
    
    Args:
        period_type (str): 'weekly', 'monthly', or 'annual'
        signal_source (str): 'Golden Key', 'Weekly Watchlist', or 'overall'
    
    Returns:
        tuple: (success_status, message)
    """
    logger.info(f"Calculating aggregated performance for {signal_source} ({period_type}).")

    today_jdate_str = get_today_jdate_str()
    
    start_jdate_str = None
    # Define the start date based on period_type
    if period_type == 'weekly':
        try:
            today_greg = jdatetime.date.today().togregorian()
            start_date_greg = today_greg - timedelta(days=7)
            start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
        except Exception as e:
            logger.error(f"Error converting Gregorian to Jalali for weekly period: {e}", exc_info=True)
            return False, "Error determining start date for weekly period."
        
        if not start_jdate_str:
            logger.error("Failed to convert Gregorian start date to Jalali for weekly period (returned None/empty).")
            return False, "Failed to determine start date for weekly period."

    elif period_type == 'monthly': # NEW: Monthly period
        try:
            today_greg = jdatetime.date.today().togregorian()
            start_date_greg = today_greg - timedelta(days=30) # Approx a month
            start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
        except Exception as e:
            logger.error(f"Error converting Gregorian to Jalali for monthly period: {e}", exc_info=True)
            return False, "Error determining start date for monthly period."
        
        if not start_jdate_str:
            logger.error("Failed to convert Gregorian start date to Jalali for monthly period (returned None/empty).")
            return False, "Failed to determine start date for monthly period."

    elif period_type == 'annual':
        try:
            today_greg = jdatetime.date.today().togregorian()
            start_date_greg = today_greg - timedelta(days=365)
            start_jdate_str = convert_gregorian_to_jalali(start_date_greg)
        except Exception as e:
            logger.error(f"Error converting Gregorian to Jalali for annual period: {e}", exc_info=True)
            return False, "Error determining start date for annual period."
        
        if not start_jdate_str:
            logger.error("Failed to convert Gregorian start date to Jalali for annual period (returned None/empty).")
            return False, "Failed to determine start date for annual period."
    else:
        return False, "Invalid period_type. Must be 'weekly', 'monthly', or 'annual'."

    # Query signals based on source and date range
    query_conditions = [
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral'])
    ]
    
    if start_jdate_str: # Only apply date filter if start_jdate_str is valid
        query_conditions.append(SignalsPerformance.jentry_date >= start_jdate_str)

    if signal_source != 'overall':
        query_conditions.append(SignalsPerformance.signal_source == signal_source)

    query = SignalsPerformance.query.filter(and_(*query_conditions))
    signals_in_period = query.all()

    if not signals_in_period:
        message = f"No closed signals found for {signal_source} ({period_type}) in the period starting {start_jdate_str}."
        logger.warning(message)
        # If no signals, we still want to save a report with zeros, or update existing to zeros
        total_signals = 0
        successful_signals = 0
        win_rate = 0.0
        total_profit_percent = 0.0
        total_loss_percent = 0.0
        average_profit_per_win = 0.0
        average_loss_per_loss = 0.0
        net_profit_percent = 0.0
    else:
        total_signals = len(signals_in_period)
        successful_signals = sum(1 for s in signals_in_period if s.status == 'closed_win')
        
        win_rate = (successful_signals / total_signals) * 100 if total_signals > 0 else 0.0

        # Ensure profit_loss_percent is not None before summing
        total_profit_percent = sum(s.profit_loss_percent for s in signals_in_period if s.status == 'closed_win' and s.profit_loss_percent is not None)
        total_loss_percent = sum(s.profit_loss_percent for s in signals_in_period if s.status == 'closed_loss' and s.profit_loss_percent is not None)

        winning_signals = [s for s in signals_in_period if s.status == 'closed_win' and s.profit_loss_percent is not None]
        losing_signals = [s for s in signals_in_period if s.status == 'closed_loss' and s.profit_loss_percent is not None]

        average_profit_per_win = (sum(s.profit_loss_percent for s in winning_signals) / len(winning_signals)) if winning_signals else 0.0
        average_loss_per_loss = (sum(s.profit_loss_percent for s in losing_signals) / len(losing_signals)) if losing_signals else 0.0
        net_profit_percent = total_profit_percent + total_loss_percent # Correct calculation

    # Check if an aggregated performance record for today and this source/period already exists
    existing_agg_perf = AggregatedPerformance.query.filter_by(
        report_date=get_today_jdate_str(), # Use get_today_jdate_str for report_date
        period_type=period_type,
        signal_source=signal_source
    ).first()

    if existing_agg_perf:
        existing_agg_perf.total_signals = total_signals
        existing_agg_perf.successful_signals = successful_signals
        existing_agg_perf.win_rate = win_rate
        existing_agg_perf.total_profit_percent = total_profit_percent
        existing_agg_perf.total_loss_percent = total_loss_percent
        existing_agg_perf.average_profit_per_win = average_profit_per_win
        existing_agg_perf.average_loss_per_loss = average_loss_per_loss
        existing_agg_perf.net_profit_percent = net_profit_percent
        existing_agg_perf.updated_at = datetime.now()
        db.session.add(existing_agg_perf)
        logger.info(f"Updated aggregated performance for {signal_source} ({period_type}) on {get_today_jdate_str()}.")
    else:
        new_agg_perf = AggregatedPerformance(
            report_date=get_today_jdate_str(), # Use get_today_jdate_str for report_date
            period_type=period_type,
            signal_source=signal_source,
            total_signals=total_signals,
            successful_signals=successful_signals,
            win_rate=win_rate,
            total_profit_percent=total_profit_percent,
            total_loss_percent=total_loss_percent,
            average_profit_per_win = average_profit_per_win,
            average_loss_per_loss = average_loss_per_loss,
            net_profit_percent = net_profit_percent,
            created_at=datetime.now()
        )
        db.session.add(new_agg_perf)
        logger.info(f"Created new aggregated performance record for {signal_source} ({period_type}) on {get_today_jdate_str()}.")
    
    try:
        db.session.commit()
        message = f"Aggregated performance for {signal_source} ({period_type}) calculated successfully. Win Rate: {win_rate:.2f}%."
        logger.info(message)
        return True, message
    except Exception as e:
        db.session.rollback()
        error_message = f"Error during aggregated performance calculation: {e}"
        logger.error(error_message, exc_info=True)
        return False, error_message

def get_aggregated_performance_reports(period_type=None, signal_source=None):
    """
    Retrieves aggregated performance reports from the database.
    
    Args:
        period_type (str, optional): Filter by 'weekly' or 'annual'. Defaults to None (all).
        signal_source (str, optional): Filter by 'Golden Key', 'Weekly Watchlist', or 'overall'. Defaults to None (all).
        
    Returns:
        list: A list of dictionaries, each representing an aggregated performance report.
    """
    logger.info(f"Retrieving aggregated performance reports (Period: {period_type}, Source: {signal_source}).")
    
    query = AggregatedPerformance.query
    
    if period_type:
        query = query.filter_by(period_type=period_type)
    if signal_source:
        query = query.filter_by(signal_source=signal_source)
        
    reports = query.order_by(AggregatedPerformance.report_date.desc(), AggregatedPerformance.created_at.desc()).all()
    
    output = []
    for r in reports:
        output.append({
            'report_id': r.id,
            'report_date': r.report_date,
            'period_type': r.period_type,
            'signal_source': r.signal_source,
            'total_signals': r.total_signals,
            'successful_signals': r.successful_signals,
            'win_rate': r.win_rate,
            'total_profit_percent': r.total_profit_percent,
            'total_loss_percent': r.total_loss_percent,
            'net_profit_percent': r.net_profit_percent,
            'average_profit_per_win': r.average_profit_per_win,
            'average_loss_per_loss': r.average_loss_per_loss,
            'created_at': safe_date_format(r.created_at, '%Y-%m-%d %H:%M:%S'), # Use safe_date_format
            'updated_at': safe_date_format(r.updated_at, '%Y-%m-%d %H:%M:%S') # Use safe_date_format
        })
    
    logger.info(f"Retrieved {len(output)} aggregated performance reports.")
    return output


def get_overall_performance_summary():
    """
    Calculates a comprehensive summary of the application's overall performance,
    including overall, weekly, monthly, and annual metrics.
    
    Returns:
        dict: A dictionary containing various performance metrics.
    """
    logger.info("Calculating application performance summary.")
    
    # 1. Overall Performance (across all time, all sources)
    all_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral'])
    ).all()
    
    overall_summary_data = {
        "total_signals_evaluated": 0,
        "overall_win_rate": 0.0,
        "average_profit_per_win_overall": 0.0,
        "average_loss_per_loss_overall": 0.0,
        "overall_net_profit_percent": 0.0
    }
    signals_by_source_data = {}

    if all_signals:
        df = pd.DataFrame([s.__dict__ for s in all_signals]).drop(columns=['_sa_instance_state'], errors='ignore')
        df['profit_loss_percent'] = pd.to_numeric(df['profit_loss_percent'], errors='coerce').fillna(0)

        overall_summary_data["total_signals_evaluated"] = len(df)
        total_wins = df[df['status'] == 'closed_win'].shape[0]
        
        overall_summary_data["overall_win_rate"] = (total_wins / overall_summary_data["total_signals_evaluated"]) * 100 if overall_summary_data["total_signals_evaluated"] > 0 else 0.0
        overall_summary_data["overall_net_profit_percent"] = df['profit_loss_percent'].sum()

        winning_signals_pl = df[df['status'] == 'closed_win']['profit_loss_percent']
        losing_signals_pl = df[df['status'] == 'closed_loss']['profit_loss_percent']

        overall_summary_data["average_profit_per_win_overall"] = winning_signals_pl.mean() if not winning_signals_pl.empty else 0.0
        overall_summary_data["average_loss_per_loss_overall"] = losing_signals_pl.mean() if not losing_signals_pl.empty else 0.0

        for source in df['signal_source'].unique():
            source_df = df[df['signal_source'] == source]
            source_total = len(source_df)
            source_wins = source_df[source_df['status'] == 'closed_win'].shape[0]
            source_losses = source_df[source_df['status'] == 'closed_loss'].shape[0]
            source_neutral = source_df[source_df['status'] == 'closed_neutral'].shape[0]
            source_win_rate = (source_wins / source_total) * 100 if source_total > 0 else 0.0
            source_net_profit = source_df['profit_loss_percent'].sum()
            
            signals_by_source_data[source] = {
                "total_signals": source_total,
                "wins": source_wins,
                "losses": source_losses,
                "neutral": source_neutral,
                "win_rate": source_win_rate,
                "net_profit_percent": source_net_profit
            }

    # 2. Fetch latest weekly and monthly aggregated overall reports
    today_jdate_str = get_today_jdate_str()

    latest_weekly_overall_report = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type='weekly',
        signal_source='overall'
    ).first()

    latest_monthly_overall_report = AggregatedPerformance.query.filter_by(
        report_date=today_jdate_str,
        period_type='monthly',
        signal_source='overall'
    ).first()

    # 3. Annual Profit/Loss from start of Jalali year
    annual_profit_loss = get_annual_profit_loss_summary()

    summary = {
        "overall_performance": overall_summary_data,
        "signals_by_source": signals_by_source_data,
        "weekly_overall_performance": {
            "win_rate": latest_weekly_overall_report.win_rate if latest_weekly_overall_report else 0.0,
            "net_profit_percent": latest_weekly_overall_report.net_profit_percent if latest_weekly_overall_report else 0.0
        },
        "monthly_overall_performance": {
            "win_rate": latest_monthly_overall_report.win_rate if latest_monthly_overall_report else 0.0,
            "net_profit_percent": latest_monthly_overall_report.net_profit_percent if latest_monthly_overall_report else 0.0
        },
        "annual_profit_loss": annual_profit_loss, # This will be the single value
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    logger.info("Application performance summary calculated successfully.")
    return summary

def get_annual_profit_loss_summary():
    """
    Calculates the total net profit/loss percentage from the beginning of the current Jalali year.
    """
    logger.info("Calculating annual profit/loss summary.")
    
    current_jalali_year = jdatetime.date.today().year
    start_of_jalali_year_str = f"{current_jalali_year}-01-01"

    # Query all closed signals from the beginning of the current Jalali year
    annual_signals = SignalsPerformance.query.filter(
        SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral']),
        SignalsPerformance.jentry_date >= start_of_jalali_year_str
    ).all()

    total_annual_profit_loss_percent = 0.0
    if annual_signals:
        df_annual = pd.DataFrame([s.__dict__ for s in annual_signals]).drop(columns=['_sa_instance_state'], errors='ignore')
        df_annual['profit_loss_percent'] = pd.to_numeric(df_annual['profit_loss_percent'], errors='coerce').fillna(0)
        total_annual_profit_loss_percent = df_annual['profit_loss_percent'].sum()
    
    logger.info(f"Annual profit/loss from {start_of_jalali_year_str} to {get_today_jdate_str()}: {total_annual_profit_loss_percent:.2f}%")
    return total_annual_profit_loss_percent


def get_detailed_signals_performance(status_filter=None, period_filter=None):
    """
    Retrieves detailed past signal performance records from the database.
    Can filter by status (e.g., 'active') and period (e.g., 'previous_week').
    
    Args:
        status_filter (str, optional): Filter by signal status (e.g., 'active', 'closed_win').
        period_filter (str, optional): Filter by period (e.g., 'previous_week').
    
    Returns:
        list: A list of dictionaries, each representing a SignalsPerformance record.
    """
    logger.info(f"Retrieving detailed SignalsPerformance records (Status: {status_filter}, Period: {period_filter}).")
    query = SignalsPerformance.query

    # Apply status filter
    if status_filter:
        query = query.filter(SignalsPerformance.status == status_filter)

    # Apply period filter
    if period_filter == 'previous_week':
        today_greg = jdatetime.date.today().togregorian()
        # Signals from the last 7 to 14 days ago (to represent "previous week")
        end_date_greg = today_greg - timedelta(days=7) # End of previous week
        start_date_greg = today_greg - timedelta(days=14) # Start of previous week

        end_jdate_str = convert_gregorian_to_jalali(end_date_greg)
        start_jdate_str = convert_gregorian_to_jalali(start_date_greg)

        if start_jdate_str and end_jdate_str:
            # Filter by jexit_date for closed signals in previous week
            # Or jentry_date if looking for signals that *started* in previous week
            # For "previous week's signals", let's assume signals that were *closed* in the previous week
            query = query.filter(
                SignalsPerformance.jexit_date >= start_jdate_str,
                SignalsPerformance.jexit_date <= end_jdate_str,
                SignalsPerformance.status.in_(['closed_win', 'closed_loss', 'closed_neutral']) # Only closed signals
            )
            logger.info(f"Filtering for previous week: jexit_date from {start_jdate_str} to {end_jdate_str}")
        else:
            logger.warning("Could not determine Jalali date range for 'previous_week' filter.")
            return [] # Return empty if date conversion fails
    
    # Default ordering
    signals = query.order_by(SignalsPerformance.exit_date.desc(), SignalsPerformance.created_at.desc()).all()

    output = []
    for s in signals:
        output.append({
            'signal_id': s.signal_id,
            'symbol_id': s.symbol_id,
            'symbol_name': s.symbol_name,
            'outlook': s.outlook,
            'reason': s.reason,
            'entry_price': s.entry_price,
            'jentry_date': s.jentry_date,
            'entry_date': safe_date_format(s.entry_date),
            'exit_price': s.exit_price,
            'jexit_date': s.jexit_date,
            'exit_date': safe_date_format(s.exit_date),
            'profit_loss_percent': s.profit_loss_percent,
            'status': s.status,
            'signal_source': s.signal_source,
            'probability_percent': s.probability_percent,
            'created_at': safe_date_format(s.created_at, '%Y-%m-%d %H:%M:%S'),
            'updated_at': safe_date_format(s.updated_at, '%Y-%m-%d %H:%M:%S')
        })
    
    logger.info(f"Retrieved {len(output)} detailed SignalsPerformance records.")
    return output