# services/market_overview_service.py
from flask import current_app
import pytse_client as tse
import pandas as pd
import requests
import json
import logging
from datetime import datetime
import jdatetime # برای JalaliDate
import numpy as np # برای مدیریت NaN و تقسیم بر صفر

# تنظیمات لاگینگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_market_indices():
    """
    Fetches real-time market index data from pytse-client using download_financial_indexes.
    Returns a dictionary of index data, or an empty dictionary if API calls fail or no data.
    """
    logger.info("Attempting to fetch market index data from pytse-client using download_financial_indexes.")
    index_data = {}

    if not current_app.config.get('PYTSE_CLIENT_AVAILABLE'):
        logger.error("pytse_client is not available. Cannot fetch real market index data.")
        return {}

    try:
        # Define the specific index symbols to download
        # These are common index names in pytse-client.
        # We pass these to download_financial_indexes to request specific indices.
        index_symbols_to_fetch = [
            "شاخص كل",
            "شاخص كل (هم وزن)",
            "شاخص قيمت (هم وزن)", # Using this for Farabourse-like general price index
            "شاخص صنعت",
        ]

        # Use download_financial_indexes which returns a dictionary of DataFrames
        financial_indexes = tse.download_financial_indexes(symbols=index_symbols_to_fetch, write_to_csv=False)

        if not financial_indexes:
            logger.warning("pytse-client download_financial_indexes returned no data.")
            return {}

        # Log all keys returned by pytse-client to help diagnose exact names
        logger.info(f"Keys available in financial_indexes: {list(financial_indexes.keys())}")

        # Initialize index_data with default values, which will be updated if data is found
        processed_index_data = {
            "Total_Index": {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None},
            "Equal_Weighted_Index": {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None},
            "Price_Equal_Weighted_Index": {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None},
            "Industry_Index": {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None},
        }

        # Create a reverse mapping for easier lookup based on the exact names pytse-client returns
        reverse_mapping = {
            "شاخص كل": "Total_Index",
            "شاخص كل (هم وزن)": "Equal_Weighted_Index",
            "شاخص قيمت (هم وزن)": "Price_Equal_Weighted_Index",
            "شاخص صنعت": "Industry_Index",
        }

        # Iterate through the actual keys returned by pytse-client and extract data from DataFrame
        for pytse_name_returned, df in financial_indexes.items():
            if not df.empty:
                latest_data = df.iloc[-1] # Get the last row (latest data)
                
                # NEW LOGGING: Print DataFrame columns and the latest data row
                logger.info(f"DataFrame columns for '{pytse_name_returned}': {df.columns.tolist()}")
                logger.info(f"Latest data row for '{pytse_name_returned}': {latest_data.to_dict()}")

                # Ensure 'date' is a datetime object before calling strftime
                date_obj = latest_data.get('date')
                if isinstance(date_obj, pd.Timestamp): # If it's a pandas Timestamp
                    formatted_date = date_obj.strftime('%Y-%m-%d')
                elif isinstance(date_obj, datetime): # If it's a standard datetime object
                    formatted_date = date_obj.strftime('%Y-%m-%d')
                else: # Fallback for other types
                    formatted_date = str(date_obj)

                # Convert 'close' and 'open' to numeric for calculation
                close_price = pd.to_numeric(latest_data.get('close'), errors='coerce')
                open_price = pd.to_numeric(latest_data.get('open'), errors='coerce')

                calculated_change = None
                calculated_percent = None # Changed variable name to match desired output

                if pd.notna(close_price) and pd.notna(open_price):
                    calculated_change = close_price - open_price
                    if open_price != 0: # Avoid division by zero
                        calculated_percent = (calculated_change / open_price) * 100 # Changed variable name
                    else:
                        calculated_percent = 0.0 # If open is zero, percent is effectively zero or undefined
                
                # Convert NumPy types to standard Python float/int for JSON serialization
                # Use float() to convert np.float64 or np.int64 to standard float
                # Handle None cases to avoid errors
                final_change = float(calculated_change) if calculated_change is not None else None
                
                # Round percent to 2 decimal places
                final_percent = round(float(calculated_percent), 2) if calculated_percent is not None else None # Changed variable name

                current_index_info = {
                    "value": float(close_price) if pd.notna(close_price) else None, # Convert to float, not string
                    "change": final_change,
                    "percent": final_percent, # <--- Changed field name here to "percent"
                    "date": formatted_date
                }

                # Use the reverse mapping to find the friendly name
                friendly_name = reverse_mapping.get(pytse_name_returned)
                
                if friendly_name:
                    processed_index_data[friendly_name] = current_index_info
                    logger.info(f"Mapped '{pytse_name_returned}' to {friendly_name} with data: {current_index_info}")
                else:
                    logger.warning(f"Returned index '{pytse_name_returned}' did not match any expected friendly names in reverse mapping.")
            else:
                logger.warning(f"DataFrame for index '{pytse_name_returned}' is empty.")
        
        index_data = processed_index_data # Update the main index_data dictionary

    except Exception as e:
        logger.error(f"Error fetching market index data from pytse-client: {e}", exc_info=True)
        return {}
    
    if not index_data:
        logger.warning("Failed to fetch any market index data. Returning empty dictionary.")
    return index_data


def fetch_real_commodity_prices():
    """
    Fetches real-time global commodity prices from Metals.dev API.
    Returns a dictionary of commodity prices, or an empty dictionary if API calls fail or no data.
    """
    logger.info("Attempting to fetch real commodity prices from Metals.dev API.")
    METALS_DEV_API_KEY = current_app.config.get('METALS_DEV_API_KEY', 'USXIBBPXNPFOPKR6BQ5N671R6BQ5N')

    if not METALS_DEV_API_KEY:
        logger.error("METALS_DEV_API_KEY is not set in Flask config. Cannot fetch commodity prices.")
        return {}

    url = f"https://api.metals.dev/v1/latest?api_key={METALS_DEV_API_KEY}&currency=USD&unit=toz"

    try:
        response = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "metals" not in data:
            logger.warning("No 'metals' key found in response.")
            return {}

        metals_data = data["metals"]
        
        # Map your internal names to metals.dev keys
        commodities = {
            "gold": "gold",
            "silver": "silver",
            "platinum": "platinum",
            "copper": "copper"
        }

        prices = {}
        for name, key in commodities.items():
            price = metals_data.get(key)
            if price is not None:
                prices[name] = price
                logger.info(f"Successfully fetched real price for {name}: {price}")
            else:
                logger.warning(f"Price not found for {name} ({key}) in response: {metals_data.get(key)}")

        return prices

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching commodity prices from Metals.dev:", exc_info=True)
    except Exception as e:
        logger.error("Unexpected error during commodity price fetch:", exc_info=True)

    logger.warning("Failed to fetch any real commodity prices. Returning empty dictionary.")
    return {}

def get_market_overview():
    """
    Combines market index data and commodity prices to provide a comprehensive market overview.
    """
    logger.info("Generating comprehensive market overview.")
    
    current_date_jalali = jdatetime.date.today().strftime("%Y/%m/%d")

    market_indices_data = fetch_market_indices()
    commodity_prices = fetch_real_commodity_prices()

    overview = {
        "date": current_date_jalali,
        "shakhes_kol": market_indices_data.get("Total_Index", {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None}),
        "shakhes_hamvazn": market_indices_data.get("Equal_Weighted_Index", {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None}),
        "farabourse": market_indices_data.get("Price_Equal_Weighted_Index", {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None}),
        "shakhes_sanat": market_indices_data.get("Industry_Index", {"value": 0.0, "change": 0.0, "percent": 0.0, "date": None}),
        "commodities": commodity_prices,
        # "is_dummy_data": False # REMOVED: No longer tracking dummy data explicitly
    }
    
    # REMOVED: Logic to set is_dummy_data is no longer needed
    # if not market_indices_data.get("Total_Index", {}).get("value") and \
    #    not market_indices_data.get("Equal_Weighted_Index", {}).get("value") and \
    #    not market_indices_data.get("Price_Equal_Weighted_Index", {}).get("value") and \
    #    not market_indices_data.get("Industry_Index", {}).get("value"):
    #     overview["is_dummy_data"] = True
    #     logger.warning("All market index data is missing. Setting is_dummy_data to True.")

    logger.info("Market overview generation complete.")
    return overview
