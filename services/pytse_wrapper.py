# services/pytse_wrapper.py
# Wrapper for pytse_client with error handling, retries, and safe fallbacks.

import requests
import time
import logging
import pandas as pd
import pytse_client as tse
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

# Setting up logging for this module
logger = logging.getLogger(__name__)


# ---------------------------
# HTTP GET with retries
# ---------------------------
def http_get(url, headers=None, max_retries=5, initial_delay=1):
    """
    Performs a safe HTTP GET request with retry logic and exponential backoff.
    Manages specific network and HTTP errors for better resilience.
    """
    retries = 0
    delay = initial_delay
    while retries < max_retries:
        try:
            # Set a timeout to prevent the worker from blocking indefinitely.
            response = requests.get(url, headers=headers, timeout=10)
            # Raise an HTTPError for bad status codes (4xx or 5xx)
            response.raise_for_status()
            return response

        except Timeout:
            logger.error(f"Timeout occurred while fetching {url}. Retrying...")
        except ConnectionError:
            logger.error(f"Connection error while fetching {url}. Retrying...")
        except HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for {url}. Retrying...")
        except RequestException as e:
            # Catches any other requests-related exceptions
            logger.error(f"Request failed: {e}. Retrying...")

        if retries < max_retries - 1:
            logger.info(f"Waiting {delay}s before next retry...")
            time.sleep(delay)
            delay *= 2  # exponential backoff

        retries += 1

    logger.error(f"Failed to fetch data from {url} after {max_retries} retries.")
    return None


# ---------------------------
# pytse_client Wrappers
# ---------------------------
def Ticker(symbol_name):
    """
    Safe wrapper for tse.Ticker object creation.
    """
    try:
        return tse.Ticker(symbol_name)
    except Exception as e:
        logger.error(f"Error creating Ticker object for {symbol_name}: {e}")
        return None


def download(symbols, write_to_csv=False, adjust=True, days_limit=None):
    """
    Wrapper for tse.download with optional days_limit filtering to limit data size.
    Handles both DataFrame and dict return types from the library with robust fallbacks.
    Note: The original library function does not support a timeout parameter,
    so we rely on our gevent worker for non-blocking behavior.
    """
    # A hard cap to prevent excessive memory usage if no days_limit is specified
    MAX_ROWS = 2000

    try:
        df = tse.download(
            symbols=symbols,
            write_to_csv=write_to_csv,
            adjust=adjust
        )

        # Ensure that if df is None, we return an empty DataFrame to avoid downstream errors.
        if df is None:
            return pd.DataFrame()

        # If days_limit is specified, use that; otherwise, use the MAX_ROWS hard cap
        limit = days_limit if days_limit is not None else MAX_ROWS

        if isinstance(df, dict):
            # Handle dict of DataFrames, returning an empty DataFrame for invalid data
            return {
                sym: data.tail(limit) if isinstance(data, pd.DataFrame) and not data.empty else pd.DataFrame()
                for sym, data in df.items()
            }
        elif isinstance(df, pd.DataFrame):
            # Handle single DataFrame
            return df.tail(limit)
        else:
            logger.warning(f"Unexpected return type from tse.download: {type(df)}. Returning empty DataFrame.")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error downloading data for symbols {symbols}: {e}")
        return pd.DataFrame()  # Safe fallback


def safe_download_batch(symbols, batch_size=20, days_limit=None, write_to_csv=False, output_filename="all_symbols_data.csv"):
    """
    Downloads data for a list of symbols in batches to manage memory usage.
    It handles errors gracefully, merges the data, and can write a single CSV file at the end.
    """
    all_data = []
    # Split the symbols list into chunks of the specified batch_size
    symbol_chunks = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    logger.info(f"Starting batch download for {len(symbols)} symbols in {len(symbol_chunks)} chunks.")
    
    for i, chunk in enumerate(symbol_chunks):
        logger.info(f"Processing batch {i + 1}/{len(symbol_chunks)} with {len(chunk)} symbols.")
        try:
            # Call the main download function for each batch. We do NOT pass write_to_csv here.
            chunk_data = download(symbols=chunk, days_limit=days_limit)
            
            # Check if the returned data is valid before concatenating
            if isinstance(chunk_data, dict) and chunk_data:
                # Concatenate all DataFrames from the dictionary into a single DataFrame
                all_data.append(pd.concat(chunk_data.values(), ignore_index=True))
            else:
                logger.warning(f"Batch {i + 1} returned empty or invalid data. Skipping...")

        except Exception as e:
            logger.error(f"Error in batch {i + 1}: {e}. Skipping this batch.")
            
    # Concatenate all collected dataframes into a final, single DataFrame
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Check if a single CSV file should be written
        if write_to_csv and not final_df.empty:
            try:
                final_df.to_csv(output_filename, index=False)
                logger.info(f"Successfully wrote all data to {output_filename}.")
            except Exception as e:
                logger.error(f"Failed to write data to CSV file {output_filename}: {e}")
        return final_df
    else:
        logger.error("No valid data was downloaded. Returning an empty DataFrame.")
        return pd.DataFrame()


def all_tickers():
    """
    Safe wrapper for tse.all_tickers.
    Note: The original library function does not support a timeout parameter.
    """
    try:
        return tse.all_tickers()
    except Exception as e:
        logger.error(f"Error fetching all tickers: {e}")
        return {}


def download_financial_indexes_safe(symbols, timeout=10, max_retries=3, backoff=2):
    """
    Safe wrapper for tse.download_financial_indexes.
    Adds a consistent error handling and logging layer.
    """
    try:
        # آرگومان‌های retries و backoff_factor را حذف کنید
        return tse.download_financial_indexes(symbols, timeout=timeout)
    except Exception as e:
        logger.error(f"Error downloading financial indexes for symbols {symbols}: {e}")
        return {}