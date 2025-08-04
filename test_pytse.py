# test_pytse.py
import pytse_client
from datetime import date

print("--- Checking pytse-client version and functionality ---")

# Check version (may not exist in all versions, but good to try)
try:
    print(f"pytse_client version: {pytse_client.__version__}")
except AttributeError:
    print("pytse_client module has no attribute '__version__'. This is common for some GitHub installs.")

# Test get_history with start_date and end_date
try:
    print("\nAttempting to call pytse_client.get_history with start_date and end_date...")
    # Use a common, active symbol like "خودرو" for testing
    test_df = pytse_client.get_history(symbols=["خودرو"], start_date=date(2024, 1, 1), end_date=date(2024, 1, 5))

    if not test_df.empty:
        print("SUCCESS: pytse_client.get_history supports start_date and end_date and returned data.")
        print("Sample data (first 5 rows):")
        print(test_df.head())
    else:
        print("SUCCESS: pytse_client.get_history supports start_date and end_date but returned empty data (might be no trades in range).")
except TypeError as e:
    print(f"FAILURE: pytse_client.get_history does NOT support start_date and end_date. Error: {e}")
except Exception as e:
    print(f"FAILURE: An unexpected error occurred during get_history test: {e}")

# Test Ticker initialization for a potentially problematic symbol like "ثملی"
try:
    print("\nAttempting to initialize Ticker for 'ثملی'...")
    ticker_test = pytse_client.Ticker("ثملی")
    if hasattr(ticker_test, 'history') and not ticker_test.history.empty:
        print("SUCCESS: Ticker for 'ثملی' initialized and has history.")
    else:
        print("WARNING: Ticker for 'ثملی' initialized but has no history. (This is expected for some symbols).")
except Exception as e:
    print(f"FAILURE: Could not initialize Ticker for 'ثملی'. Error: {e}")

print("\n--- pytse-client test completed ---")