import mysql.connector
import time
import sys
from datetime import datetime, time as dt_time
from dhanhq import dhanhq
import Utilities

# --- CONFIGURATION ---
CLIENT_ID = "1107702034"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzcwMjE4MzQxLCJpYXQiOjE3NzAxMzE5NDEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NzAyMDM0In0.uPpqm8xW1v9xd8q8GzKYgNz3o9cReIfd8oqkeiH8CyDVR4DGMtS_I7Bys4OhdsK_yMh6FSTtjXFvi2PXzasiLQ"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "qwerty123456",
    "database": "analyzer_db"
}

dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

def run_pipeline():
    try:
        # Get expiry once at the start
        expiry_data = Utilities.get_expiry_list(dhan)

        if isinstance(expiry_data, list) and len(expiry_data) > 0:
            expiry = expiry_data[0]
        else:
            expiry = expiry_data

        if not expiry:
            print("Failed to fetch expiry list. Exiting.")
            return

        print(f"--- Pipeline active for Expiry: {expiry} ---")

        while True:
            now = datetime.now()
            # Market Hours Check: Monday-Friday, 09:15 to 15:30
            if 1==1 : #dt_time(9, 15) <= now.time() <= dt_time(15, 30) and now.weekday() < 5:
                db_connection = None
                try:
                    db_connection = mysql.connector.connect(**DB_CONFIG)
                    cursor = db_connection.cursor()

                    response = dhan.option_chain(
                        under_security_id=13,
                        under_exchange_segment="IDX_I",
                        expiry=expiry
                    )

                    if response.get('status') == 'success':
                        data_payload = response.get('data', {})
                        inner_data = data_payload.get('data', data_payload)
                        oc_data = inner_data.get('oc', {})
                        spot_price = inner_data.get('last_price', 0)

                        if not oc_data:
                            print(f"[{now.strftime('%H:%M:%S')}] API success, but Option Chain data is empty.")
                            time.sleep(5)
                            continue

                        current_date = now.strftime('%Y-%m-%d')
                        current_time_str = now.strftime('%H:%M:%S')

                        ATM_Strike = Utilities.get_atm_strike(spot_price)
                        strike_step = 50
                        Min_Strike = ATM_Strike - (15 * strike_step)
                        Max_Strike = ATM_Strike + (15 * strike_step)

                        insert_query = """
                            INSERT INTO NIFTY_OC_HISTORICAL 
                            (Date, Time, Spot_price, Strike_price, ce_oi, ce_volume, ce_IV, 
                            ce_delta, ce_gamma, ce_theta, ce_price, ce_vega, 
                            pe_oi, pe_volume, pe_IV, pe_delta, pe_gamma, 
                            pe_theta, pe_price, pe_vega)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                        stored_count = 0
                        for strike_str, strike_values in oc_data.items():
                            strike_price = float(strike_str)

                            if Min_Strike <= strike_price <= Max_Strike:
                                ce = strike_values.get('ce', {})
                                pe = strike_values.get('pe', {})

                                # Ensure dicts exist for greeks to avoid AttributeError
                                ce_greeks = ce.get('greeks', {}) if ce else {}
                                pe_greeks = pe.get('greeks', {}) if pe else {}

                                # Robust price extraction: checks last_price, then ltp, then defaults to 0
                                # This handles the 'zero price' issue if the key name is different
                                ce_price = ce.get('last_price', ce.get('ltp', 0)) if ce else 0
                                pe_price = pe.get('last_price', pe.get('ltp', 0)) if pe else 0

                                values = (
                                    current_date, current_time_str, spot_price, strike_price,
                                    ce.get('oi', 0) if ce else 0,
                                    ce.get('volume', 0) if ce else 0,
                                    ce.get('implied_volatility', 0) if ce else 0,
                                    ce_greeks.get('delta', 0),
                                    ce_greeks.get('gamma', 0),
                                    ce_greeks.get('theta', 0),
                                    ce_price,
                                    ce_greeks.get('vega', 0),
                                    pe.get('oi', 0) if pe else 0,
                                    pe.get('volume', 0) if pe else 0,
                                    pe.get('implied_volatility', 0) if pe else 0,
                                    pe_greeks.get('delta', 0),
                                    pe_greeks.get('gamma', 0),
                                    pe_greeks.get('theta', 0),
                                    pe_price,
                                    pe_greeks.get('vega', 0)
                                )
                                cursor.execute(insert_query, values)
                                stored_count += 1

                        db_connection.commit()
                        print(f"[{current_time_str}] Stored {stored_count} strikes (Spot: {spot_price})")

                    else:
                        print(f"Dhan Error: {response.get('remarks')}")

                except mysql.connector.Error as err:
                    print(f"Database Error: {err}")
                except Exception as e:
                    print(f"Unexpected Error: {e}")
                finally:
                    if db_connection and db_connection.is_connected():
                        cursor.close()
                        db_connection.close()

                # Sleep 4 seconds between iterations
                time.sleep(4)

            else:
                print(f"[{now.strftime('%H:%M:%S')}] Market Closed or Weekend. Sleeping...")
                time.sleep(60)

    except KeyboardInterrupt:
        print("\n--- STOPPING SCRIPT ---")
        sys.exit(0)

if __name__ == "__main__":
    run_pipeline()