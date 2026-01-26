
import mysql.connector
import time
import sys
from datetime import datetime, time as dt_time
from dhanhq import dhanhq
import Utilities

# --- CONFIGURATION ---
CLIENT_ID = "1107702034"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY5MTcyNDgwLCJpYXQiOjE3NjkwODYwODAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NzAyMDM0In0.2FBJetXfMZKYXs0aEDArL5frVYb96h6HjR0ORANKdFlXajWOvGbsU9l-BxCP-RVK4yGFURxbFoR50RYl4MLeLQ"

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
        expiry = Utilities.get_expiry_list(dhan)
        if not expiry:
            print("Failed to fetch expiry list. Exiting.")
            return


        print("--- Pipeline active. ------")

        while True:

            now = datetime.now()
            current_time_now = now.time()

            # Market Hours: 09:15 to 15:30, Monday to Friday
            if dt_time(9, 15) <= current_time_now <= dt_time(15, 30) and now.weekday() < 5:
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
                        inner_data = response['data']['data']
                        oc_data = inner_data['oc']
                        spot_price = inner_data['last_price']

                        current_date = now.strftime('%Y-%m-%d')
                        current_time_str = now.strftime('%H:%M:%S')

                        ATM_Strike = Utilities.get_atm_strike(spot_price)
                        strike_step = 50
                        Min_Strike = ATM_Strike - (15 * strike_step)
                        Max_Strike = ATM_Strike + (15 * strike_step)

                        insert_query = """
                                       INSERT INTO NIFTY_OC_HISTORICAL (Date, Time, Spot_price, Strike_price,
                                                                        ce_oi, ce_volume, ce_IV, ce_delta, ce_gamma, \
                                                                        ce_theta, ce_price, ce_vega,
                                                                        pe_oi, pe_volume, pe_IV, pe_delta, pe_gamma, \
                                                                        pe_theta, pe_price, pe_vega)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                               %s, %s)
                                       """

                        stored_count = 0
                        for strike_str, data in oc_data.items():
                            strike_price = float(strike_str)

                            if Min_Strike <= strike_price <= Max_Strike:
                                ce = data.get('ce', {})
                                pe = data.get('pe', {})

                                values = (
                                    current_date, current_time_str, spot_price, strike_price,
                                    ce.get('oi', 0), ce.get('volume', 0), ce.get('implied_volatility', 0),
                                    ce.get('greeks', {}).get('delta', 0), ce.get('greeks', {}).get('gamma', 0),
                                    ce.get('greeks', {}).get('theta', 0), ce.get('last_price', 0),
                                    ce.get('greeks', {}).get('vega', 0),
                                    pe.get('oi', 0), pe.get('volume', 0), pe.get('implied_volatility', 0),
                                    pe.get('greeks', {}).get('delta', 0), pe.get('greeks', {}).get('gamma', 0),
                                    pe.get('greeks', {}).get('theta', 0), pe.get('last_price', 0),
                                    pe.get('greeks', {}).get('vega', 0)
                                )
                                cursor.execute(insert_query, values)
                                stored_count += 1

                        db_connection.commit()
                        print(f"[{current_time_str}] Stored {stored_count} strikes (ATM: {ATM_Strike})")

                    else:
                        print(f"Dhan Error: {response.get('remarks')}")

                except mysql.connector.Error as err:
                    print(f"Database Error: {err}")
                finally:
                    if db_connection and db_connection.is_connected():
                        cursor.close()
                        db_connection.close()

                time.sleep(4)

            else:
                # Still checking for Ctrl+C even when market is closed
                print(f"[{now.strftime('%H:%M:%S')}] Market Closed. Sleeping... (Ctrl+C to stop)")
                time.sleep(60)

    except KeyboardInterrupt:
        print("\n--- STOPPING SCRIPT ---")
        print("Finalizing any pending tasks and exiting gracefully. Goodbye!")
        sys.exit(0)


if __name__ == "__main__":
    run_pipeline()