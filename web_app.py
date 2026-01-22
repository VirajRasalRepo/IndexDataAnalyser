from flask import Flask, render_template
import mysql.connector

app = Flask(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "qwerty123456",
    "database": "analyzer_db"
}


def get_all_historical_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Comprehensive query including Greeks and IV
        query = """
                SELECT
                    Date, Time, Spot_price, Strike_price, ce_oi, ce_volume, ce_IV, ce_delta, ce_gamma, ce_theta, ce_price, ce_vega, pe_oi, pe_volume, pe_IV, pe_delta, pe_gamma, pe_theta, pe_price, pe_vega
                FROM NIFTY_OC_HISTORICAL
                ORDER BY Date DESC, Time DESC, Strike_price DESC
                    LIMIT 2000 \
                """

        cursor.execute(query)
        data = cursor.fetchall()
        return data

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


# ONLY ONE '@app.route("/")' and ONLY ONE 'def home()'
@app.route('/')
def home():
    all_data = get_all_historical_data()
    return render_template('index.html', data=all_data)


if __name__ == '__main__':
    app.run(debug=True, port=5000)