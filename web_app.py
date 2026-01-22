from flask import Flask, render_template, jsonify
import mysql.connector
import datetime

app = Flask(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "qwerty123456",
    "database": "analyzer_db"
}


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/get_data')
def get_data_api():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        # Fetch data - includes your new OI_Diff column
        query = "SELECT * FROM NIFTY_OC_HISTORICAL ORDER BY Date DESC, Time DESC, Strike_price DESC LIMIT 2000"
        cursor.execute(query)
        rows = cursor.fetchall()

        processed_rows = []
        for row in rows:
            # Force all keys to lowercase for consistent frontend mapping
            new_row = {}
            for key, value in row.items():
                clean_key = key.lower()
                if isinstance(value, (datetime.timedelta, datetime.date)):
                    new_row[clean_key] = str(value)
                else:
                    new_row[clean_key] = value
            processed_rows.append(new_row)

        cursor.close()
        conn.close()

        return jsonify({
            "data": processed_rows,
            "last_updated": datetime.datetime.now().strftime("%H:%M:%S")
        })
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"data": [], "error": str(e)})


if __name__ == '__main__':
    app.run(debug=True, port=5000, use_reloader=False)