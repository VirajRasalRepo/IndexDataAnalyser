from flask import Flask, render_template, jsonify
import mysql.connector
import datetime

app = Flask(__name__)
DB_CONFIG = {"host": "localhost", "user": "root", "password": "qwerty123456", "database": "analyzer_db"}


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/get_data')
def get_data_api():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM NIFTY_OC_HISTORICAL ORDER BY Date DESC, Time DESC, Strike_price DESC LIMIT 2000"
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            for key, value in row.items():
                if isinstance(value, (datetime.timedelta, datetime.date)):
                    row[key] = str(value)

        cursor.close()
        conn.close()

        # Return both the data and the current server time
        return jsonify({
            "data": rows,
            "last_updated": datetime.datetime.now().strftime("%H:%M:%S")
        })
    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == '__main__':
    app.run(debug=True, port=5000, use_reloader=False)