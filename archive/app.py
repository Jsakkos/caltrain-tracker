from flask import Flask, render_template, jsonify
import sqlite3
from datetime import datetime, time

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('caltrain_performance.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/delay_stats')
def delay_stats():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='train_arrivals'")
    if cursor.fetchone() is None:
        # Table doesn't exist, return empty data
        return jsonify({
            'overall': {'total': 0, 'delayed': 0},
            'morning': {'total': 0, 'delayed': 0},
            'evening': {'total': 0, 'delayed': 0}
        })
    # Overall delay stats
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trips,
            SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_trips
        FROM train_arrivals
    ''')
    overall_stats = cursor.fetchone()

    # Morning commute stats (6 AM to 10 AM)
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trips,
            SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_trips
        FROM train_arrivals
        WHERE time(scheduled_arrival) BETWEEN time('06:00:00') AND time('10:00:00')
    ''')
    morning_stats = cursor.fetchone()

    # Evening commute stats (4 PM to 8 PM)
    cursor.execute('''
        SELECT 
            COUNT(*) as total_trips,
            SUM(CASE WHEN delay > 0 THEN 1 ELSE 0 END) as delayed_trips
        FROM train_arrivals
        WHERE time(scheduled_arrival) BETWEEN time('16:00:00') AND time('20:00:00')
    ''')
    evening_stats = cursor.fetchone()

    conn.close()

    return jsonify({
        'overall': {
            'total': overall_stats['total_trips'],
            'delayed': overall_stats['delayed_trips']
        },
        'morning': {
            'total': morning_stats['total_trips'],
            'delayed': morning_stats['delayed_trips']
        },
        'evening': {
            'total': evening_stats['total_trips'],
            'delayed': evening_stats['delayed_trips']
        }
    })

if __name__ == '__main__':
    app.run(debug=True)