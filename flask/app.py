import sqlite3
from flask import Flask, render_template

app = Flask(__name__)


def get_db_connection():
    conn = sqlite3.connect("sqlite_dt.db")
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def index():
    conn = get_db_connection()
    lfns = conn.execute("SELECT * FROM migdal_db").fetchall()
    conn.close()
    return render_template("index.html", mFiles=lfns)
