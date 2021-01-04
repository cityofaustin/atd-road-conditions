""" This app can be used as a dummy sensor data endpoint for development purposes.
You 'll need to set the IP address of your Sensor Class instance to this local host"""
from flask import Flask
app = Flask(__name__)

@app.route('/data.zhtml')
def data():
    return "1808   1759  1.028  27.05  30.21 1 1 DRY DRY 3 3 0.80 0.80 4 GOOD  78.18  27.19  45.29 -102."

if __name__ == "__main__":
    app.run()