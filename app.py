from flask import Flask,request,render_template
import json
import csv
import pandas as pd
app = Flask(__name__)


def compare_event_ids(main_file_path, temp_file_path):
    # Load CSV files into pandas DataFrames
    main_file = pd.read_csv(main_file_path)
    temp_file = pd.read_csv(temp_file_path)

    # Check if all event_id from temp_file are in main_file
    all_event_ids_present = temp_file['event_id'].isin(main_file['event_id']).all()

    return all_event_ids_present
@app.route("/")
def index():
    main_file_path = 'football_data.csv'
    temp_file_path = 'football_data_temp.csv'

    result = compare_event_ids(main_file_path, temp_file_path)
    print(result)
    return render_template('index.html', result=result, main_file_path=main_file_path, temp_file_path=temp_file_path)
    


app.run(debug=True)