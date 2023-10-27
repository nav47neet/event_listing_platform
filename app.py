from flask import Flask,request,render_template
import json
import csv
import pandas as pd
import shutil
from deepdiff import DeepDiff

app = Flask(__name__)

def compare_event_ids_json(main_file_path, temp_file_path):
    # Load JSON files
    with open(main_file_path, 'r') as main_file:
        main_data = json.load(main_file)

    with open(temp_file_path, 'r') as temp_file:
        temp_data = json.load(temp_file)

    # Find differences between temp_file and main_file
    differences = DeepDiff(main_data, temp_data, ignore_order=True)

    if differences:
        with open(temp_file_path, 'w') as temp_file:
            json.dump(main_data, temp_file, indent=2)

    return not bool(differences)

def compare_event_ids_csv(main_file_path, temp_file_path):
    # Load CSV files into pandas DataFrames
    main_file = pd.read_csv(main_file_path)
    temp_file = pd.read_csv(temp_file_path)

    # Check if all event_id from temp_file are in main_file
    all_event_ids_present = temp_file['event_id'].isin(main_file['event_id']).all()
    if all_event_ids_present is False:
        shutil.copyfile(main_file_path, temp_file_path)
    return all_event_ids_present


@app.route("/")
def index():
    main_file_path_f = 'football_data.csv'
    temp_file_path_f = 'football_data_temp.csv'

    main_file_path_b = 'basketball_data.json'
    temp_file_path_b = 'basketball_data_temp.json'

    result_football = compare_event_ids_csv(main_file_path_f, temp_file_path_f)
    result_basketball = compare_event_ids_json(main_file_path_b,temp_file_path_b)
    
    return render_template('index.html', result=result_basketball, main_file_path=main_file_path_f, temp_file_path=temp_file_path_f)



app.run(debug=True)