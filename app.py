from flask import Flask,request,render_template
import json
import csv
import pandas as pd
import shutil
from deepdiff import DeepDiff
import mysql.connector

app = Flask(__name__)
def combine_and_store_data():
    # Define the MySQL database connection parameters
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "changeme",
        "database": "demo_db",
    }

    # Define the global schema for the combined data
    global_schema = {
        "event_id": "CHAR(36)",
        "event_name": "VARCHAR(255)",
        "event_date": "DATETIME",
        "event_location": "VARCHAR(255)",
        "event_description": "TEXT",
        "event_category": "VARCHAR(50)",
    }

    # Read football data from CSV file
    football_data = []
    with open("football_data.csv", "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            football_data.append(row)

    # Read basketball data from JSON file
    basketball_data = []
    with open("basketball_data.json", "r") as json_file:
        basketball_data = json.load(json_file)

    # Establish a connection to the MySQL database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Create a table in the database using the global schema
    create_table_query = "CREATE TABLE IF NOT EXISTS events ("
    for field, field_type in global_schema.items():
        create_table_query += f"{field} {field_type}, "
    create_table_query = create_table_query.rstrip(", ") + ")"
    cursor.execute(create_table_query)
    conn.commit()


    # Clear existing records in the events table
    clear_table_query = "DELETE FROM events"
    cursor.execute(clear_table_query)
    conn.commit()
    
    # Insert football data into the database
    for football_event in football_data:
        insert_query = "INSERT INTO events (event_id, event_name, event_date, event_location, event_description, event_category) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (
            football_event["event_id"],
            football_event["event_name"],
            football_event["event_date"],
            football_event["event_location"],
            football_event["event_description"],
            "Football",
        )
        cursor.execute(insert_query, values)
    conn.commit()

    # Insert basketball data into the database
    for basketball_event in basketball_data:
        insert_query = "INSERT INTO events (event_id, event_name, event_date, event_location, event_description, event_category) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (
            basketball_event["event_id_custom"],
            basketball_event["event_name_custom"],
            basketball_event["event_date_custom"],
            basketball_event["event_location_custom"],
            basketball_event["event_description_custom"],
            "Basketball",
        )
        cursor.execute(insert_query, values)
    conn.commit()

    # Close the database connection
    cursor.close()
    conn.close()

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

    # Find differences in event IDs
    differences = set(temp_file['event_id']).symmetric_difference(set(main_file['event_id']))

    if differences:
        shutil.copyfile(main_file_path, temp_file_path)

    return not bool(differences)



@app.route("/")
def index():
    main_file_path_f = 'football_data.csv'
    temp_file_path_f = 'football_data_temp.csv'

    main_file_path_b = 'basketball_data.json'
    temp_file_path_b = 'basketball_data_temp.json'

    result_football = compare_event_ids_csv(main_file_path_f, temp_file_path_f)
    result_basketball = compare_event_ids_json(main_file_path_b,temp_file_path_b)

    if result_basketball is False or result_football is False:
        combine_and_store_data()
    
    return render_template('index.html', 
                           football_main_file_path=main_file_path_f, 
                           football_temp_file_path=temp_file_path_f, 
                           football_result=result_football,
                           basketball_main_file_path=main_file_path_b,
                           basketball_temp_file_path=temp_file_path_b,
                           basketball_result=result_basketball)


@app.route('/favicon.ico')
def favicon():
    return jsonify({"message": "No favicon here!"})
app.run(debug=True,port = 8990)