from flask import Flask,request,render_template,redirect
import json
import csv
import pandas as pd
import shutil
from deepdiff import DeepDiff
import mysql.connector
from datetime import datetime, timedelta
from fuzzywuzzy.process import fuzz, extractOne
from fuzzywuzzy import process
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
app = Flask(__name__)
df1 = pd.read_csv('concert_sports.csv')
df2 = pd.read_csv('only_concert.csv')
column_mapping = {
    'event_id': 'e_id',
    'event_name': 'e_name',
    'event_date': 'event_date',
    'event_location': 'event_place',
    'event_description': 'e_dest',
    'event_category': 'e_category'
}
df1 = df1.rename(columns=column_mapping)
df1['e_id'] = df1['e_id'].astype(str)

global_df = pd.concat([df1, df2], ignore_index=True)

def fuzzy_merge(row):
    e_name = row['e_name']
    if pd.notna(e_name):  # Check if it's not NaN
        matches = process.extractOne(e_name, global_df['e_name'].dropna())
        if matches[1] >= 90:  # You can adjust the threshold as needed
            return global_df[global_df['e_name'] == matches[0]]['e_id'].values[0]
    return row['e_id']

global_df['e_id'] = global_df.apply(fuzzy_merge, axis=1)

def similarity_score(row1, row2):
    e_name1 = str(row1['e_name']) if pd.notna(row1['e_name']) else ""
    e_name2 = str(row2['e_name']) if pd.notna(row2['e_name']) else ""
    event_place1 = str(row1['event_place']) if pd.notna(row1['event_place']) else ""
    event_place2 = str(row2['event_place']) if pd.notna(row2['event_place']) else ""
    
    name_similarity = fuzz.ratio(e_name1, e_name2)
    place_similarity = fuzz.ratio(event_place1, event_place2)
    
    # Standardize date formats to DD-MM-YYYY
    def standardize_date(date_str):
        if isinstance(date_str, str):
            try:
                date_obj = datetime.strptime(date_str, "%m-%d-%Y")
                return date_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        return None


    date1 = standardize_date(row1['event_date'])
    date2 = standardize_date(row2['event_date'])
    
    if date1 is not None and date2 is not None:
        date_similarity = 100 if date1 == date2 else 0
    else:
        date_similarity = 0
    
    return name_similarity, place_similarity, date_similarity

global_df = global_df.drop_duplicates(subset=['e_id'])

global_df.reset_index(drop=True, inplace=True)

global_df = global_df.rename(columns={'e_category': 'Event_details', 'Unnamed: 6': 'event category'})

global_df.to_csv('concert_data.csv', index=False)



def combine_and_store_data():
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

    # Read concert data from CSV file
    concert_data = []
    with open("concert_data.csv", "r") as csv_file2:
        csv_reader = csv.DictReader(csv_file2)
        for row in csv_reader:
            concert_data.append(row)

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

    #Insert concert data into databse
    for concert_event in concert_data:
        event_date = datetime.strptime(concert_event["event_date"], "%m-%d-%Y").strftime("%Y-%m-%d %H:%M:%S")
        insert_query = "INSERT INTO events (event_id, event_name, event_date, event_location, event_description, event_category) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (
            concert_event["e_id"],
            concert_event["e_name"],
            event_date,
            concert_event["e_dest"],
            concert_event["Event_details"],
            "Concert",
        )
        cursor.execute(insert_query, values)
        conn.commit()

    # Close the database connection
    cursor.close()
    conn.close()
def get_locations():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Assuming your locations are stored in a 'locations' table
    select_locations_query = "SELECT DISTINCT event_location FROM events"
    cursor.execute(select_locations_query)
    locations = [location[0] for location in cursor.fetchall()]

    cursor.close()
    conn.close()

    return locations
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
def compare_e_ids_csv(main_file_path, temp_file_path):
    # Load CSV files into pandas DataFrames
    main_file = pd.read_csv(main_file_path)
    temp_file = pd.read_csv(temp_file_path)

    # Find differences in event IDs
    differences = set(temp_file['e_id']).symmetric_difference(set(main_file['e_id']))

    if differences:
        shutil.copyfile(main_file_path, temp_file_path)

    return not bool(differences)

@app.route("/filter_events", methods=["POST"])
def filter_events():
    # Connect to the database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Get filter parameters from the form
    selected_category = request.form.get("category")
    selected_location = request.form.get("location")
    start_date_str = request.form.get("start_date")
    end_date_str = request.form.get("end_date")

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None

    # Build the SQL query based on filter parameters
    select_query = "SELECT * FROM events WHERE 1=1"
    params = []

    if selected_category and selected_category != "None":
        select_query += " AND event_category = %s"
        params.append(selected_category)

    if selected_location and selected_location != "None":
        select_query += " AND event_location = %s"
        params.append(selected_location)

    if start_date:
        select_query += " AND event_date >= %s"
        params.append(start_date)

    if end_date:
        select_query += " AND event_date <= %s"
        # Increment the end_date by one day to include events on the end_date
        params.append(end_date + timedelta(days=1))

    # Execute the SQL query
    cursor.execute(select_query, tuple(params))
    filtered_events_data = cursor.fetchall()

    # Close the database connection
    cursor.close()
    conn.close()

    # Get locations for the dropdown menu
    locations = get_locations()

    # Generate date options for the dropdown menu
    today = datetime.today()
    date_options = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]

    # Pass the filtered data, filter parameters, date options, and locations to the template
    return render_template(
        'index.html',
        events_data=filtered_events_data,
        selected_category=selected_category,
        selected_location=selected_location,
        date_options=date_options,
        start_date=start_date_str,
        end_date=end_date_str,
        locations=locations
    )
@app.route("/events")
def show_events():
    # Connect to the database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Fetch data from the events table
    select_query = "SELECT * FROM events"
    cursor.execute(select_query)
    events_data = cursor.fetchall()

    # Close the database connection
    cursor.close()
    conn.close()

    # Get locations for the dropdown menu
    locations = get_locations()

    # Pass the data and locations to the template
    return render_template('index.html', events_data=events_data, locations=locations)


@app.route("/")
def index():
    main_file_path_f = 'football_data.csv'
    temp_file_path_f = 'football_data_temp.csv'

    main_file_path_b = 'basketball_data.json'
    temp_file_path_b = 'basketball_data_temp.json'

    main_file_path_c = 'concert_data.csv'
    temp_file_path_c = 'concert_data_temp.csv'

    result_football = compare_event_ids_csv(main_file_path_f, temp_file_path_f)
    result_basketball = compare_event_ids_json(main_file_path_b, temp_file_path_b)
    result_concert = compare_e_ids_csv(main_file_path_c, temp_file_path_c)

    #if result_basketball is False or result_football is False or result_concert is False:
    combine_and_store_data()

    today = datetime.today()
    date_options = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]

    locations = get_locations()  # Add this line
    return render_template('index.html', events_data=[], selected_category=None, date_options=date_options, locations=locations)


@app.route('/favicon.ico')
def favicon():
    return jsonify({"message": "No favicon here!"})
app.run(debug=True,port = 8990)