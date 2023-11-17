import pandas as pd
from fuzzywuzzy import fuzz
import jellyfish  # For Soundex
from nltk.metrics import jaccard_distance
from nltk.util import ngrams
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

# Read the CSV files
file1 = 'D:\\IIITD\\IIA\\Demo\\concert_sports.csv'
file2 = 'D:\\IIITD\\IIA\\Demo\\only_concert.csv'

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Define column name mappings
column_mapping = {
    "event_id": "e_id",
    "event_name": "e_name",
    "event_date": "event_date",
    "event_location": "event_place",
    "event_description": "e_description",
    "event_category": "e_category"
}

# Rename columns to match the common names
df1 = df1.rename(columns=column_mapping)

# Merge the two data frames
global_df = pd.concat([df1, df2], ignore_index=True)

# Function to check for similarity between names
def is_similar(s1, s2):
    return fuzz.ratio(s1.lower(), s2.lower()) >= 90  # You can adjust the similarity threshold as needed

# Create a mapping of unique names
unique_names = {}
for name in global_df['e_name'].unique():
    matched = False
    for key, value in unique_names.items():
        if is_similar(name, key):
            unique_names[key].append(name)
            matched = True
            break
    if not matched:
        unique_names[name] = [name]

# Replace similar names with a common name
for common_name, similar_names in unique_names.items():
    for name in similar_names:
        global_df.loc[global_df['e_name'] == name, 'e_name'] = common_name

# Check for duplicates based on matching event_name, event_place, and event_date

# Phonetic similarity using Soundex
def soundex_similarity(s1, s2):
    return jellyfish.soundex(s1) == jellyfish.soundex(s2)

# Add more phonetic similarity measures as needed

# Remove duplicate entries
global_df = global_df.drop_duplicates(subset=['e_name', 'event_place', 'event_date'], keep='first')

# Export the resulting global schema to a new CSV file
global_df.to_csv('D:\\IIITD\\IIA\\Demo\\global_schema.csv', index=False)
