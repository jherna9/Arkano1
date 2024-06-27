import pandas as pd
import sqlite3

# Connect to the database 
conn = sqlite3.connect('db_complaints.db')
cursor = conn.cursor()

# Table 
table_name = 'tbl_complaints'

# Create the table with schema and unique constraint on complaint_id
create_table_query = '''
CREATE TABLE IF NOT EXISTS '''+table_name+''' (
    complaint_id INTEGER PRIMARY KEY,
    date_received TEXT,
    product TEXT,
    sub_product TEXT,
    issue TEXT,
    sub_issue TEXT,
    consumer_complaint_narrative TEXT,
    company_public_response TEXT,
    company TEXT,
    state TEXT,
    zip_code INTEGER,
    tags TEXT,
    consumer_consent_provided TEXT,
    submitted_via TEXT,
    date_sent_to_company TEXT,
    company_response_to_consumer TEXT,
    timely_response TEXT,
    consumer_disputed TEXT,
    UNIQUE(complaint_id)
);
'''

# Run the query
cursor.execute(create_table_query)
conn.commit()

# Close the initial connection
conn.close()

print("Database and table created successfully.")

# Read the file 
csv_file_path = 'complaints.csv'

new_headers = ['date_received','product','sub_product','issue','sub_issue','consumer_complaint_narrative',
               'company_public_response','company','state','zip_code','tags','consumer_consent_provided',
               'submitted_via','date_sent_to_company','company_response_to_consumer','timely_response',
               'consumer_disputed','complaint_id']
    
# Read the CSV, skip header and add new header
df = pd.read_csv(csv_file_path, header=None, skiprows=1)
df.columns = new_headers 

def insert_batch(df, table_name, batch_size=10000):
    with sqlite3.connect('db_complaints.db') as connection:
        cursor = connection.cursor()
        # Iterate the df and add the batches
        for start in range(0, len(df), batch_size):
            end = start + batch_size
            batch_df = df[start:end]

            # Convert the dataframe to a list of tuples
            records_to_insert = batch_df.to_records(index=False).tolist()

            # Create the SQL insert statement
            insert_query = f'''
            INSERT OR IGNORE INTO {table_name} (date_received, product, sub_product, issue, sub_issue, consumer_complaint_narrative,
                                                company_public_response, company, state, zip_code, tags, consumer_consent_provided,
                                                submitted_via, date_sent_to_company, company_response_to_consumer, timely_response,
                                                consumer_disputed, complaint_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''

            # Execute the insert statement with the records
            cursor.executemany(insert_query, records_to_insert)
            connection.commit()

insert_batch(df, table_name)
