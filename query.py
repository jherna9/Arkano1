import sqlite3

# Connect database
conn = sqlite3.connect('db_complaints.db') 
cursor = conn.cursor()

query = """
    SELECT 
        product, 
        sub_product, 
        COUNT(*) AS num_complaints
    FROM 
        tbl_complaints
    WHERE 
        date_received >= '2023-01-01' AND date_received <= '2023-12-31'
    GROUP BY 
        product, 
        sub_product
    ORDER BY 
        product ASC, 
        sub_product ASC;
"""

# Run query
cursor.execute(query)

# Fetch all rows from the result set
results = cursor.fetchall()

# Print results
for row in results:
    print(f"Product: {row[0]}, Sub-Product: {row[1]}, Number of Complaints: {row[2]}")

# Close cursor and connection
cursor.close()
conn.close()