import csv
import happybase

# HBase connection details
hbase_host = 'localhost'
hbase_table = 'caesar_tags'
column_family = 'column_family'

# Connect to HBase
def connect_to_hbase():
  connection = happybase.Connection(host=hbase_host)
  table = connection.table(hbase_table)
  batch = table.batch()
  return connection, batch

def write_to_hbase(file_path, batch):
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        rows_added = 0
        for row in csvreader:
            row_key = str(row[0])  # Convert to string
            column_family_and_qualifier = row[1]
            value = str(row[2])  # Convert to string

            # Split the column family and qualifier
            column_family, qualifier = column_family_and_qualifier.split('#')

            # Construct the column
            column = f"{column_family}:{qualifier}"

            batch.put(row_key, {column: value})
            print("Added row: ", row_key, column, value)
            rows_added += 1
    if rows_added > 0:
        batch.send()

if __name__ == '__main__':
  file_path = 'hbase_import.csv'
  connection, batch = connect_to_hbase()
  write_to_hbase(file_path, batch)
  connection.close()
  print("Data imported successfully into HBase!")
