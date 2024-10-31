import socket
import time
import pyodbc
from parse_HL7 import get_event_type_and_patient_class

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

# Create the server socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(('', 8200))

# Establish the database connection outside the loop for reuse
server_socket.listen()
client, address = server_socket.accept()
print("{} connected".format(address))

# Parameters for batch insert
batch_size = 10  # Number of messages to accumulate before inserting
batch_timeout = 30  # Max time (in seconds) before flushing the batch to the DB
message_batch = []
last_insert_time = time.time()

while True:
    # Receive response
    response = client.recv(8112)
    
    # Check for non-empty response
    if response != b'':
        print(response)
        
        # Send ACK 
        default_ack = "\x0b" + r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3" + "\r\x1c\r"
        client.send(default_ack.encode())
        
        # Filtering 
        event_type, patient_class = get_event_type_and_patient_class(response)
        allowed_event_types = ['ADT^A01', 'ADT^A02', 'ADT^A03', 'ADT^A04', 'ADT^A05', 'ADT^A06', 'ADT^A07', 'ADT^A08', 'ADT^A09', 'ADT^A11', 'ADT^A17']
        print(event_type)
        print(patient_class)
        if event_type in allowed_event_types and patient_class == 'I':
            decoded_response = response.decode()
            message_batch.append(decoded_response)
            print(message_batch)

        # Upload Batch to DB
        if len(message_batch) >= batch_size or (time.time() - last_insert_time) > batch_timeout:
            print("BATCH" , message_batch)
            try:
                if len(message_batch) > 0:
                    cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
                    cursor = cnxn.cursor()
                    cursor.executemany('''
                        INSERT INTO adt_feed_raw (raw_message) 
                        VALUES (?)
                        ''', [(message,) for message in message_batch])
                    cnxn.commit()
                    cnxn.close()
                    message_batch.clear()
                    last_insert_time = time.time()
            except Exception as e:
                print(f"Database insert error: {e}")
    else:
        break
    
# Upload Leftovers to DB
if message_batch:
    try:
        cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = cnxn.cursor()
        cursor.executemany('''
            INSERT INTO adt_feed_raw (raw_message, event_type)
            VALUES (?)
            ''', [(message,) for message in message_batch])
        cnxn.commit()
        cnxn.close()

    except Exception as e:
            print(f"Database insert error: {e}")

# Close client connection
client.close()