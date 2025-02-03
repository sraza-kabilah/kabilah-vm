import socket
import time
import pyodbc

# Constants and Configuration
SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'
BATCH_SIZE = 10
BATCH_TIMEOUT = 30
DEFAULT_ACK = "\x0b" + r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3" + "\r\x1c\r"

# Helper function to parse patient class
def get_patient_class(hl7_message):
    # If the message is bytes, decode it to a string
    if isinstance(hl7_message, bytes):
        hl7_message = hl7_message.decode('utf-8')

    # Remove any starting/trailing special characters
    hl7_message = hl7_message.strip('\x0b').strip('\x1c\r')
    segments = hl7_message.split('\r')
    
    patient_class = None
    for segment in segments:
        fields = segment.split('|')
        if fields[0] == 'PV1' and len(fields) >= 3:
            patient_class = fields[2]
            break
    return patient_class

# Database insertion function
def insert_batch_to_db(batch):
    if batch:
        print("Inserting batch into the database:")
        print(batch)
        try:
            with pyodbc.connect(SQL_CONNECTION_STRING) as cnxn:
                cursor = cnxn.cursor()
                cursor.executemany('INSERT INTO adt_feed_raw (raw_message) VALUES (?)', [(message,) for message in batch])
                cnxn.commit()
            print("Batch inserted into the database.")
        except Exception as e:
            print(f"Database insert error: {e}")
    else:
        print("Batch is empty. Nothing to insert into the database.")

# Socket setup
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(('', 8200))
server_socket.listen()

print("Server is listening on port 8200...")

while True:
    # Accept a new client connection
    client, address = server_socket.accept()
    print(f"{address} connected")

    message_batch = []
    last_insert_time = time.time()

    try:
        while True:
            response = client.recv(8112)
            if not response:
                print("Not Response, End of Client Session")
                # Break the inner loop if response is empty, signaling end of client session
                break

            print("Received message:", response)
            client.send(DEFAULT_ACK.encode())

            # Filter based on patient class
            patient_class = get_patient_class(response)
            print(f"Patient Class: {patient_class}")

            if patient_class == 'I':
                decoded_response = response.decode('utf-8')
                message_batch.append(decoded_response)
                print("Message added to batch.")

            # Check if it's time to insert the batch into the database
            if len(message_batch) >= BATCH_SIZE or (time.time() - last_insert_time) > BATCH_TIMEOUT:
                print("Batch size or timeout reached. Preparing to insert batch into the database.")
                insert_batch_to_db(message_batch)
                message_batch.clear()
                last_insert_time = time.time()
    finally:
        # Final batch insert for any remaining messages
        if message_batch:
            print("Final insert for remaining messages in batch.")
            insert_batch_to_db(message_batch)
        client.close()
        print(f"Connection with {address} closed. Waiting for new connection...")