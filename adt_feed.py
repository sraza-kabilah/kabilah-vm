import socket
import pyodbc
import time

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

# Set up the server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('0.0.0.0', 2575))  # Listen on all interfaces on port 2575
server.listen(5)

print("Listening for ADT messages...")

# Store messages temporarily in a list
messages = []

# Set up a timing mechanism to rewrite every 30 minutes
last_rewrite_time = time.time()

while True:
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")

    # Receive data in chunks
    message = ""
    while True:
        chunk = client_socket.recv(4096).decode('utf-8')
        if not chunk:
            break
        print(chunk)
        message += chunk
    
    # Store the message in the list
    messages.append(message)
    
    print("Message received and stored in memory.")

    # Check if 30 minutes have passed
    current_time = time.time()
    if current_time - last_rewrite_time >= 120:
        cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = cnxn.cursor()	

        # Clear the table
        cursor.execute('TRUNCATE TABLE adt_feed_raw')
        
        # Insert the stored messages into the database
        for msg in messages:
            cursor.execute('''
            INSERT INTO adt_feed_raw (raw_message) 
            VALUES (?)
            ''', (msg,))
        
        cursor.commit()
        print("Database has been rewritten with the latest messages.")
        
        # Reset the timing mechanism
        last_rewrite_time = current_time
        
        # Clear the messages list
        messages.clear()
    
    # Close the connection
    client_socket.close()
