import socket
import pyodbc

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

# Set up the server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('0.0.0.0', 2575))  # Listen on all interfaces on port 2575
server.listen(5)

print("Listening for ADT messages...")

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
    
    print("Message received and storing in the database...")

    # Write the message directly to the database
    try:
        cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = cnxn.cursor()	

        # Insert the message into the database
        cursor.execute('''
        INSERT INTO adt_feed_raw (raw_message) 
        VALUES (?)
        ''', (message,))
        
        cursor.commit()
        print("Message successfully written to the database.")

        # Send back an acknowledgment to the client
        acknowledgment = "ACK: Message received and stored successfully."
        client_socket.send(acknowledgment.encode('utf-8'))
        print("Acknowledgment sent to the client.")

    except Exception as e:
        print(f"Error writing to database: {e}")

    finally:
        # Close the connection to the database
        cnxn.close()

    # Close the connection with the client
    client_socket.close()
