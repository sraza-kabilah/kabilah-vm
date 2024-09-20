import socket
import time
import pyodbc

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

# Create the server socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(('', 8200))

# Establish the database connection outside the loop for reuse
cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
cursor = cnxn.cursor()

while True:
    server_socket.listen()
    client, address = server_socket.accept()
    print("{} connected".format(address))
    time.sleep(0.2)
    
    # Receive response
    response = client.recv(8112)
    
    # Check for non-empty response
    if response != b'':
        print(response)
        
        # Send default ACK response
        default_ack = "\x0b" + r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3" + "\r\x1c\r"
        client.send(default_ack.encode())
        
        # Insert response into database
        try:
            cursor.execute('''
                INSERT INTO adt_feed_raw (raw_message) 
                VALUES (?)
            ''', (response,))
            cnxn.commit()
        except Exception as e:
            print(f"Database insert error: {e}")
    
    # Close client connection
    client.close()

# Properly close the database connection when done
cnxn.close()
