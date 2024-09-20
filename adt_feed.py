import socket
import time
import pyodbc

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(('', 8200))

while True:
    server_socket.listen()
    client, address = server_socket.accept()
    print("{} connected".format( address ))
    time.sleep(0.2)
    response = client.recv(8112)

    default_ack = "\x0b" + r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3" + "\r\x1c\r"
    if response != "":
        client.send(default_ack.encode())
    
    cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
    cursor = cnxn.cursor()
    cursor.execute('''
            INSERT INTO adt_feed_raw (raw_message) 
            VALUES (?)
            ''', (response.decode('utf-8'),))
    cursor.commit()
    cnxn.close()
    
    client.close()