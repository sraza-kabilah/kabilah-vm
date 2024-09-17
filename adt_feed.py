import socket
import pyodbc
import threading
from datetime import datetime
from hl7apy.core import Message
from hl7apy.parser import parse_message

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

# Set up the server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('0.0.0.0', 2575))  # Listen on all interfaces on port 2575
server.listen(500)

print("Listening for ADT messages...")

def create_ack(hl7_message):
    try:
        original_msg = parse_message(hl7_message)
        if not original_msg or not original_msg.msh:
            raise ValueError("MSH segment is missing or malformed")

        ack_msg = Message("ACK")
        ack_msg.msh.msh_3 = original_msg.msh.msh_5 if original_msg.msh.msh_5 else 'UNKNOWN_SENDER'
        ack_msg.msh.msh_5 = original_msg.msh.msh_3 if original_msg.msh.msh_3 else 'UNKNOWN_RECEIVER'
        ack_msg.msh.msh_7 = datetime.now().strftime("%Y%m%d%H%M%S")
        ack_msg.msh.msh_10 = original_msg.msh.msh_10 if original_msg.msh.msh_10 else 'UNKNOWN_CONTROL_ID'
        ack_msg.msh.msh_12 = original_msg.msh.msh_12 if hasattr(original_msg.msh, 'msh_12') else '2.3'

        ack_msg.msa.msa_1 = "AA"
        ack_msg.msa.msa_2 = original_msg.msh.msh_10
        
        return ack_msg.to_er7()
    except Exception as e:
        print(f"Error creating ACK message: {e}")
        raise e

def process_message(client_socket, message):
    print("Message received and storing in the database...")

    try:
        cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = cnxn.cursor()

        cursor.execute('''
        INSERT INTO adt_feed_raw (raw_message) 
        VALUES (?)
        ''', (message,))
        
        cnxn.commit()
        print("Message successfully written to the database.")

        ack_message = create_ack(message)
        client_socket.send(ack_message.encode('utf-8'))
        print("Acknowledgment sent to the client.")

    except Exception as e:
        print(f"Error writing to database: {e}")

    finally:
        cnxn.close()
        client_socket.close()

while True:
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")

    message = ""
    while True:
        chunk = client_socket.recv(4096).decode('utf-8')
        if not chunk:
            break
        message += chunk
        if '\r' in message:
            print("Complete HL7 message received.")
            break
    
    # Start a new thread for processing each message
    processing_thread = threading.Thread(target=process_message, args=(client_socket, message))
    processing_thread.start()