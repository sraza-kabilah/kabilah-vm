import socket
import pyodbc
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
        # Parse the original HL7 message
        original_msg = parse_message(hl7_message)

        # Ensure the MSH segment exists and has required fields
        if not original_msg or not original_msg.msh:
            raise ValueError("MSH segment is missing or malformed")

        # Create a new ACK message
        ack_msg = Message("ACK")

        # MSH (Message Header) Segment: Copy some fields from the original message
        ack_msg.msh.msh_3 = original_msg.msh.msh_5 if original_msg.msh.msh_5 else 'UNKNOWN_SENDER'  # Sending application from original receiver
        ack_msg.msh.msh_5 = original_msg.msh.msh_3 if original_msg.msh.msh_3 else 'UNKNOWN_RECEIVER'  # Receiving application from original sender
        ack_msg.msh.msh_7 = datetime.now().strftime("%Y%m%d%H%M%S")  # Current Date/time of the message
        ack_msg.msh.msh_10 = original_msg.msh.msh_10 if original_msg.msh.msh_10 else 'UNKNOWN_CONTROL_ID'  # Control ID (same as original)
        ack_msg.msh.msh_12 = original_msg.msh.msh_12 if hasattr(original_msg.msh, 'msh_12') else '2.3'  # HL7 Version ID (use '2.3' if missing)

        # MSA (Message Acknowledgment) Segment
        ack_msg.msa.msa_1 = "AA"  # Acknowledgment code (AA = Application Acknowledgment: Accept)
        ack_msg.msa.msa_2 = original_msg.msh.msh_10  # Original message control ID
        
        return ack_msg.to_er7()  # Convert to ER7 format (pipe-delimited)
    except Exception as e:
        print(f"Error creating ACK message: {e}")
        raise e

while True:
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")

    # Receive data in chunks, accumulate the message until the end of the HL7 message
    message = ""
    while True:
        chunk = client_socket.recv(4096).decode('utf-8')
        if not chunk:
            break
        message += chunk
        # Check if the message contains the HL7 message termination character '\r'
        if '\r' in message:
            print("Complete HL7 message received.")
            break
    
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
        
        cnxn.commit()  # Use cnxn.commit() to commit the transaction
        print("Message successfully written to the database.")

        # Generate HL7-compliant acknowledgment message using create_ack function
        ack_message = create_ack(message)

        # Send back HL7-compliant acknowledgment to the client
        client_socket.send(ack_message.encode('utf-8'))
        print("Acknowledgment sent to the client.")

    except Exception as e:
        print(f"Error writing to database: {e}")

    finally:
        # Close the connection to the database
        cnxn.close()

    # Close the connection with the client
    client_socket.close()


