import socket
import pyodbc
import threading
from datetime import datetime, timedelta
from hl7apy.core import Message
from hl7apy.parser import parse_message

SQL_CONNECTION_STRING = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:kabilah-sqlserver-1.database.windows.net,1433;Database=TBHC-csv;Uid=kabilahsql;Pwd=Kabilah123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;'

# Set up the server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('0.0.0.0', 2575))  # Listen on all interfaces on port 2575
server.listen(500)

print("Listening for ADT messages...")

batch = []
batch_lock = threading.Lock()  # Lock to manage access to the batch
batch_event = threading.Event()  # Event to signal when to write the batch
BATCH_SIZE = 10  # Number of messages to collect before writing
BATCH_INTERVAL = 5  # Interval in seconds to write the batch if not full

def create_ack(hl7_message):
    ack_msg = Message("ACK")
    
    try:
        original_msg = parse_message(hl7_message)
        
        # If parsing succeeds, fill the ACK with values from the original message
        ack_msg.msh.msh_3 = original_msg.msh.msh_5 or 'UNKNOWN_SENDER'
        ack_msg.msh.msh_5 = original_msg.msh.msh_3 or 'UNKNOWN_RECEIVER'
        ack_msg.msh.msh_10 = original_msg.msh.msh_10 or 'UNKNOWN_CONTROL_ID'
        ack_msg.msh.msh_12 = original_msg.msh.msh_12 if hasattr(original_msg.msh, 'msh_12') else '2.3'
        ack_msg.msa.msa_1 = "AA"
        ack_msg.msa.msa_2 = original_msg.msh.msh_10 or 'UNKNOWN_CONTROL_ID'
    
    except Exception as e:
        # Handle the case where the original message is malformed
        print(f"Error creating ACK message, generating default ACK: {e}")
        # Set default values for the ACK
        ack_msg.msh.msh_3 = 'EAGLE 2000'
        ack_msg.msh.msh_5 = 'HIHLSEA-230502'
        ack_msg.msh.msh_10 = '22409150240012595001'
        ack_msg.msh.msh_12 = '2.3'
        ack_msg.msa.msa_1 = "AA"  # Changed to correct 
        ack_msg.msa.msa_2 = 'UNKNOWN_CONTROL_ID'
    
    # Set the timestamp in either case
    ack_msg.msh.msh_7 = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Return the ACK message in ER7 format
    return ack_msg.to_er7()


def batch_writer():
    """Periodically writes batched messages to the database."""
    while True:
        # Wait for either the batch to fill up or the batch interval to pass
        batch_event.wait(timeout=BATCH_INTERVAL)
        
        with batch_lock:
            if not batch:
                continue  # If the batch is empty, skip
            try:
                cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
                cursor = cnxn.cursor()

                # Batch insert all messages
                cursor.executemany('''
                INSERT INTO adt_feed_raw (raw_message) 
                VALUES (?)
                ''', [(msg,) for msg in batch])
                
                cnxn.commit()
                print(f"Batch of {len(batch)} messages written to the database.")
                batch.clear()  # Clear the batch after writing
            except Exception as e:
                print(f"Error writing batch to database: {e}")
            finally:
                cnxn.close()

        # Reset the event after writing
        batch_event.clear()

def process_message(client_socket, message, start_time):
    print("Message received.")
    
    try:
        # Create and send ACK immediately
        ack_message = create_ack(message)
        client_socket.send(ack_message.encode('utf-8'))
        ack_sent_time = datetime.now()
        print("Acknowledgment sent to the client.")
        
        # Calculate and log ACK latency
        ack_latency = (ack_sent_time - start_time).total_seconds()
        print(f"Total Latency (ACK sent): {ack_latency} seconds")

        # After sending the ACK, add the message to the batch for database writing
        print("Adding message to batch for database storage...")
        with batch_lock:
            batch.append(message)
            if len(batch) >= BATCH_SIZE:
                # If the batch is full, signal the batch writer
                batch_event.set()

    except Exception as e:
        print(f"Error during acknowledgment or batch processing: {e}")

    finally:
        client_socket.close()


# Start the batch writer thread
batch_writer_thread = threading.Thread(target=batch_writer, daemon=True)
batch_writer_thread.start()

while True:
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")

    message = ""
    start_time = datetime.now()

    while True:
        chunk = client_socket.recv(4096).decode('utf-8')
        if not chunk:
            break
        message += chunk
        if '\r' in message:
            print("Complete HL7 message received.")
            break
    
    # Start a new thread for processing each message
    processing_thread = threading.Thread(target=process_message, args=(client_socket, message, start_time))
    processing_thread.start()