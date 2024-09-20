import socket
import time

def create_default_ack():
    """
    Creates a default ACK message with generic values in case of an error.
    """
    default_ack = r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3"
    return str(default_ack)

def handle_client(client_socket, client_address):
    """
    Handles communication with the connected client.
    Sends ACK immediately upon receiving any data.
    """
    print(f"Connection established with {client_address}")
    client_socket.settimeout(10)  # Set a timeout for receiving data (10 seconds)
    
    try:
        while True:
            try:
                data = client_socket.recv(1024)
                if data:
                    hl7_message = data.decode('utf-8')
                    print(f"Received data: \n{hl7_message}")
                    
                    # Send back an HL7 ACK immediately upon receiving any message
                    ack_message = create_default_ack()
                    print(f"Sending ACK: \n{ack_message}")
                    client_socket.sendall(ack_message.encode('utf-8'))

                    # Add a small delay to slow down the loop and give the client some time
                    time.sleep(1)

                else:
                    # If no data is received, the client has likely disconnected
                    print(f"Client {client_address} disconnected.")
                    break

            except socket.timeout:
                # Timeout happened, waiting for data again
                print(f"No data received from {client_address}, waiting...")
                time.sleep(1)  # Add a small delay before trying again

            except UnicodeDecodeError as decode_error:
                print(f"Error decoding data: {decode_error}")
                break  # If we can't decode the data, close the connection
    
    except socket.error as sock_err:
        print(f"Socket error occurred: {sock_err}")
    
    except Exception as e:
        print(f"General error occurred: {e}")
    
    finally:
        client_socket.close()

def start_server(host='0.0.0.0', port=8200):
    """
    Starts a TCP server to listen for HL7 messages and send ACKs.
    Keeps the connection open and continues to handle multiple messages.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(1)
        print(f"Listening on port {port} for HL7 ADT messages...")

        while True:
            try:
                client_socket, client_address = server_socket.accept()
                handle_client(client_socket, client_address)
            except socket.error as accept_error:
                print(f"Error accepting connections: {accept_error}")
            except Exception as general_error:
                print(f"Unexpected server error: {general_error}")

# Call the start_server function directly to run the server
start_server()
