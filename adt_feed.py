import socket
import hl7

def create_hl7_ack(message):
    """
    Creates an HL7 ACK message for a given incoming HL7 message.
    """
    try:
        ack_message = hl7.parse(message)
        # Extract necessary information to create an ACK
        message_control_id = ack_message.segment('MSH')[9]
        
        # Create ACK message structure
        ack = hl7.parse(f"MSH|^~\\&|ACK|SENDER|RECEIVER|{ack_message.segment('MSH')[4]}||ACK^A01|{message_control_id}|P|2.3\r" +
                        "MSA|AA|{message_control_id}\r")
        return str(ack)
    except Exception as e:
        print(f"Error parsing HL7 message: {e}")
        return None

def handle_client(client_socket, client_address):
    """
    Handles communication with the connected client.
    Sends ACK immediately upon receiving any data.
    """
    print(f"Connection established with {client_address}")
    
    try:
        while True:
            data = client_socket.recv(1024)
            if data:
                hl7_message = data.decode('utf-8')
                print(f"Received data: \n{hl7_message}")
            
                # Send back an HL7 ACK immediately upon receiving any message
                ack_message = create_hl7_ack(hl7_message)
                if ack_message:
                    print(f"Sending ACK: \n{ack_message}")
                    client_socket.sendall(ack_message.encode('utf-8'))
                else:
                    print("Failed to generate ACK. Invalid HL7 message format.")
    
    except Exception as e:
        print(f"Error occurred: {e}")

def start_server(host='0.0.0.0', port=2575):
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
            client_socket, client_address = server_socket.accept()
            handle_client(client_socket, client_address)

# Call the start_server function directly to run the server
start_server()