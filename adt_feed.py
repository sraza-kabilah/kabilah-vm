import socket

# Set up the default ACK message
default_ack = r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3"

# Function to handle incoming messages
def handle_adt_feed(connection, address):
    print(f"Connection established with {address}")
    try:
        while True:
            # Receive data from the client
            data = connection.recv(1024)
            if data:
                # Print the received feed
                print("Received ADT feed:")
                print(data.decode('utf-8'))

                # Send the ACK message
                connection.sendall(default_ack.encode('utf-8'))
                print("ACK sent")

            else:
                # No more data; close the connection
                print(f"Connection closed by {address}")
                break
    except Exception as e:
        print(f"Error handling connection: {e}")
    finally:
        connection.close()

def start_server(host='0.0.0.0', port=8200):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Server listening on port {port}")

        while True:
            connection, address = server_socket.accept()
            handle_adt_feed(connection, address)

start_server()