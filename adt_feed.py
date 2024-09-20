import socket
import traceback

# Set up the default ACK message
default_ack = r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3"

def handle_adt_feed(connection, address):
    print(f"Connection established with {address}")
    try:
        while True:
            # Receive data from the client
            data = connection.recv(1024)
            
            if not data:
                # No more data from client, break the loop
                print(f"Connection closed by {address}")
                break

            # Print the received feed
            print("Received ADT feed:")
            print(data.decode('utf-8'))

            # Send the ACK message
            connection.sendall(default_ack.encode('utf-8'))
            print("ACK sent")
    
    except socket.error as e:
        # Catch and print socket-specific errors
        print(f"Socket error: {e}")
        traceback.print_exc()
    except Exception as e:
        # Handle any other exceptions
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        # Always close the connection when done
        connection.close()
        print(f"Connection closed for {address}")

def start_server(host='0.0.0.0', port=8200):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        try:
            # Set socket options to reuse the address and avoid "address already in use" error
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind the server to the provided host and port
            server_socket.bind((host, port))
            server_socket.listen(5)  # Listen for up to 5 connections
            print(f"Server listening on port {port}")
        
            while True:
                # Accept incoming client connections
                connection, address = server_socket.accept()
                handle_adt_feed(connection, address)

        except socket.error as e:
            print(f"Socket binding error: {e}")
            traceback.print_exc()
        except Exception as e:
            print(f"Server error: {e}")
            traceback.print_exc()

start_server()