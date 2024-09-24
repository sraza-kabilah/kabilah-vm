import socket

server_address = ('localhost', 8200)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(server_address)

# Send multiple test messages
for i in range(15):
    message = f"Test message {i+1}"
    client_socket.sendall(message.encode())
    # Receive ACK from the server
    ack = client_socket.recv(1024)
    print(f"Received ACK: {ack.decode()}")

# Close the connection
client_socket.close()
