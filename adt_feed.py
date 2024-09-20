import socket
import time

socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket.bind(('', 8200))

while True:
    socket.listen()
    client, address = socket.accept()
    print("{} connected".format( address ))
    time.sleep(0.2)
    response = client.recv(8112)
    if response != "":
        client.send(b"\x06")