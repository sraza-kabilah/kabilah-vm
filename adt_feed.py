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
    print(response)
    default_ack = r"MSH|^~\&|||HIHLSEA-230502|EAGLE 2000|20240920010246||ACK|2.3|T|2.3" + "\n" + r"MSA|CA|2.3"
    if response != "":
        client.send(default_ack)