import socket

server_address = ('localhost', 8200)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(server_address)

# Send multiple test messages
for i in range(15):
    message = b'\x0bMSH|^~\\&|HIHLSEA-230502|EAGLE 2000|||20240923233546||ADT^A08|22409150240031769001|P|2.3|||NE|NE|\rEVN|A08|20240923233542||VHF|JJ6\rPID|0001||1622237||ROMAN^CHRISTINA^""^""^""^""|S SONIA&""|19890626|F|""|O~""|3724 FLATLANDS AVE^3F^BROOKLYN^NY^11234^US^^^KING|KING|(347)424-6969~(347)424-6969^^CP|""|EN|S|C|35800766|113-80-0290|||H~""|""|||||||""\rNK1|0001|ROMAN^SONI|A|2^MOTHER|3724FLATLANDS AVE^3F^BROOKLYN^NY^11234^US^^^KING|(347)359-5182|""|N.O.K.|||""|||""|""|F|""||||||||||""|||||||||||""\rNK1|0002|MORENO^ALEXANDER|C^OTHER|""|(929)672-7874|""|NOTY IFY|||""|||""|""|M|""||||||||||""|||||||||||""\rPV1|0001|O|""^""^""^GEM001^^GEMERG|""|||5417  ^WILLIAMS^MOLLIE^V.|5417  ^WILLIAMS^MOLLIE^V.|5417  ^WILLIAMS^MOLLIE^V.|EMR||||B|V|||O|24093 231317|SLF||||||||||||||||A|||G|||||20240923131700|20240923174100||||||V|\rPV2|||""^VAGINAL BLEEDING, WEAKNESS||||||\rGT1|0001||ROMAN^CHRISTINA||3724 FLATLANDS AVE^3F^BROOKLYN^NY^11234^^ US^^^KING|(347)424-6969~(347)424-6969^^CP|""|19890626|F||SLF^SELF|113-80-0290||||ORIGINS|""|""||E||||||||||S||||||||||||SONIA\rIN1|0001|SLFJ|""|SELF-PAY|""|""|""|""|""|||""|""|""|P|ROMAI N^CHRISTINA|SLF^SELF|19890626|3724 FLATLANDS AVE^3F^BROOKLYN^NY^11234^US^^^KING|""|""||||||""|||||||||""||||||E|F|""||\rIN2||113800290|ORIGINS|||""|||||||||||||||||||""|""||||||||||||||| |||||||||||||||||||""|||||(347)424-6969~(347)424-6969^^CP|""\rZEG|""|""|E|""|""|""|""|""|""|""|""|""|""||||||||||""|""|""|""|""|""||||||||||""|""|A|N|Y||""|""|""|""|AAAA  ^NOPCP^PRIMARYC2 ARE|""|20240923131700|""|""||""||||""|""|""||||""|""|""||||""|""|""||||""|""|""||||""|""|""||||||||||||||||||||||9ZZJML\rZUD|0001|""~Y~Y~011224~Y~""~""~""~011224~""~""~""~""~""~""~""~"" ~""~""~""||42~""~AAA~""~""~""||""~""~""~""~""~""~""~NONE~""~""~""~""|""|""|N|""|\r\x1c\r'
    client_socket.sendall(message)
    # Receive ACK from the server
    ack = client_socket.recv(1024)
    print(f"Received ACK: {ack.decode()}")

# Close the connection
client_socket.close()
