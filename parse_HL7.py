def get_event_type_and_patient_class(hl7_message):
    """
    Extracts the Event Type from the MSH segment and the Patient Class from the PV1 segment in an HL7 message.

    Parameters:
    hl7_message (str or bytes): The HL7 message to parse.

    Returns:
    tuple:
        - event_type (str): The Event Type (e.g., 'ADT^A08'), or None if not found.
        - patient_class (str): The Patient Class (e.g., 'I', 'O'), or None if not found.
    """
    # If the message is bytes, decode it to a string
    if isinstance(hl7_message, bytes):
        hl7_message = hl7_message.decode('utf-8')

    # Remove any starting/trailing special characters
    hl7_message = hl7_message.strip('\x0b').strip('\x1c\r')

    # Split the message into segments
    segments = hl7_message.split('\r')

    # Initialize variables
    event_type = None
    patient_class = None

    # Iterate over the segments
    for segment in segments:
        fields = segment.split('|')
        if fields[0] == 'MSH':
            # Event Type is in field 9 (MSH-9), index 8
            if len(fields) >= 9:
                event_type = fields[8]
        elif fields[0] == 'PV1':
            # Patient Class is in field 2 (PV1-2), index 2
            if len(fields) >= 3:
                patient_class = fields[2]
        # Break early if both values are found
        if event_type is not None and patient_class is not None:
            break

    return event_type, patient_class


# message = b'\x0bMSH|^~\\&|HIHLSEA-230502|EAGLE 2000|||20240923233546||ADT^A08|22409150240031769001|P|2.3|||NE|NE|\rEVN|A08|20240923233542||VHF|JJ6\rPID|0001||1622237||ROMAN^CHRISTINA^""^""^""^""|S SONIA&""|19890626|F|""|O~""|3724 FLATLANDS AVE^3F^BROOKLYN^NY^11234^US^^^KING|KING|(347)424-6969~(347)424-6969^^CP|""|EN|S|C|35800766|113-80-0290|||H~""|""|||||||""\rNK1|0001|ROMAN^SONI|A|2^MOTHER|3724FLATLANDS AVE^3F^BROOKLYN^NY^11234^US^^^KING|(347)359-5182|""|N.O.K.|||""|||""|""|F|""||||||||||""|||||||||||""\rNK1|0002|MORENO^ALEXANDER|C^OTHER|""|(929)672-7874|""|NOTY IFY|||""|||""|""|M|""||||||||||""|||||||||||""\rPV1|0001|O|""^""^""^GEM001^^GEMERG|""|||5417  ^WILLIAMS^MOLLIE^V.|5417  ^WILLIAMS^MOLLIE^V.|5417  ^WILLIAMS^MOLLIE^V.|EMR||||B|V|||O|24093 231317|SLF||||||||||||||||A|||G|||||20240923131700|20240923174100||||||V|\rPV2|||""^VAGINAL BLEEDING, WEAKNESS||||||\rGT1|0001||ROMAN^CHRISTINA||3724 FLATLANDS AVE^3F^BROOKLYN^NY^11234^^ US^^^KING|(347)424-6969~(347)424-6969^^CP|""|19890626|F||SLF^SELF|113-80-0290||||ORIGINS|""|""||E||||||||||S||||||||||||SONIA\rIN1|0001|SLFJ|""|SELF-PAY|""|""|""|""|""|||""|""|""|P|ROMAI N^CHRISTINA|SLF^SELF|19890626|3724 FLATLANDS AVE^3F^BROOKLYN^NY^11234^US^^^KING|""|""||||||""|||||||||""||||||E|F|""||\rIN2||113800290|ORIGINS|||""|||||||||||||||||||""|""||||||||||||||| |||||||||||||||||||""|||||(347)424-6969~(347)424-6969^^CP|""\rZEG|""|""|E|""|""|""|""|""|""|""|""|""|""||||||||||""|""|""|""|""|""||||||||||""|""|A|N|Y||""|""|""|""|AAAA  ^NOPCP^PRIMARYC2 ARE|""|20240923131700|""|""||""||||""|""|""||||""|""|""||||""|""|""||||""|""|""||||""|""|""||||||||||||||||||||||9ZZJML\rZUD|0001|""~Y~Y~011224~Y~""~""~""~011224~""~""~""~""~""~""~""~"" ~""~""~""||42~""~AAA~""~""~""||""~""~""~""~""~""~""~NONE~""~""~""~""|""|""|N|""|\r\x1c\r'
# print(get_event_type_and_patient_class(message))