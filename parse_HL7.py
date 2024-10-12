import re
from datetime import datetime

def parse_hl7_message(message):
    # Regular expression to find segment identifiers
    segment_regex = re.compile(r'([A-Z][A-Z0-9]{2})\|')

    segments = {}
    positions = []
    for match in segment_regex.finditer(message):
        positions.append((match.start(), match.group(1)))

    # Extract segments based on positions
    for segment in segments:
        fields = segment.split('|')
        print("other fields", fields)
        segment_type = fields[0]

    for i in range(len(positions)):
        start_pos = positions[i][0]
        segment_name = positions[i][1]
        end_pos = positions[i+1][0] if i+1 < len(positions) else len(message)
        segment_content = message[start_pos:end_pos+3]
        fields = segment_content.strip().split('|')
        segments[segment_name] = fields

    # print("SEGMENTS", segments['A02'])

    data = {
        'name': '',
        'age': '',
        'gender': '',
        'admit_date': '',
        'current_doctor': '',
        'unit': '',
        'room': '',
        'patient_class': '',
        'message_type': '',
        'mrn': ''
    }

    if 'MSH' in segments:
        msh_fields = segments['MSH']
        if len(msh_fields) >= 9:
            event_type = msh_fields[8]
            data['message_type'] = event_type.split('^')[1]

    if data['message_type'] in segments: 
        admit_date_fields = segments[data['message_type']]
        data['admit_date'] = admit_date_fields[1]


    if 'PID' in segments:
        pid_fields = segments['PID']
        # Name is in PID-5
        if len(pid_fields) > 5:
            mrn = pid_fields[3]
            data['mrn'] = mrn
            patient_name_field = pid_fields[5]
            name_components = patient_name_field.split('^')
            # Name format: Last^First^Middle^Suffix^Prefix^Degree
            first_name = name_components[1] if len(name_components) > 1 else ''
            last_name = name_components[0] if len(name_components) > 0 else ''
            data['name'] = f"{first_name} {last_name}".strip()

        # Date of Birth is in PID-7
        if len(pid_fields) > 7:
            dob_str = pid_fields[7]
            if dob_str:
                try:
                    dob = datetime.strptime(dob_str, '%Y%m%d')
                    today = datetime.today()
                    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                    data['age'] = age
                except ValueError:
                    data['age'] = 'Invalid DOB'
            else:
                data['age'] = 'Unknown'
        else:
            data['age'] = 'Unknown'

        # Gender is in PID-8
        if len(pid_fields) > 8:
            data['gender'] = pid_fields[8]
        else:
            data['gender'] = 'Unknown'

    # Parse PV1 segment
    if 'PV1' in segments:
        pv1_fields = segments['PV1']
        # print("PV1", pv1_fields)
        # Patient Class is in PV1-2
        if len(pv1_fields) > 2:
            data['patient_class'] = pv1_fields[2]

        # Room is in PV1-3
        if len(pv1_fields) > 3:
            room_field = pv1_fields[3]
            room_components = room_field.split('^')
            data['unit'] = str(room_components[0])
            data['room'] = '-'.join(room_components[1:])

        # Current Doctor is in PV1-7 (Attending Doctor)
        if len(pv1_fields) > 7:
            attending_doctor_field = pv1_fields[7]
            doctor_components = attending_doctor_field.split('^')
            # Doctor Name format: ID^LastName^FirstName^MiddleInitialOrName^Suffix^Prefix^Degree
            doctor_first_name = doctor_components[2] if len(doctor_components) > 2 else ''
            doctor_last_name = doctor_components[1] if len(doctor_components) > 1 else ''
            data['current_doctor'] = f"{doctor_first_name} {doctor_last_name}".strip()
        else:
            data['current_doctor'] = 'Unknown'
        

    return data


def get_event_type_and_patient_class(hl7_message):
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