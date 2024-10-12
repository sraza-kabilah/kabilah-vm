# Function to check if patient exists in the database
def patient_exists(mrn, cursor):
    select_query = "SELECT * FROM CopyPatientsADT WHERE MRN = ?"
    cursor.execute(select_query, (mrn,))
    return cursor.fetchone()

# Function to add a new patient
def add_patient(result, cursor, cnxn):
    insert_query = """
            INSERT INTO CopyPatientsADT (Name, Gender, Age, Room, Doctor, PatientClass, MRN, AdmitDate, Unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    cursor.execute(insert_query, (result['name'], result['gender'], result['age'],
                                      result['room'], result['current_doctor'], result['patient_class'], result['mrn'], result['admit_date'], result['unit']))
    cnxn.commit()

# Function to update patient information
def update_patient(result, cursor, cnxn):
    update_query = """
            UPDATE CopyPatientsADT
            SET Gender = ?, Age = ?, Room = ?, Doctor = ?, PatientClass = ?, Name = ?, AdmitDate = ?, Unit = ?
            WHERE MRN = ?
        """
    cursor.execute(update_query, (result['gender'], result['age'], result['room'],
                                      result['current_doctor'], result['patient_class'], result['name'], result['admit_date'], result['unit'], result['mrn']))
    cnxn.commit()

# Function to remove a patient
def remove_patient(mrn, cursor, cnxn):
    delete_query = "DELETE FROM CopyPatientsADT WHERE MRN = ?"
    cursor.execute(delete_query, (mrn,))
    cnxn.commit()