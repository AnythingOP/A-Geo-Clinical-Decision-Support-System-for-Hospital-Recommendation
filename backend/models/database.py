import sqlite3
class Database:
    def __init__(self, db_path='aarogyasaathi.db'):
        self.path = db_path
    def get_hospitals_by_treatment(self, t_id):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT h.*, c.estimated_cost FROM Hospitals h JOIN Hospital_Treatment_Costs c ON h.hospital_id = c.hospital_id WHERE c.treatment_id = ?", (t_id,))
        d = [dict(r) for r in cur.fetchall()]
        conn.close()
        return d