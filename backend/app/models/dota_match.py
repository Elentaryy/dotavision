import psycopg2

class MatchManager:
    def __init__(self, db_conn):
        self.db_conn = db_conn