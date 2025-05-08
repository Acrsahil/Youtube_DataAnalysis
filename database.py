import sqlalchemy as sq

# Database Connection
DATABASE_URL = "mssql+pyodbc://@SAHIL-WINDOW\\SQLEXPRESS/YoutubeDataBase?&driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes&Encrypt=no"
engine = sq.create_engine(DATABASE_URL)

# Establish connection
try:
    conn = engine.connect()
    print("Connection successful!")
except Exception as e:
    print("Connection failed:", e)
    conn = None

