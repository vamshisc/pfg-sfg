import ibm_db
import ibm_db_dbi
import pandas as pd

# DB2 connection details
db_conn_str = "DATABASE=your_db;HOSTNAME=your_host;PORT=50000;PROTOCOL=TCPIP;UID=your_user;PWD=your_password;"

# Input name
input_name = 'ABC'

# Connect to DB2
conn = ibm_db.connect(db_conn_str, "", "")
db = ibm_db_dbi.Connection(conn)

def detect_role(input_name):
    """Determine if input is PRODUCER or CONSUMER"""
    producer_name = f"{input_name}_PRODUCER"
    consumer_name = f"{input_name}_CONSUMER"

    sql = "SELECT OBJECT_NAME FROM SCI_PROFILE WHERE OBJECT_NAME IN (?, ?)"
    stmt = db.cursor()
    stmt.execute(sql, (producer_name, consumer_name))
    results = stmt.fetchall()
    if producer_name in [r[0] for r in results]:
        return 'PRODUCER', producer_name
    elif consumer_name in [r[0] for r in results]:
        return 'CONSUMER', consumer_name
    else:
        return 'UNKNOWN', None

def get_codelists_for_producer(producer_name):
    """Get codelists linked to producer"""
    sql = "SELECT DISTINCT LIST_NAME FROM CODELIST_XREF_ITEMS WHERE SENDER_ITEM=?"
    df = pd.read_sql(sql, db, params=(input_name,))
    return df['LIST_NAME'].tolist()

def get_latest_versions(list_names):
    """Get latest version for each codelist"""
    versions = {}
    for name in list_names:
        sql = "SELECT DEFAULT_VERSION FROM CODELIST_XREF_VERS WHERE LIST_NAME=?"
        df = pd.read_sql(sql, db, params=(name,))
        versions[name] = df.iloc[0][']()
