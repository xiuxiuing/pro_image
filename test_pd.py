import sqlite3
import pandas as pd
conn = sqlite3.connect(':memory:')
conn.execute("CREATE TABLE t1 (id int)")
df = pd.DataFrame({'id': [1], 'new_col': ['a']})
try:
    df.to_sql('t1', conn, if_exists='append', index=False)
    print("Success")
except Exception as e:
    print("Error:", e)
