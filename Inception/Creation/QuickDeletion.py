# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# Delete Foreign Key Tables
cursor.execute("""DROP TABLE submissiontracking""")
cursor.execute("""DROP TABLE commenttracking""")
conn_odin_obj.commit()

# Delete Primary Key Tables
cursor.execute("""DROP TABLE comment_info""")
cursor.execute("""DROP TABLE subreddit_info""")
cursor.execute("""DROP TABLE submission_info""")
conn_odin_obj.commit()

conn_odin_obj.close()

