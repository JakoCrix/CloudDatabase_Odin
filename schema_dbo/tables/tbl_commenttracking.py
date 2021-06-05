# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# %% commenttracking Table
#cursor.execute("""DROP TABLE commenttracking""")
cursor.execute("""CREATE TABLE commenttracking (
                    idcomment INT FOREIGN KEY REFERENCES comment_info(idcomment),
                    lastfetched DATETIME NOT NULL,
                    score INTEGER NULL,
                    stickied BIT NULL);
               """)

conn_odin_obj.commit()
conn_odin_obj.close()

