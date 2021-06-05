# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# %% Subreddit Info Table
#cursor.execute("""DROP TABLE subreddit_info""")
cursor.execute("""CREATE TABLE subreddit_info (
                    idsubreddit INT PRIMARY KEY,
                    idsubreddit_reddit VARCHAR(10) NOT NULL, 
                    createdatetime DATETIME NOT NULL,
                    title VARCHAR(50),
                    url VARCHAR(600) NOT NULL
                    );""")

conn_odin_obj.commit()
conn_odin_obj.close()


