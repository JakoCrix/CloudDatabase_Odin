# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

#cursor.execute("""DROP TABLE financialtracking""")
cursor.execute("""CREATE TABLE financialtracking (
                    idstock INT FOREIGN KEY REFERENCES stock_info(idstock),
                    lastfetched DATETIME NOT NULL,
                    marketcap FLOAT NULL,
                    volume INT NULL
                    );""")

conn_odin_obj.commit()
conn_odin_obj.close()