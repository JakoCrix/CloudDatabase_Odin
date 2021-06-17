# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

#cursor.execute("""DROP TABLE ticker_info""")
cursor.execute("""CREATE TABLE stock_info (
                    idstock INT PRIMARY KEY,
                    name VARCHAR(250) NOT NULL, 
                    ticker VARCHAR(15) NOT NULL,
                    country VARCHAR(30) NULL,
                    ipoyear INT NULL,
                    sector VARCHAR(30) NULL,
                    industry VARCHAR(70) NULL
                    );""")

conn_odin_obj.commit()
conn_odin_obj.close()