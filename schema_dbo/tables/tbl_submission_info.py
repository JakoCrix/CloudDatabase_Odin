# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# %% Submission Info Table
#cursor.execute("""DROP TABLE submission""")
cursor.execute("""CREATE TABLE submission_info (
                    idsubmission INT PRIMARY KEY,
                    idsubmission_reddit VARCHAR(10) NOT NULL, 
                    title VARCHAR(350) NOT NULL,
                    submissiontext VARCHAR(MAX) NULL,
                    createdatetime DATETIME NOT NULL,
                    url VARCHAR(MAX) NOT NULL,
                    originalcontent BIT NOT NULL
                    );""")

conn_odin_obj.commit()
conn_odin_obj.close()

