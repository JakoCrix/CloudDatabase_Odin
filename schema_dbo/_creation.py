# %% Admin
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()



# DBO Table Creations

