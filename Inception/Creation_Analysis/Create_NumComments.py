# %% Admin
from Helper.Connections_Database import connect_to_odinprod
conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# %%
# cursor.execute("DROP TABLE commentscount")
cursor.execute("""CREATE TABLE commentscount (
                    datetime_hour DATETIME NOT NULL,
                    idsubreddit INT NOT NULL,
                    sumcomments_hour INT NOT NULL
                    );""")
conn_odin_obj.commit()