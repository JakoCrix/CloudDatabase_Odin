# %% Admin
import pyodbc
from Helper.Connections import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()


# %% comment Table
#cursor.execute("""DROP TABLE comment""")
cursor.execute("""CREATE TABLE comment (
                    idcomment INT PRIMARY KEY,
                    idcomment_reddit VARCHAR(10) NOT NULL, 
                    body VARCHAR(5000) NOT NULL,
                    idparent INT NOT NULL,
                    idparent_reddit VARCHAR(10) NOT NULL,
                    createdatetime DATETIME NOT NULL
                    );
               """)
conn_odin_obj.commit()


# %% commenttracking Table
#cursor.execute("""DROP TABLE commenttracking""")
cursor.execute("""CREATE TABLE commenttracking (
                    idsubmission INT FOREIGN KEY REFERENCES submission(idsubmission),
                    idsubreddit INT FOREIGN KEY REFERENCES subreddit(idsubreddit),
                    idcomment INT FOREIGN KEY REFERENCES comment(idcomment),
                    lastfetched DATETIME NOT NULL,
                    score INTEGER NULL,
                    upvoteratio FLOAT NULL,
                    stickied BIT NULL);
               """)
conn_odin_obj.commit()
conn_odin_obj.close()
