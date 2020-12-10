import psycopg2
import pandas

from dotenv import load_dotenv
import os


load_dotenv()

user = os.getenv("PRODUSER")
password = os.getenv("PRODPASSWORD")
host = os.getenv("PRODHOST")

# print(user, password, host)
connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port="5432",
    database="stocks",
)


cursor = connection.cursor()


def execute_many(df):
    """
    Using cursor.executemany() to insert the dataframe
    """
    print("started writting")
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
    # SQL quert to execute
    query = 'INSERT INTO "Mega"(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s, %%s)' % (
        cols,
    )
    cursor = connection.cursor()

    try:
        print("writting more")
        cursor.executemany(query, tuples)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(tuples)

        print("Error: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()
