import psycopg2
import pandas

connection = psycopg2.connect(
    user="galibin24",
    password="Nn240494",
    host="127.0.0.1",
    port="5432",
    database="local_stocks",
)

cursor = connection.cursor()


def execute_many(df):
    """
    Using cursor.executemany() to insert the dataframe
    """

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # print(tuples)
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
    # SQL quert to execute
    query = 'INSERT INTO "Mega"(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s, %%s)' % (
        cols,
    )
    cursor = connection.cursor()

    try:
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
