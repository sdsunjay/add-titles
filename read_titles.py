import config
import psycopg2

def database():

    # Define our connection string
    conn_string = "host='localhost' dbname='{}' user='{}' password = '{}'".format(
        config.DB_NAME, config.USER, config.PASSWORD)
    # print the connection string we will use to connect
    print("Connecting to database\n	->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    # cursor = conn.cursor()
    print("Connected!\n")
    return conn

def read_words(filename):
    open_file = open(filename, 'r')
    words_list =[]
    contents = open_file.readlines()
    for i in range(len(contents)):
         words_list.append(contents[i].strip('\n'))
    open_file.close()
    return words_list

def check_db_for_titles(conn, titles_list):
    """ query data from the movies table """
    try:
        cur = conn.cursor()
        for title in titles_list:
            # print('Title: ' + title)
            # "'{0}' is longer than '{1}'".format(name1, name2)
            cur.execute("SELECT true FROM movies where lower(title)=%s",(title))
            # print(cur.fetchone())
            # print("The number of parts: ", cur.rowcount)
            row = cur.fetchone()

            if row is None:
                print(title)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main():

    conn = database()
    titles_list= read_words('titles/small_apple_kaggle.txt')
    check_db_for_titles(conn, titles_list)

if __name__ == '__main__':
    main()
