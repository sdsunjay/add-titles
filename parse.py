import  urllib3.exceptions
import sys, traceback
import config
import tmdbsimple as tmdb
import psycopg2
from datetime import datetime
import time
# from sqlalchemy.exc import IntegrityError
import argparse
import urllib3.request
import socket

def is_connected():
    try:
        # connect to the host -- tells us if the host is actually
        # reachable
        socket.create_connection(("www.google.com", 80))
        return True
    except OSError:
        pass
    return False

def database():
    """ connect to the database """
    # Define our connection string
    conn_string = "host='localhost' dbname='{}' user='{}' password = '{}'".format(
        config.DB_NAME, config.USER, config.PASSWORD)
    # print the connection string we will use to connect
    print("Connecting to database\n ->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    # cursor = conn.cursor()
    print("Connected!\n")
    return conn


def handle_genres(cur, movie_id, genre_ids):
    """ Connect a movie with genre(s) """
    dt = datetime.now()
    # insert movie id and each genre id into categorization table
    sql = "INSERT INTO categorizations(movie_id, genre_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
    for genre_id in genre_ids:
        cur.execute(sql, (movie_id, genre_id, dt, dt))
    return True

def check_movie_fields_exist(movie):
    # TODO (Sunjay) This function does not work properly
    # test it fully
    try:
        keySet = ['release_date', 'title', 'vote_count', 'vote_average', 'popularity',
                'poster_path', 'original_language', 'backdrop_path', 'adult',
                'overview']
        for key in keySet:
            if key not in movie or movie[key] == "":
                if movie['title'] is not None or movie['title'] != "":
                    print(movie['title'] + ' was missing '+ key + '\n')
                else:
                    print('Movie without a title was missing '+ key + '\n')
                return key
        return ''

    except:
        if movie['title'] is not None or movie['title'] != "":
            print(movie['title'] + ' cause an error. not inserted!\n')
        else:
            print('Movie without a title caused an error\n')
        return 'title'


def movie_exists(cur, movie_id):
    """ check if movie already exists in movies table """
    sql = "SELECT true FROM movies WHERE id = {0}".format(movie_id)
    cur.execute(sql)
    row = cur.fetchone()
    if row is None:
        return False
    return True

def set_blank_movie_key(movie, key):
    if key == 'release_date':
        return "2025-01-01 00:00:01"
    elif key == 'poster_path':
        if movie['backdrop_path'] is not None or movie['backdrop_path']:
            return movie['backdrop_path']
    return ""

def create_movie_production_company(cur, movie_id, company_id):
    """ Connect a movie with a production company """
    dt = datetime.now()
    sql = "INSERT INTO movie_production_companies(movie_id, company_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
    # execute the INSERT statement
    try:
        cur.execute(sql, (movie_id, company_id, dt, dt))
        return True
    except:
        return False

def create_company(cur, company):
    """ insert a new production company into the companies table """
    # print('create company')
    dt = datetime.now()
    sql = "INSERT INTO companies(id, name, logo_path, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"
    try:
        cur.execute(sql, (company['id'], company['name'], company['logo_path'], dt, dt))
        # print('Inserted: ' + company['name'])
        return True
    except:
        return False

def company_exists(cur, company_id):
    """ check if company already exists in companies table """
    cur.execute("SELECT true FROM companies WHERE id = %(id)s", {"id": company_id})
    row = cur.fetchone()
    # print('company exists')
    if row is None:
        return False
    return True

def company_and_movie_exists(cur, movie_id):
    """ check if movie already exists in movie_production_companies table """
    sql = "SELECT true FROM movie_production_companies WHERE movie_id = %(id)s"
    cur.execute(sql, {"id": movie_id})
    row = cur.fetchone()
    if row is None:
        return False
    # print('Movie already exists in movie_production_companies!')
    return True

def handle_companies(cur, movie_id, movie_companies):
    """ Insert new companies and connect them with movies """
    for company in movie_companies:
        if not company_exists(cur, company['id']):
            create_company(cur, company)
        return create_movie_production_company(cur, movie_id, company['id'])
    return True

def translate_status(status_string):
    """ translate status string into integer """
    # Allowed Values: Rumored, Planned, In Production, Post Production, Released, Canceled
    #  enum status: { rumored: 1, planned: 2, in_production: 3, post_production: 4, released: 0, cancelled: 5 }
    if status_string == 'Released':
        return 0
    elif status_string == 'Rumored':
        return 1
    elif status_string == 'Planned':
        return 2
    elif status_string == 'In Production':
        return 3
    elif status_string == 'Post Production':
        return 4
    elif status_string == 'Canceled' or status_string == 'Cancelled':
        return 5
    return -1

def update_movie_info(conn, cur, movie):
    """ Update vote_count, vote_average, budget, runtime, revenue, popularity, status, etc into movie table """
    sql = "UPDATE movies SET vote_count = %s, vote_average = %s, title = %s, tagline = %s, status = %s, poster_path = %s, backdrop_path = %s, overview = %s, popularity = %s, adult = %s, release_date = %s, budget = %s, revenue = %s, runtime = %s, updated_at = %s WHERE id = %s"
    # print(movie['title'])
    dt = datetime.now()
    updated_rows = 0
    try:
        movie_status = translate_status(movie['status'])
        if movie_status == -1:
            movie_status = 0
            print('A valid status was not found')
        # execute the UPDATE  statement
        cur.execute(sql, (movie['vote_count'], movie['vote_average'], movie['title'], movie['tagline'], movie_status, movie['poster_path'], movie['backdrop_path'], movie['overview'], movie['popularity'], movie['adult'], movie['release_date'], movie['budget'], movie['revenue'], movie['runtime'], dt, movie['id']))
        # print('Movie production companies: ' + movie['production_companies'])
        # get the number of updated rows
        updated_rows = cur.rowcount
        if updated_rows == 1:
            conn.commit()
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return False

def insert_movie(cur, movie):
    """ Insert new movie into database """
    dt = datetime.now()
    cur.execute("INSERT INTO movies(id, vote_count,\
                vote_average, title, popularity, poster_path,\
                original_language, backdrop_path, adult, overview,\
                release_date, status, created_at, updated_at)\
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                %s) ON CONFLICT (id) DO NOTHING",
                (movie['id'], movie['vote_count'], movie['vote_average'],
                    movie['title'], movie['popularity'], movie['poster_path'],
                    movie['original_language'], movie['backdrop_path'],
                    movie['adult'], movie['overview'], movie['release_date'],
                    '0', dt, dt))
    print("Inserted: " + movie['title'])
    return handle_genres(cur, movie['id'], movie['genre_ids'])

def handle_movie(conn, cursor, movie):
    try:
        key = check_movie_fields_exist(movie)
        if key:
            movie[key] = set_blank_movie_key(movie, key)
        if not movie_exists(cursor, movie['id']):
            # insert new movie title and additional info
            if insert_movie(cursor, movie):
                # Make the changes to the database persistent
                conn.commit()

        movie_dict = get_movie_dict(conn, cursor, movie['id'])
        return update_movie_and_company_info(conn, cursor, movie_dict)

    except psycopg2.DataError:
        print('Data Error in ' + movie['title'] + ' \n')
        return False
        # conn.rollback()
    except psycopg2.IntegrityError:
        print('Integrity Error in ' + movie['title'] + ' \n')
        return False
        # conn.rollback()


def handle_genre(conn, cur, genre):
    dt = datetime.now()
    cur.execute("INSERT INTO genres(id, name, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                (genre['id'], genre['name'], dt, dt))

def create_table(conn):
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS genres(
    id integer PRIMARY KEY,
    name text
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies( \
        id integer PRIMARY KEY, \
        vote_count integer, \
        vote_average NUMERIC (2, 1), \
        title text NOT NULL UNIQUE, \
        tagline text, \
        status integer, \
        popularity NUMERIC (6, 3), \
        poster_path text, \
        original_language VARCHAR(2), \
        backdrop_path text, \
        adult BOOLEAN, \
        overview text, \
        release_date date \
    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS categorizations( \
        id serial PRIMARY KEY, \
        movie_id INTEGER REFERENCES movies(id), \
        genre_id INTEGER REFERENCES genres(id) \
    )""")
    cur.close()

def sleep_until_connected(counter):
    print('Counter ' + str(counter) + ' Sleeping for 10 seconds.')
    while True:
        # Wait for 10 seconds to avoid rate limiting
        time.sleep(10)
        if is_connected():
            break
        else:
            print('Sleeping for 30 seconds')
            time.sleep(30)
    return True


def delete_movie(conn, cursor, movie_id):
    movie_id = str(movie_id)
    sql = "DELETE FROM categorizations WHERE movie_id = {0}".format(movie_id)
    cursor.execute(sql)
    sql = "DELETE FROM movie_lists WHERE movie_id = {0}".format(movie_id)
    cursor.execute(sql)
    sql = "DELETE FROM movie_production_companies WHERE movie_id = {0}".format(movie_id)
    cursor.execute(sql)
    sql = "DELETE FROM movie_user_recommendations WHERE movie_id = {0}".format(movie_id)
    cursor.execute(sql)
    sql = "DELETE FROM reviews WHERE movie_id = {0}".format(movie_id)
    cursor.execute(sql)
    sql = "DELETE FROM movies WHERE id = {0}".format(movie_id)
    cursor.execute(sql)
    # Make the changes to the database persistent
    conn.commit()

def delete_ids_in_file(conn, cursor, filename):

    with open(filename, "r") as file_in:
        lines = []
        for line in file_in:
            lines.append(line.rstrip())
        for movie_id in lines:
            delete_movie(conn, cursor, movie_id)

def validate_ids_in_file(conn, cursor, filename):

    with open(filename, "r") as file_in:
        lines = []
        for line in file_in:
            lines.append(line.rstrip())
        for movie_id in lines:
            if not movie_exists(cursor, movie_id):
                print('Error: Movie ID {0} does not exist'.format(movie_id))

def read_titles_from_file(conn, movie_cur, filename, search):

    movies_not_found = []
    movies_with_errors = []
    counter = 0
    inserted_movies = 0
    with open(filename, "r") as ins:
        for line in ins:
            movies = search.movie(**{'query': line.rstrip('\n')})
            counter = counter + 1
            if movies['total_results'] > 0:
                for movie in movies['results']:
                    counter = counter + 1
                    if handle_movie(conn, movie_cur, movie):
                        inserted_movies = inserted_movies + 1
                    else:
                        movies_with_errors.append(line)
            else:
                movies_not_found.append(line)
            if counter % 140 == 0:
                print(str(inserted_movies) + ' movies inserted or updated.')
            if counter % 35 == 0:
                sleep_until_connected(counter)


    print('Movies not found:')
    for title in movies_not_found:
        print(title)
    print('Movies with errors:')
    for title in movies_with_errors:
        print(title)
    print(str(counter) + ' searched.')
    print(str(inserted_movies) + ' movies inserted or updated.')

def update_movie_and_company_info(conn, cursor, movie_dict):
    try:
        if not movie_dict:
            return False
        key = check_movie_fields_exist(movie_dict)
        if key:
            movie_dict[key] = set_blank_movie_key(movie_dict, key)
        if update_movie_info(conn, cursor, movie_dict):
            if not company_and_movie_exists(cursor, movie_dict['id']):
                if handle_companies(cursor, movie_dict['id'], movie_dict['production_companies']):
                    # Make the changes to the database persistent
                    conn.commit()
                else:
                    print('Error occurred with: ' + movie_dict['title'] + ' (Companies)')
                    return False
            return True
        else:
            print('Error occurred with: ' + movie_dict['title'])
            return False
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")

def get_movie_dict(conn, cursor, movie_id):

    try:
        # Get movie info using existing movie id
        tmdb.API_KEY = config.API_KEY
        movie = tmdb.Movies(movie_id)
        movie_dict = movie.info()
        return movie_dict
    except:
        print("Error: movie id (" + str(movie_id) + ") not updated.")
        delete_movie(conn, cursor, movie_id)
        return False

def update_all_movies(conn, cursor):
    """ Call tmdb API to get details for a movie """
    tmdb.API_KEY = config.API_KEY
    counter = 0
    try:
        # Open a connection to the database
        conn1 = database()
        # Open a cursor to perform this one specific query
        movie_id_cursor = conn1.cursor('movie_id_cursor')
        # sql = "SELECT id, title FROM movies WHERE created_at > '2019-01-05 01:01:24' ORDER BY title LIMIT 100000"
        # select title from movies where updated_at < NOW() - INTERVAL '1 days' limit 10;
        sql = "SELECT id FROM movies where age(now(), created_at) > '30 days' AND age(now(), updated_at) > '1 days' ORDER BY title LIMIT 108000"
        movie_id_cursor.execute(sql)
        movies_updated = 0
        while True:
            rows = movie_id_cursor.fetchmany(5000)
            if not rows:
                break
            for row in rows:
                # check that the movie does not already exist in
                # movie_production_companies table
                movie_dict = get_movie_dict(conn, cursor, row[0])
                # if update_movie_and_company_info(conn, cursor, movie_dict):
                #    movies_updated = movies_updated + 1
                counter =  counter + 1
                if counter % 35 == 0:
                    print('Last movie ID: ' + str(row[0]))
                    # print('Last movie title: ' + str(row[1]))
                    print('Movie updated: ' + str(movies_updated))
                    print('Counter: ' + str(counter))
                    # Wait for 10 seconds to avoid rate limiting
                    sleep_until_connected(counter)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        print("Exception in user code:")
        print('-'*60)
        traceback.print_exc(file=sys.stdout)
        print('-'*60)
    finally:
        print('Final Counter: ' + str(counter))
        movie_id_cursor.close()
        # Close communication with the database
        conn1.close()

def help_add_movies(conn, movie_cur, dict_of_movies, page_number, counting_dict):

    counter = counting_dict['counter'] + 1
    movies_inserted = counting_dict['movies_inserted'] + 1

    if counter % 40 == 0:
        conn.commit()
        print('Page Number: ' + str(page_number))
        sleep_until_connected(counter)
    for movie in dict_of_movies['results']:
        flag = handle_movie(conn, movie_cur, movie)
        if not flag:
            print('Error: ' + movie['title'] + '\n')
        else:
            movies_inserted = movies_inserted + 1
        counter = counter + 1
        if counter % 40 == 0:
            conn.commit()
            print('Page Number: ' + str(page_number))
            sleep_until_connected(counter)

    return {'counter': counter, 'movies_inserted': movies_inserted}

def add_movies_in_theaters(conn, movie_cur, discover, counter_dictionary):
    for page_number in range(1, 10):
        dict_of_movies = discover.movie(**{'page': page_number, 'release_date.gte': '2013-01-01', 'release_date.lte': '2013-12-31'})
        counter_dictionary = help_add_movies(conn, movie_cur, dict_of_movies, page_number, counter_dictionary)

    print('Page Number: ' + str(page_number))
    print('Counter: ' + str(counter_dictionary['counter']))
    print('Movies Inserted: ' + str(counter_dictionary['movies_inserted']))

def add_movies_now_playing(conn, movie_cur, movies, counter_dictionary):
    for page_number in range(1, 100):
        dict_of_movies = movies.now_playing(**{'page':  page_number})
        counter_dictionary = help_add_movies(conn, movie_cur, dict_of_movies, page_number, conter_dictionary)

    print('Page Number: ' + str(page_number))
    print('Counter: ' + str(counter_dictionary['counter']))
    print('Movies Inserted: ' + str(counter_dictionary['movies_inserted']))

def add_popular_movies(conn, movie_cur, movies, counter_dictionary):
    for page_number in range(1, 500):
        dict_of_movies = movies.popular(**{'page':  page_number})
        counter_dictionary = help_add_movies(conn, movie_cur, dict_of_movies, page_number, counter_dictionary)

    print('Page Number: ' + str(page_number))
    print('Counter: ' + str(counter_dictionary['counter']))
    print('Movies Inserted: ' + str(counter_dictionary['movies_inserted']))

# query and store all movies from the API
def main():
    parser = argparse.ArgumentParser(
        description='A script for reading title into movies database')
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity", action="store_true")
    parser.add_argument(
        "-u", "--update", help="update movie info and movie companies", action="store_true")
    parser.add_argument(
        "-p", "--popular", help="insert all popular movies into the database", action="store_true")
    parser.add_argument(
        "-t", "--theaters", help="insert all movies in theaters into the database", action="store_true")
    parser.add_argument('-f', "--filename", nargs='*',
                        type=str, help='insert movie titles from file into the database')
    parser.add_argument('-d', "--delete", nargs='*',
                        type=str, help='delete movie IDs in file from the database')
    parser.add_argument('-c', "--check", nargs='*',
                        type=str, help='validate movie IDs in file exist in the database')
    args = parser.parse_args()

    datetime = time.strftime('%Y-%m-%d %H:%M:%S')

    tmdb.API_KEY = config.API_KEY
    # Open a connection to the database
    try:
        conn = database()
        # Open a cursor to perform database operations
        movie_cur = conn.cursor()
        counter_dictionary = {'counter': 0, 'movies_inserted': 0}


        if args.filename:
            search = tmdb.Search()
            for filename in args.filename:
                if filename.endswith('.txt'):
                    print('Reading from ' + filename)
                    read_titles_from_file(conn, movie_cur, filename, search)
                else:
                    print('Skipping: ' + filename)
        elif args.popular:
            movies = tmdb.Movies()
            add_popular_movies(conn, movie_cur, movies, counter_dictionary)
        elif args.theaters:
            movies = tmdb.Movies()
            add_movies_now_playing(conn, movie_cur, movies, counter_dictionary)
            # discover = tmdb.Discover()
            # add_movies_in_theaters(conn, movie_cur, discover, counter_dictionary)
        elif args.update:
            print('Updating all movies')
            update_all_movies(conn, movie_cur)
        elif args.check:
            for filename in args.check:
                if filename.endswith('.txt'):
                    print('Reading from ' + filename)
                    validate_ids_in_file(conn, movie_cur, filename)
                else:
                    print('Skipping: ' + filename)
        elif args.delete:
            for filename in args.delete:
                if filename.endswith('.txt'):
                    print('Reading from ' + filename)
                    delete_ids_in_file(conn, movie_cur, filename)
                else:
                    print('Skipping: ' + filename)

    finally:
        # Close communication with the database
        print('Closing connection to database')
        movie_cur.close()
        conn.close()


# strictly for testing purposes
def test_main():

    genres = [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}, {"id": 16, "name": "Animation"}, {"id": 35, "name": "Comedy"}, {"id": 80, "name": "Crime"}, {"id": 99, "name": "Documentary"}, {"id": 18, "name": "Drama"}, {"id": 10751, "name": "Family"}, {"id": 14, "name": "Fantasy"}, {"id": 36, "name": "History"}, {
        "id": 27, "name": "Horror"}, {"id": 10402, "name": "Music"}, {"id": 9648, "name": "Mystery"}, {"id": 10749, "name": "Romance"}, {"id": 878, "name": "Science Fiction"}, {"id": 10770, "name": "TV Movie"}, {"id": 53, "name": "Thriller"}, {"id": 10752, "name": "War"}, {"id": 37, "name": "Western"}]

    # movies = [{'vote_count': 1337, 'id': 353081, 'video': False, 'vote_average': 7.4, 'title': 'Mission: Impossible - Fallout', 'popularity': 121.388, 'poster_path': '/AkJQpZp9WoNdj7pLYSj1L0RcMMN.jpg', 'original_language': 'en', 'original_title': 'Mission: Impossible - Fallout', 'genre_ids': [12, 28, 53], 'backdrop_path': '/5qxePyMYDisLe8rJiBYX8HKEyv2.jpg', 'adult': False,
    #           'overview': 'When an IMF mission ends badly, the world is faced \
    #           with dire consequences. As Ethan Hunt takes it upon himself to \
    #           fulfil his original briefing, the CIA begin to question his \
    #           loyalty and his motives. The IMF team find themselves in a race \
    #           against time, hunted by assassins while trying to prevent a \
    #           global catastrophe.', "release_date":"2016-06-16" }]

   #  movies = [{"vote_count": 12, "id": 497698, "video": False,
   #  "vote_average": 8.8, "title": "Black Widow", "popularity": 3.049,
   #  "poster_path": "\/b6MisbmskF6m63YPmYP85YW0WRS.jpg", "original_language":
   #  "en", "original_title": "Black Widow", "genre_ids": [28, 878, 12], "backdrop_path":"", "adult":False, "overview":"First standalone movie of Black Widow in the Marvel Cinematic Universe.", "release_date":""}]

    # movies = [ {"adult":"False","backdrop_path":"/VuukZLgaCrho2Ar8Scl9HtV3yD.jpg","belongs_to_collection":"null","budget":116000000,"genres":[{"id":878,"name":"Science Fiction"}],"homepage":"http://www.venom.movie/site/","id":335983,"imdb_id":"tt1270797","original_language":"en","original_title":"Venom","overview":"When Eddie Brock acquires the powers of a symbiote, he will have to release his alter-ego \"Venom\" to save his life.","popularity":318.021,"poster_path":"/2uNW4WbgBXL25BAbXGLnLqX71Sw.jpg","production_companies":[{"id":5,"logo_path":"/71BqEFAF4V3qjjMPCpLuyJFB9A.png","name":"Columbia Pictures","origin_country":"US"},{"id":7505,"logo_path":"/837VMM4wOkODc1idNxGT0KQJlej.png","name":"Marvel Entertainment","origin_country":"US"},{"id":34,"logo_path":"/GagSvqWlyPdkFHMfQ3pNq6ix9P.png","name":"Sony Pictures","origin_country":"US"},{"id":31828,"logo_path":"null","name":"Avi Arad Productions","origin_country":"US"}],"production_countries":[{"iso_3166_1":"US","name":"United States of America"}],"release_date":"2018-10-03","revenue":508400000,"runtime":112,"spoken_languages":[{"iso_639_1":"en","name":"English"}],"status":"Released","tagline":"The world has enough Superheroes.","title":"Venom","vote_average":6.6,"vote_count":2041}]
    # create_table(conn)
    # Open a cursor to perform database operations
    # genre_cur = conn.cursor()
    # for genre in genres:
    #    handle_genre(conn, genre_cur, genre)

    # genre_cur.close()
    #for movie in movies:
    #    if handle_movie(conn, movie_cur, movie):
            # Make the changes to the database persistent
    #        conn.commit()
    try:
        # Open a connection to the database
        conn = database()
        # Open a cursor to perform database operations
        cursor = conn.cursor()
        update_all_movies(conn, cursor)

    finally:
        print('Closing connection to database')
        # Close communication with the database
        cursor.close()
        conn.close()

if __name__ == '__main__':
    # use test_main to create tables and genres
    # test_main()
    main()
