import config
import tmdbsimple as tmdb
import psycopg2
from datetime import datetime
import time
from sqlalchemy.exc import IntegrityError
import argparse


def database():

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
    dt = datetime.now()
    # insert movie id and each genre id into categorization table
    for genre_id in genre_ids:
        cur.execute(
            "INSERT INTO categorizations(movie_id, genre_id, created_at, updated_at) VALUES (%s, %s, %s, %s)", (movie_id, genre_id, dt, dt))
    return True

def check_movie_fields_exist(movie):
    try:
        keySet = ['id', 'release_date', 'title', 'vote_count', 'vote_average', 'popularity',
                'poster_path', 'original_language', 'backdrop_path', 'adult',
                'overview', 'genre_ids']
        for key in keySet:
            if key not in movie:
                if movie['title'] is not None or movie['title'] != "":
                    print(movie['title'] + ' was missing '+ key +  '. not inserted!\n')
                else:
                    print('Movie without a title was missing '+ key + '\n')
                return False, key
        return True, ''

    except:
        if movie['title'] is not None or movie['title'] != "":
            print(movie['title'] + ' cause an error. not inserted!\n')
        else:
            print('Movie without a title caused an error\n')
        return False, ''


def movie_exists(cur, movie_id):
    cur.execute("SELECT true FROM movies WHERE id = %(id)s", {"id": movie_id})
    row = cur.fetchone()
    if row is None:
        return False
    return True

def max_movie_position(cur):
    cur.execute("SELECT max(position) FROM movies")
    row = cur.fetchone()
    return row[0]

def set_blank_movie_key(key):
    if key == 'release_date':
        return "2025-01-01 00:00:01"
    elif key == 'backdrop_path':
        return ""
    elif key == 'poster_path':
        return ""
    elif key == 'overview':
        return ""
    return ""

def handle_movie(conn, cur, movie, position, page_number):
    try:
        dt = datetime.now()
        flag, key = check_movie_fields_exist(movie)
        if not flag:
            movie[key] = set_blank_movie_key(key)
        if not movie_exists(cur, movie['id']):
            cur.execute("INSERT INTO movies(id, vote_count,\
                        vote_average, title, popularity, poster_path,\
                        original_language, backdrop_path, adult, overview,\
                        release_date, position, page_number, created_at, \
                        updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON\
                        CONFLICT (id) DO NOTHING", (movie['id'], movie['vote_count'], movie['vote_average'], movie['title'], movie['popularity'], movie['poster_path'], movie['original_language'], movie['backdrop_path'], movie['adult'], movie['overview'], movie['release_date'], position, page_number, dt, dt))
            return handle_genres(cur, movie['id'], movie['genre_ids'])
        else:
            print(movie['title'] + ' already exists in the database.')
            return False

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
        popularity NUMERIC (6, 3), \
        poster_path text, \
        original_language VARCHAR(2), \
        backdrop_path text, \
        adult BOOLEAN, \
        overview text, \
        release_date date, \
        position intege,r \
        page_number integer \
    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS categorizations( \
        id serial PRIMARY KEY, \
        movie_id INTEGER REFERENCES movies(id), \
        genre_id INTEGER REFERENCES genres(id) \
    )""")
    cur.close()

# For testing


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

    movies = [{"vote_count": 12, "id": 497698, "video": False, "vote_average": 8.8, "title": "Black Widow", "popularity": 3.049, "poster_path": "\/b6MisbmskF6m63YPmYP85YW0WRS.jpg", "original_language": "en",
        "original_title": "Black Widow", "genre_ids": [28, 878, 12], "backdrop_path":"", "adult":False, "overview":"First standalone movie of Black Widow in the Marvel Cinematic Universe.", "release_date":""}]
    conn = database()
    # create_table(conn)
    # Open a cursor to perform database operations
    # genre_cur = conn.cursor()
    # for genre in genres:
    #    handle_genre(conn, genre_cur, genre)

    # genre_cur.close()
    # Open a cursor to perform database operations
    movie_cur = conn.cursor()
    position = 1
    for movie in movies:
        if handle_movie(conn, movie_cur, movie, position, 0):
            position += 1
            # Make the changes to the database persistent
            conn.commit()

    # Close communication with the database
    movie_cur.close()
    conn.close()

def read_titles_from_file(conn, movie_cur, filename, search):

    movies_not_found = []
    movies_with_errors = []
    position = int(max_movie_position(movie_cur)) + 1
    print('Starting position: ' + str(position))
    with open(filename, "r") as ins:
        for line in ins:
            movies = search.movie(**{'query': line.rstrip('\n')})
            if movies['total_results'] > 0:
                for movie in movies['results']:
                    if handle_movie(conn, movie_cur, movie, position, movie['page']):
                        position += 1
                        # Make the changes to the database persistent
                        conn.commit()
                    else:
                        movies_with_errors.append(line)
            else:
                movies_not_found.append(line)
            if position % 36 == 0:
                print('Position ' + str(position) + ' Sleeping for 10 seconds.')
                # Wait for 10 seconds to avoid rate limiting
                time.sleep(10)

    print('Movies not found: \n')
    for title in movies_not_found:
        print(title)
    print('Movies with errors: \n')
    for title in movies_with_errors:
        print(title)

def add_popular_movies(conn, movie_cur, movies):
    movies_with_errors = []

    position = 1
    # TODO (Sunjay) make the position and the page range
    # dynamic
    for page_number in range(1, 990):
        # dict_of_movies = discover.movie(**{'page': page_number, 'release_date.gte': '2018-08-01', 'release_date.lte': '2018-10-30'})
        # dict_of_movies = discover.movie(**{'page': page_number})
        dict_of_movies = movies.popular(**{'page':  page_number})
        for movie in dict_of_movies['results']:
            if handle_movie(conn, movie_cur, movie, position, 0):
                position += 1
                # Make the changes to the database persistent
                conn.commit()
            else:
                 movies_with_errors.append(line)
        if page_number % 36 == 0:
            print('Page Number: ' + str(page_number) + ' Sleeping for 10 seconds.')
            # Wait for 10 seconds to avoid rate limiting
            time.sleep(10)
    print('Movies with errors: \n')
    for title in movies_with_errors:
        print(title)

# query and store all movies from the API
def main():
    parser = argparse.ArgumentParser(
        description='A script for reading title into movies database')
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity", action="store_true")
    parser.add_argument(
        "-db", "--database", help="utilize the database for storing movies", action="store_true")
    parser.add_argument(
        "-p", "--popular", help="insert all popular movies into the database", action="store_true")
    parser.add_argument('-f', "--filename", nargs='*',
                        type=str, help='insert movie titles from file into the database')
    args = parser.parse_args()

    datetime = time.strftime('%Y-%m-%d %H:%M:%S')

    tmdb.API_KEY = config.API_KEY
    # Open a connection to the database
    conn = database()
    # Open a cursor to perform database operations
    movie_cur = conn.cursor()


    if args.filename:
        search = tmdb.Search()
        for filename in args.filename:
            if filename.endswith('.txt'):
                print('Reading from ' + filename)
                read_titles_from_file(conn, movie_cur, filename, search)
    elif args.popular:
        movies = tmdb.Movies()
        # discover = tmdb.Discover()
        add_popular_movies(conn, movie_cur, movies)

        print("Final Position: " + str(position))
        print("Final Page Number: " + str(page_number))

    # Close communication with the database
    movie_cur.close()
    conn.close()


if __name__ == '__main__':
    # use test_main to create tables and genres
    # test_main()
    main()
