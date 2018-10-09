import config
import tmdbsimple as tmdb
import psycopg2
from datetime import datetime
import time
from sqlalchemy.exc import IntegrityError

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


def handle_genres(cur, movie_id, genre_ids):
    dt = datetime.now()
    # insert movie id and each genre id into categorization table
    for genre_id in genre_ids:
        cur.execute(
            "INSERT INTO categorizations(movie_id, genre_id, created_at, updated_at) VALUES (%s, %s, %s, %s)", (movie_id, genre_id, dt, dt))

def check_movie_field_values(movie):
    try:
        keySet = ['release_date', 'id', 'vote_count', 'vote_average', 'title', 'popularity',
                'poster_path', 'original_language', 'backdrop_path', 'adult',
                'overview', 'genre_ids']
        for key in keySet:
            if key in movie:
                value = movie[key]
                if value is None or value  == "":
                        return False, key

        return True, ""
    except:
        if movie['title'] is not None or movie['title'] != "":
            print(movie['title'] + 'cause an error. Not inserted!\n')
        else:
            print('Some movie without a title caused an error\n')
        return False, None

def check_movie_fields_exist(movie):
    try:
        keySet = ['release_date', 'id', 'vote_count', 'vote_average', 'title', 'popularity',
                'poster_path', 'original_language', 'backdrop_path', 'adult',
                'overview', 'genre_ids']
        value = None
        if all (k in movie for k in keySet):
            return True
        else:
            if movie['title'] is not None or movie['title'] != "":
                print(movie['title'] + ' was missing fields. Not inserted!\n')
            else:
                print('Some movie without a title was missing fields\n')
            return False
    except:
        if movie['title'] is not None or movie['title'] != "":
            print(movie['title'] + 'cause an error. Not inserted!\n')
        else:
            print('Some movie without a title caused an error\n')
        return False
def movie_exists(cur, movie_id):
    cur.execute("SELECT true FROM movies WHERE id = %(id)s", {"id": movie_id})
    row = cur.fetchone()
    if row is None:
        return False
    return True
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
    user_id = 1
    # if None not in movie.values():
    try:
        dt = datetime.now()
        if check_movie_fields_exist(movie):
            flag, key = check_movie_field_values(movie)
            if not flag:
                if key:
                    movie[key] = set_blank_movie_key(key)
                else:
                    return
            if not movie_exists(cur, movie['id']):
                cur.execute("INSERT INTO movies(id, user_id, vote_count,\
                            vote_average, title, popularity, poster_path,\
                            original_language, backdrop_path, adult, overview,\
                            release_date, position, page_number, created_at, updated_at) VALUES (%s, %s, %s,\
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON\
                            CONFLICT (id) DO NOTHING", (movie['id'], user_id,
                            movie['vote_count'], movie['vote_average'],
                            movie['title'], movie['popularity'],
                            movie['poster_path'],
                            movie['original_language'],
                            movie['backdrop_path'], movie['adult'],
                            movie['overview'], movie['release_date'],
                            position, page_number, dt, dt))
                handle_genres(cur, movie['id'], movie['genre_ids'])
            else:
                print(movie['title'] + ' already exists in the database.')

    except psycopg2.DataError:
        print('Data Error in ' + movie['title'] + ' \n')
        # conn.rollback()
    except psycopg2.IntegrityError:
        print('Integrity Error in ' + movie['title'] + ' \n')
        # conn.rollback()

def handle_genre(conn, cur, genre):
    dt = datetime.now()
    cur.execute("INSERT INTO genres(id, name, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                (genre['id'], genre['name'], dt, dt))


def create_table(conn):
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
        user_id integer, \
        vote_count integer, \
        vote_average NUMERIC (2, 1), \
        title text NOT NULL UNIQUE, \
        popularity NUMERIC (6, 3), \
        poster_path text, \
        original_language VARCHAR(2), \
        backdrop_path text, \
        adult BOOLEAN, \
        overview text, \
        release_date date \
        position integer \
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

    movies = [{"vote_count":12,"id":497698,"video":False,"vote_average":8.8,"title":"Black Widow","popularity":3.049,"poster_path":"\/b6MisbmskF6m63YPmYP85YW0WRS.jpg","original_language":"en","original_title":"Black Widow","genre_ids":[28,878,12],"backdrop_path":"","adult":False,"overview":"First standalone movie of Black Widow in the Marvel Cinematic Universe.","release_date":""}]
    conn = database()
    # create_table(conn)
    # Open a cursor to perform database operations
    # genre_cur = conn.cursor()
    # for genre in genres:
    #    handle_genre(conn, genre_cur, genre)

    #genre_cur.close()
    # Open a cursor to perform database operations
    movie_cur = conn.cursor()
    position = 1
    for movie in movies:
        handle_movie(conn, movie_cur, movie, position, 0)
        position += 1
        # Make the changes to the database persistent
        conn.commit()

    # Close communication with the database
    movie_cur.close()
    conn.close()


# query and store all movies from the API
def main():
    tmdb.API_KEY = config.API_KEY
    conn = database()
    # create_table(conn)
    # Open a cursor to perform database operations
    genre_cur = conn.cursor()
    genres = [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}, {"id": 16, "name": "Animation"}, {"id": 35, "name": "Comedy"}, {"id": 80, "name": "Crime"}, {"id": 99, "name": "Documentary"}, {"id": 18, "name": "Drama"}, {"id": 10751, "name": "Family"}, {"id": 14, "name": "Fantasy"}, {"id": 36, "name": "History"}, {
        "id": 27, "name": "Horror"}, {"id": 10402, "name": "Music"}, {"id": 9648, "name": "Mystery"}, {"id": 10749, "name": "Romance"}, {"id": 878, "name": "Science Fiction"}, {"id": 10770, "name": "TV Movie"}, {"id": 53, "name": "Thriller"}, {"id": 10752, "name": "War"}, {"id": 37, "name": "Western"}]

    # for genre in genres:
     #   handle_genre(conn, genre_cur, genre)

    # genre_cur.close()
    # Open a cursor to perform database operations
    movie_cur = conn.cursor()

    movies = tmdb.Movies()
    # discover = tmdb.Discover()
    position = 1
    # TODO (Sunjay) make the page rage nonstatic
    for page_number in range(1, 990):
        # dict_of_movies = discover.movie(**{'page': page_number, 'release_date.gte': '2018-08-01', 'release_date.lte': '2018-10-30'})
        # dict_of_movies = discover.movie(**{'page': page_number})
        dict_of_movies = movies.popular(**{'page':  page_number})
        for movie in dict_of_movies['results']:
            handle_movie(conn, movie_cur, movie, position, page_number)
            position += 1
            # Make the changes to the database persistent
            conn.commit()
        if page_number % 36 == 0:
            # Make the changes to the database persistent
            # conn.commit()
            print('Page Number: '+ str(page_number) + ' Sleeping for 10 seconds.')
            # Wait for 10 seconds to avoid rate limiting
            time.sleep(10)

    print("Final Position: " + str(position))
    print("Final Page NUmber: " + str(page_number))
    # Close communication with the database
    movie_cur.close()
    conn.close()


if __name__ == '__main__':
    # test_main()
    main()
