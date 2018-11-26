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

def set_blank_movie_key(movie, key):
    if key == 'release_date':
        return "2025-01-01 00:00:01"
    elif key == 'poster_path':
        if movie['backdrop_path'] is not None or movie['backdrop_path'] != "":
            return movie['backdrop_path']
    return ""

def create_movie_production_company(cur, movie_id, company_id):
    dt = datetime.now()
    sql = "INSERT INTO movie_production_companies(movie_id, company_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
    cur.execute(sql, (movie_id, company_id, dt, dt))

def create_company(cur, company):
    dt = datetime.now()
    sql = "INSERT INTO companies(id, name, logo_path, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"
    cur.execute(sql, (company['id'], company['name'], company['logo_path'], dt, dt))

def company_exists(cur, company_id):
    cur.execute("SELECT true FROM companies WHERE id = %(id)s", {"id": company_id})
    row = cur.fetchone()
    if row is None:
        return False
    return True

def handle_companies(cur, movie_id, movie_companies):
    for company in movie_companies:
        if not company_exists(cur, company['id']):
            create_company(cur, company)
        create_movie_production_company(cur, movie_id, company['id'])

def translate_status(status_string):
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

def update_movie_info(cur, movie):
    sql = "UPDATE movies SET vote_count = %s, vote_average = %s, budget = %s,\
    runtime = %s, revenue = %s, popularity = %s, status = %s, tagline = %s,\
    adult = %s, release_date = %s, updated_at = %s WHERE id = %s"
    dt = datetime.now()
    updated_rows = 0
    try:
        movie_status = translate_status(movie['status'])
        if movie_status == -1:
            print('A valid status was not found')
        # execute the UPDATE  statement
        cur.execute(sql, (movie['vote_count'], movie['vote_average'],
        movie['budget'], movie['runtime'], movie['revenue'], movie['popularity'], movie_status,
        movie['tagline'], movie['adult'], movie['release_date'], dt, movie['id']))
        handle_companies(cur, movie['id'], movie['production_companies'])
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        # conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return updated_rows == 1

def update_movie(cur, movie):
    # TODO (Sunjay) update status too for where release date is less than today
    """ update vote_count, vote_average, popularity based on the movie id """
    sql = "UPDATE movies SET vote_count = %s, vote_average = %s, popularity = %s, release_date = %s, updated_at = %s WHERE id = %s"
    dt = datetime.now()
    updated_rows = 0
    try:
        # execute the UPDATE  statement
        cur.execute(sql, (movie['vote_count'], movie['vote_average'], movie['popularity'], movie['release_date'], dt, movie['id']))
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        # conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return updated_rows == 1

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
                    0, dt, dt))
    return handle_genres(cur, movie['id'], movie['genre_ids'])

def handle_movie(conn, cur, movie):
    try:
        flag, key = check_movie_fields_exist(movie)
        if not flag:
            movie[key] = set_blank_movie_key(movie, key)
        if not movie_exists(cur, movie['id']):
            # insert new movie title and additional info
            return insert_movie(cur, movie)
        else:
            # update movie fields: vote_count, vote_average, popularity
            # return update_movie(cur, movie)
            return update_movie_info(cur, movie)

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
        release_date date \
    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS categorizations( \
        id serial PRIMARY KEY, \
        movie_id INTEGER REFERENCES movies(id), \
        genre_id INTEGER REFERENCES genres(id) \
    )""")
    cur.close()

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
                    if handle_movie(conn, movie_cur, movie):
                        inserted_movies = inserted_movies + 1
                        # Make the changes to the database persistent
                        conn.commit()
                    else:
                        movies_with_errors.append(line)
            else:
                movies_not_found.append(line)
            if counter % 36 == 0:
                print('Counter ' + str(counter) + ' Sleeping for 10 seconds.')
                # Wait for 10 seconds to avoid rate limiting
                time.sleep(10)

    print('Movies not found:')
    for title in movies_not_found:
        print(title)
    print('Movies with errors:')
    for title in movies_with_errors:
        print(title)
    print(str(counter) + ' searched.')
    print(str(inserted_movies) + ' movies inserted or updated.')

def get_movies_and_info(conn, movie_cur, movies):
    movies_with_errors = []
    count = 0
    movie_ids = [335983]
    for movie_id in movie_ids:
        count = count + 1
        # TODO - this does not work...how come?
        dict_of_movies = movies.info(**{'id': movie_id})
        for movie in dict_of_movies['results']:
            if handle_movie(conn, movie_cur, movie):
                # Make the changes to the database persistent
                conn.commit()
            else:
                 movies_with_errors.append(movie['title'] + '\n')
        if count % 36 == 0:
            print('Movie Count: ' + str(count) + ' Sleeping for 10 seconds.')
            # Wait for 10 seconds to avoid rate limiting
            time.sleep(10)
    print('Movies with errors: \n')
    for title in movies_with_errors:
        print(title)

def add_popular_movies(conn, movie_cur, movies):
    movies_with_errors = []

    for page_number in range(1, 990):
        # dict_of_movies = discover.movie(**{'page': page_number, 'release_date.gte': '2018-08-01', 'release_date.lte': '2018-10-30'})
        # dict_of_movies = discover.movie(**{'page': page_number})
        dict_of_movies = movies.popular(**{'page':  page_number})
        for movie in dict_of_movies['results']:
            if handle_movie(conn, movie_cur, movie):
                # Make the changes to the database persistent
                conn.commit()
            else:
                 movies_with_errors.append(movie['title'] + '\n')
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
        "-u", "--update", help="update movie info and movie companies", action="store_true")
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
            else:
                print('Skipping: ' + filename)
    elif args.popular:
        movies = tmdb.Movies()
        # discover = tmdb.Discover()
        add_popular_movies(conn, movie_cur, movies)
    elif args.update:
        print('update movies')

    # Close communication with the database
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
    # Open a connection to the database
    conn = database()
    # Open a cursor to perform database operations
    movie_cur = conn.cursor()
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
    tmdb.API_KEY = config.API_KEY
    movies = tmdb.Movies()
    get_movies_and_info(conn, movie_cur, movies)
    # Close communication with the database
    movie_cur.close()
    conn.close()

if __name__ == '__main__':
    # use test_main to create tables and genres
    test_main()
    # main()
