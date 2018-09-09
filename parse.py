import tmdbsimple as tmdb
import psycopg2

def database():

    #Define our connection string
    conn_string = "host='localhost' dbname='test_database' user='test_user' password = 'your_password'"

    # print the connection string we will use to connect
    print("Connecting to database\n	->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    # cursor = conn.cursor()
    print("Connected!\n")
    return conn

def handle_genres(cur, movie_id, genre_ids):

    # insert movie id and each genre id into categorization table
    for genre_id in genre_ids:
        cur.execute("INSERT INTO categorizations(movie_id, genre_id) VALUES (%s, %s)", (movie_id, genre_id))

def handle_movie(conn, cur, movie):
    user_id = 1
    cur.execute("INSERT INTO movies(id, user_id, vote_count, vote_average, title, popularity, poster_path, original_language, backdrop_path, adult, overview, release_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ", (movie['id'], user_id, movie['vote_count'],  movie['vote_average'], movie['title'], movie['popularity'], movie['poster_path'], movie['original_language'], movie['backdrop_path'], movie['adult'], movie['overview'], movie['release_date']))
    handle_genres(cur, movie['id'], movie['genre_ids'])

def handle_genre(conn, cur, genre):
    cur.execute("INSERT INTO genres(id, name) VALUES (%s, %s)", (genre['id'], genre['name']))

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
    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS categorizations( \
        id serial PRIMARY KEY, \
        movie_id INTEGER REFERENCES movies(id), \
        genre_id INTEGER REFERENCES genres(id) \
    )""")
    cur.close()

# For testing
def main1():

    genres = [{"id":28,"name":"Action"},{"id":12,"name":"Adventure"},{"id":16,"name":"Animation"},{"id":35,"name":"Comedy"},{"id":80,"name":"Crime"},{"id":99,"name":"Documentary"},{"id":18,"name":"Drama"},{"id":10751,"name":"Family"},{"id":14,"name":"Fantasy"},{"id":36,"name":"History"},{"id":27,"name":"Horror"},{"id":10402,"name":"Music"},{"id":9648,"name":"Mystery"},{"id":10749,"name":"Romance"},{"id":878,"name":"Science Fiction"},{"id":10770,"name":"TV Movie"},{"id":53,"name":"Thriller"},{"id":10752,"name":"War"},{"id":37,"name":"Western"}]

    movies = [{'vote_count': 1337, 'id': 353081, 'video': False, 'vote_average': 7.4, 'title': 'Mission: Impossible - Fallout', 'popularity': 121.388, 'poster_path': '/AkJQpZp9WoNdj7pLYSj1L0RcMMN.jpg', 'original_language': 'en', 'original_title': 'Mission: Impossible - Fallout', 'genre_ids': [12, 28, 53], 'backdrop_path': '/5qxePyMYDisLe8rJiBYX8HKEyv2.jpg', 'adult': False, 'overview': 'When an IMF mission ends badly, the world is faced with dire consequences. As Ethan Hunt takes it upon himself to fulfil his original briefing, the CIA begin to question his loyalty and his motives. The IMF team find themselves in a race against time, hunted by assassins while trying to prevent a global catastrophe.', 'release_date': '2018-07-25'}]

    conn = database()
    create_table(conn)
    # Open a cursor to perform database operations
    genre_cur = conn.cursor()
    for genre in genres:
        handle_genre(conn, genre_cur, genre)

    genre_cur.close()
    # Open a cursor to perform database operations
    movie_cur = conn.cursor()
    for movie in movies:
        handle_movie(conn, movie_cur, movie)

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    movie_cur.close()
    conn.close()


# query and store all movies from the API
def main():
    tmdb.API_KEY = 'your_api_token'
    movies = tmdb.Movies()
    # TODO (Sunjay) make the page rage nonstatic
    for page_number in range(1, 992):
        dict_of_movies = movies.popular(page_number)
        for movie in dict_of_movies['results']:
            handle_movie(movie)

if __name__ == '__main__':
    main1()
