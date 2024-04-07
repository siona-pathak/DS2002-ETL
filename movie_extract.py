import pandas as pd
import ast
import config
import psycopg2
import psycopg2.extras
import requests, json
import locale

# https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/data

# Files are kept locally in movie/ folder
MOVIES_DB = "./the_movies_db/"
print("Movie DB is in - ", MOVIES_DB)
COUNTER = 5000
PROGRESS = 500

DB_CONNECTION = psycopg2.connect(host=config.DB_HOST, port=config.DB_PORT, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
DB_CONNECTION.autocommit = True
#DB_CURSOR = DB_CONNECTION.cursor(cursor_factory=psycopg2.extras.DictCursor)
#print(type(DB_CONNECTION))

locale.setlocale(locale.LC_ALL, '')

MOVIES_DF = pd.read_csv(MOVIES_DB+"movies_metadata.csv")
#MOVIES_DF.info()

CREDITS_DF = pd.read_csv(MOVIES_DB+"credits.csv")
#CREDITS_DF.info()

USER_DF = pd.read_csv(MOVIES_DB+"ratings_small.csv")
#USER_DF.info()

# Data validation
MOVIES_DF.fillna('NULL')
MOVIES_DF = MOVIES_DF.drop_duplicates(subset='id')

# Load into 'movie' table. Return count of movies uploaded
# NOTE: We are only loading a subset of the total MovieLens data set (uptil COUNTER)
def load_movie(movies_df: pd.DataFrame, db_connection: psycopg2.extensions.connection, m_count:int = 5000) -> int:
    print("load_movies>")
    i = 0
    movie_tbl_l = []
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for index, rows in movies_df.iterrows():
        if (i >= m_count):
            break

        # Data Validation
        # Release date fields are empty for many movies. Need to make it null
        if pd.isna(rows['release_date']):
            rows['release_date'] = None
        
        #print(rows['genres'])
        # The JSON is embedded inside a string. This JSON is a list of dictionaries.
        # Need to use 'ast' library to extract a list from a String
        # https://stackoverflow.com/questions/49890561/extracting-list-from-string
        #print(type(rows['genres']), type(ast.literal_eval(rows['genres'])))
        genres_l = [mv_dt['name'] for mv_dt in ast.literal_eval(rows['genres'])]
        #print(genres_l)

        prod_cos_l = [mv_dt['name'] for mv_dt in ast.literal_eval(rows['production_companies'])]
        #print(prod_cos_l)

        #print(i, rows['id'], rows['title'], rows['imdb_id'], rows['release_date'])
        movie_tbl_l.append((rows['id'], rows['title'], rows['imdb_id'], rows['release_date'], genres_l, prod_cos_l))
        #print(movie_tbl_l[i])

        # try:
        #     db_cursor.execute("INSERT INTO movie VALUES(%s, %s, %s, %s, %s, %s)", 
        #                                   movie_tbl_l[i])
        # except Exception as e:
        #     print("ERROR: ", e)

        i = i+1
        # Indicate progress
        if i % PROGRESS == 0:
            print(i, " ...")
    
    # Bulk insert is efficient
    try:
        psycopg2.extras.execute_batch(db_cursor, "INSERT INTO movie VALUES(%s, %s, %s, %s, %s, %s)", movie_tbl_l)
    except Exception as e:
        print("ERROR: ", e)
    return len(movie_tbl_l)

# Load into 'movie_stats' table. Return count of movies uploaded
# NOTE: We are only loading a subset of the total MovieLens data set (uptil COUNTER)
def load_movie_stats(movies_df: pd.DataFrame, db_connection: psycopg2.extensions.connection, m_count:int = 5000) -> int:
    print("load_movie_stats>")
    i = 0
    movie_tbl_l = []
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for index, rows in movies_df.iterrows():
        if (i >= m_count):
            break

        # Get revenue from Open Movie Database (OMDb) API.
        # The OMDb API is a RESTful web service to obtain movie information, all content 
        # and images on the site are contributed and maintained by our users.
        # NOTE: The free version of OMDB API allows only 1000 calls per day.
        revenue = 0
        try:
            omdb_url = config.OMDB_API_URL + "&" + "t=" + rows['title']
            #print(omdb_url)
            movie_response = requests.get(omdb_url)
            movie_json = movie_response.json()
            #print(movie_json)

            # Upon inspection of the JSON, it is a string representation - ex: $23,920,048
            # This needs to be converted to a numeric type.
            # https://stackoverflow.com/questions/3887469/python-how-to-convert-currency-to-decimal
            

            if movie_json['BoxOffice'] != "N/A":
                revenue = locale.atof(movie_json['BoxOffice'][1:])
        except Exception as e:
            print("ERROR while fetching data from OMDB API: " + e)

        # Data Validation
        # Runtime field is empty for many movies. Need to make it 0
        if pd.isna(rows['runtime']):
            rows['runtime'] = 0
        
        #print(i, rows['id'], rows['title'], rows['runtime'], rows['budget'], revenue)
        movie_tbl_l.append((rows['id'], rows['runtime'], rows['budget'], revenue))
       
        # try:
        #     db_cursor.execute("INSERT INTO movie_stats VALUES(%s, %s, %s, %s)", 
        #                                   movie_tbl_l[i])
        # except Exception as e:
        #     print("ERROR: ", e)

        i=i+1
        # Indicate progress
        if i % PROGRESS == 0:
            print(i, " ...")

    # Bulk insert is efficient
    try:
        psycopg2.extras.execute_batch(db_cursor, "INSERT INTO movie_stats VALUES(%s, %s, %s, %s)", movie_tbl_l)
    except Exception as e:
        print("ERROR: ", e)
    return len(movie_tbl_l)

# Load crew and cast subset into 'movie_credit' table. Return count of movies uploaded
# NOTE: We are loading a subset of actors - 5 max. Only 1 director and screenplay.
# NOTE: We are only loading a subset of the total MovieLens data set
def load_movie_credits(credits_df: pd.DataFrame, db_connection: psycopg2.extensions.connection, m_count:int=5000) -> int:
    print("load_movie_credits>")
    i = 0
    credit_tbl_l = []
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for index, rows in credits_df.iterrows():
        if (i >= m_count):
            break

        # The JSON is embedded inside a string. This JSON is a list of dictionaries.
        # Need to use 'ast' library to extract a list from a String
        # https://stackoverflow.com/questions/49890561/extracting-list-from-string
        prod_l = [cr_d['name'] for cr_d in ast.literal_eval(rows['crew']) if cr_d['job'] == "Producer"]
        #print(prod_l)

        # Get 5 actors at max
        actors_l = [cr_d['name'] for cr_d in ast.literal_eval(rows['cast']) if cr_d['order'] <= 5]
        #print(actors_l)

        director_l = [cr_d['name'] for cr_d in ast.literal_eval(rows['crew']) if cr_d['job'] == "Director"]
        director = "" if len(director_l) == 0 else director_l[0]

        editor_l = [cr_d['name'] for cr_d in ast.literal_eval(rows['crew']) if cr_d['job'] == "Editor"]
        editor = "" if len(editor_l) == 0 else editor_l[0]

        screen_l = [cr_d['name'] for cr_d in ast.literal_eval(rows['crew']) if cr_d['job'] == "Screenplay"]
        screenplay = "" if len(screen_l) == 0 else screen_l[0]

        # Store producers, upto 5 actors, 1 director, 1 editor, 1 screenplay writer
        #print(i, rows['id'], prod_l, director, actors_l, editor, screenplay)
        credit_tbl_l.append((rows['id'], prod_l, director, actors_l, editor, screenplay))
        # try:
        #     db_cursor.execute("INSERT INTO movie_credits VALUES(%s, %s, %s, %s, %s, %s)", 
        #                                   credit_tbl_l[i])
        # except Exception as e:
        #     print("ERROR: ", e)

        i=i+1
        # Indicate progress
        if i % PROGRESS == 0:
            print(i, " ...")
    
    # Bulk insert is efficient
    try:
        psycopg2.extras.execute_batch(db_cursor, "INSERT INTO movie_credits VALUES(%s, %s, %s, %s, %s, %s)", credit_tbl_l)
    except Exception as e:
        print("ERROR: ", e)
    return len(credit_tbl_l)

# Load review statistics into 'movie_reviews' table. Return count of movies uploaded
# NOTE: We are only loading a subset of the total MovieLens data set
def load_movie_reviews(movies_df: pd.DataFrame, db_connection: psycopg2.extensions.connection, m_count:int = 5000) -> int:
    print("load_movie_reviews>")
    i = 0
    movie_tbl_l = []
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for index, rows in movies_df.iterrows():

        if (i >= m_count):
            break

        #print(i, rows['id'], rows['vote_average'], rows['vote_count'])
        movie_tbl_l.append((rows['id'], rows['vote_count'], rows['vote_average']))
       
        # try:
        #     db_cursor.execute("INSERT INTO movie_reviews VALUES(%s, %s, %s)", 
        #                                   movie_tbl_l[i])
        # except Exception as e:
        #     print("ERROR: ", e)

        i=i+1
        # Indicate progress
        if i % PROGRESS == 0:
            print(i, " ...")

    # Bulk insert is efficient
    try:
        psycopg2.extras.execute_batch(db_cursor, "INSERT INTO movie_reviews VALUES(%s, %s, %s)", movie_tbl_l)
    except Exception as e:
        print("ERROR: ", e)
    return len(movie_tbl_l)

# Load user_reviews into 'user_ratings' table. Return count of movies found and ratings uploaded
# NOTE: We are only loading a subset of the total MovieLens data set. This means:
# Load users reviews of only the movies subset
def load_user_ratings(movies_df: pd.DataFrame, user_df: pd.DataFrame, db_connection: psycopg2.extensions.connection, 
                      m_count:int = 5000, u_count: int = 50000) -> int:
    print("load_user_ratings>")
    i = 0
    movie_count = 0
    rating_count = 0
    rate_tbl_l = []
    test_l = []
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for index, rows in movies_df.iterrows():
        if (i >= m_count):
            break

        rate_df = user_df[(user_df['movieId'] == rows['id'])]
        if rate_df.shape[0] > 0:
            #print(i, rows['id'], rows['title'], rate_df.shape[0])
            movie_count+=1
            rating_count+= rate_df.shape[0]

            #print(rate_df[['movieId', 'userId', 'rating']])
            # for r_index, r_rows in rate_df.iterrows():
            #     rate_tbl_l.append((r_rows['movieId'], r_rows['userId'], r_rows['rating']))
            test_l = (rate_df[['movieId', 'userId', 'rating']].values.tolist())
            # tolist() returns a list of lists. We need to flatten it.
            for t in test_l:
                rate_tbl_l.append(t)
            
        i+=1
        # Indicate progress
        if i % PROGRESS == 0:
            print(i, " ...")

    # Bulk insert is efficient
    try:
        psycopg2.extras.execute_batch(db_cursor, "INSERT INTO user_ratings VALUES(%s, %s, %s)", rate_tbl_l)
    except Exception as e:
        print("ERROR: ", e)
    return (movie_count, rating_count)

# For each movie loaded in master movie database, find user reviews from ratings table. 
# Create 'bucket' summarization of ratings, ranging from 0 to 5.
# Save sum of rating across these ratings per movie ID
def create_rating_summaries(db_connection: psycopg2.extensions.connection):
    print("create_rating_summaries>")
    i = 0
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        db_cursor.execute("SELECT DISTINCT(m.id), m.title FROM movie m, user_ratings r WHERE r.id = m.id")
        movies = db_cursor.fetchall()
        summary_tbl_l = []
        for m in movies:
            #print(m['id'], m['title'])

            rating_summary_l = []
            rating_summary_l.append(m['id'])
            for r in range(0, 5):
                sql_str = f'''SELECT COUNT(rating) FROM user_ratings WHERE id = {m['id']}'''
                sql_str += f''' AND rating > {r} AND rating <= + {r+1}'''
                db_cursor.execute(sql_str)
                rating_count = db_cursor.fetchone()
                #print("\t", r, "-", r+1, rating_count[0])
                rating_summary_l.append(rating_count[0])
            summary_tbl_l.append(rating_summary_l)
            #print("\t", rating_summary_l)
        
            i+=1
            # Indicate progress. This is more intensive so do shorter updates
            if i % (PROGRESS/2) == 0:
                print(i, " ...")

        psycopg2.extras.execute_batch(db_cursor, "INSERT INTO ratings_summary VALUES(%s, %s, %s, %s, %s, %s)", summary_tbl_l)
    
    except Exception as e:
            print("ERROR: ", e)


movies_count = load_movie(MOVIES_DF, DB_CONNECTION, 5000)
movies_stats_count = load_movie_stats(MOVIES_DF, DB_CONNECTION, 10) 
load_movie_credits(CREDITS_DF, DB_CONNECTION, 5000)
load_movie_reviews(MOVIES_DF, DB_CONNECTION, 5000)
movie_cnt, rate_cnt = load_user_ratings(MOVIES_DF, USER_DF, DB_CONNECTION, 5000, 100000)
print(f"Loaded {movie_cnt} movies and {rate_cnt} ratings")
create_rating_summaries(DB_CONNECTION)