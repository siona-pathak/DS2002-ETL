
DROP TABLE IF EXISTS movie CASCADE;
CREATE TABLE movie (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    imdb_id TEXT NOT NULL,
    release_date DATE,
    genres TEXT[],
    production_companies TEXT[]
);

DROP TABLE IF EXISTS movie_stats CASCADE;
CREATE TABLE movie_stats (
    id INTEGER REFERENCES movie,
    runtime INTEGER,
    budget INTEGER,
    revenue INTEGER
);

DROP TABLE IF EXISTS movie_credits CASCADE;
CREATE TABLE movie_credits (
    id INTEGER REFERENCES movie,
    producers TEXT[],
    director TEXT,
    actors TEXT[],
    editor  TEXT,
    screenplay TEXT
);

DROP TABLE IF EXISTS movie_reviews CASCADE;
CREATE TABLE movie_reviews (
    id INTEGER REFERENCES movie,
    vote_count INTEGER,
    vote_average NUMERIC
);

DROP TABLE IF EXISTS user_ratings CASCADE;
CREATE TABLE user_ratings (
    id INTEGER REFERENCES movie,
    user_id INTEGER NOT NULL,
    rating NUMERIC
);

DROP TABLE IF EXISTS ratings_summary CASCADE;
CREATE TABLE ratings_summary (
    id INTEGER REFERENCES movie,
    range_0_1 INTEGER,
    range_1_2 INTEGER,
    range_2_3 INTEGER,
    range_3_4 INTEGER,
    range_4_5 INTEGER
);

