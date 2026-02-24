CREATE TABLE IF NOT EXISTS movies (
    movie_id   BIGINT PRIMARY KEY,
    title      TEXT NOT NULL,
    genres     TEXT
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id    BIGINT NOT NULL,
    movie_id   BIGINT NOT NULL REFERENCES movies(movie_id),
    rating     NUMERIC(2,1) NOT NULL,
    ts         TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (user_id, movie_id)
);

CREATE TABLE IF NOT EXISTS user_aggregates (
    user_id    BIGINT PRIMARY KEY,
    top_movies BIGINT[] DEFAULT '{}',
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS movie_popularity (
    movie_id   BIGINT PRIMARY KEY,
    event_count BIGINT,
    avg_rating NUMERIC(3,2),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);
