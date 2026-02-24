import pandas as pd
from spark.batch.aggregates import aggregate_user_features, aggregate_movie_popularity


def test_aggregate_user_features_top_movies_order():
    df = pd.DataFrame([
        {"user_id": 1, "movie_id": 10, "rating": 5.0},
        {"user_id": 1, "movie_id": 20, "rating": 4.0},
        {"user_id": 1, "movie_id": 30, "rating": 3.0},
    ])
    users = aggregate_user_features(df)
    assert users == [(1, [10, 20, 30])]


def test_aggregate_movie_popularity_counts():
    df = pd.DataFrame([
        {"user_id": 1, "movie_id": 10, "rating": 5.0},
        {"user_id": 2, "movie_id": 10, "rating": 3.0},
        {"user_id": 3, "movie_id": 20, "rating": 4.0},
    ])
    movies = aggregate_movie_popularity(df)
    movies_dict = {m[0]: (m[1], round(m[2], 1)) for m in movies}
    assert movies_dict[10] == (2, 4.0)
    assert movies_dict[20] == (1, 4.0)
