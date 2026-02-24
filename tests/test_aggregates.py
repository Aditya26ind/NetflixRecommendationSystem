import pandas as pd

from spark.batch.aggregates import aggregate_movie_popularity, aggregate_user_features


def test_aggregate_user_features_picks_top_five_per_user():
    df = pd.DataFrame(
        [
            {"user_id": 1, "movie_id": m, "rating": r}
            for m, r in [(10, 2.0), (11, 4.5), (12, 5.0), (13, 3.0), (14, 4.0), (15, 4.8)]
        ]
        + [
            {"user_id": 2, "movie_id": m, "rating": r}
            for m, r in [(20, 5.0), (21, 4.9), (22, 4.8), (23, 4.7), (24, 4.6)]
        ]
    )

    results = dict(aggregate_user_features(df))

    assert results[1] == [12, 15, 11, 14, 13]
    assert results[2] == [20, 21, 22, 23, 24]


def test_aggregate_movie_popularity_counts_and_mean():
    df = pd.DataFrame(
        [
            {"user_id": 1, "movie_id": 10, "rating": 4.0},
            {"user_id": 2, "movie_id": 10, "rating": 2.0},
            {"user_id": 1, "movie_id": 11, "rating": 5.0},
            {"user_id": 2, "movie_id": 11, "rating": 3.0},
            {"user_id": 3, "movie_id": 11, "rating": 4.0},
        ]
    )

    popularity = dict(((m, (c, round(avg, 2))) for m, c, avg in aggregate_movie_popularity(df)))

    assert popularity[10] == (2, 3.0)
    assert popularity[11] == (3, 4.0)
