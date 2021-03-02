import os

import psycopg2 as psycopg2
from psycopg2.extras import DictCursor

from models.movie import Movie


class PostgresLoader:
    def __init__(self, conn: psycopg2.connect):
        self.conn = conn

    def get_all_data(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT "movie_movie"."created",
                   "movie_movie"."id",
                   "movie_movie"."title",
                   "movie_movie"."description",
                   "movie_movie"."create_date",
                   "movie_movie"."age_qualification",
                   "movie_movie"."rating",
                   "movie_movie"."file",
                   ARRAY_AGG(DISTINCT "movie_category"."title" ) AS "type",
                   ARRAY_AGG(DISTINCT "movie_genre"."title" ) AS "genres",
                   ARRAY_AGG(DISTINCT "movie_person"."full_name" ) FILTER (WHERE "movie_personmovie"."role" = 'actor') AS "actors",
                   ARRAY_AGG(DISTINCT "movie_person"."full_name" ) FILTER (WHERE "movie_personmovie"."role" = 'writer') AS "writers",
                   ARRAY_AGG(DISTINCT "movie_person"."full_name" ) FILTER (WHERE "movie_personmovie"."role" = 'director') AS "directors"
                   FROM "movie_movie"
                   LEFT OUTER JOIN "movie_category"
                    ON ("movie_movie"."category_id" = "movie_category"."id")
                   LEFT OUTER JOIN "movie_movie_genres"
                    ON ("movie_movie"."id" = "movie_movie_genres"."movie_id")
                   LEFT OUTER JOIN "movie_genre"
                    ON ("movie_movie_genres"."genre_id" = "movie_genre"."id")
                   LEFT OUTER JOIN "movie_personmovie"
                    ON ("movie_movie"."id" = "movie_personmovie"."movie_id")
                   LEFT OUTER JOIN "movie_person"
                    ON ("movie_personmovie"."person_id" = "movie_person"."id")
                   GROUP BY "movie_movie"."id"
            """
        )
        movies = cursor.fetchall()
        cursor.close()
        return movies


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('POSTGRES_DB_NAME'), 'user': os.environ.get('POSTGRES_USER'),
           'password': os.environ.get('POSTGRES_PASSWORD'), 'host': os.environ.get('POSTGRES_HOST'),
           'port': os.environ.get('POSTGRES_PORT')}
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        postgres = PostgresLoader(conn=pg_conn)
        for movie in postgres.get_all_data():
            print(movie)
