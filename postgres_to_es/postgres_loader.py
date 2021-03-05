import os
import json
import logging

import requests
import psycopg2 as psycopg2

from typing import List
from functools import wraps

from psycopg2.extras import DictCursor
from urllib.parse import urljoin

from models.movie import Movie

logger = logging.getLogger()


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        return fn

    return inner


@coroutine
def get_updated_data(connection, table_name: str) -> List[dict]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT "movie_movie"."created", "movie_movie"."modified", "movie_movie"."id", 
            "movie_movie"."title", "movie_movie"."description", "movie_movie"."create_date", 
            "movie_movie"."age_qualification", "movie_movie"."rating", "movie_movie"."file",
            ARRAY_AGG(DISTINCT "movie_category"."title" ) AS "type",
            ARRAY_AGG(DISTINCT "movie_genre"."title" ) AS "genres",
            ARRAY_AGG(DISTINCT "movie_person"."full_name" ) 
                FILTER (WHERE "movie_personmovie"."role" = 'actor') AS "actors",
            ARRAY_AGG(DISTINCT "movie_person"."full_name" ) 
                FILTER (WHERE "movie_personmovie"."role" = 'writer') AS "writers",
            ARRAY_AGG(DISTINCT "movie_person"."full_name" ) 
                FILTER (WHERE "movie_personmovie"."role" = 'director') AS "directors"
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
            WHERE "{table_name}"."modified" > CURRENT_DATE
            GROUP BY "movie_movie"."id"
            LIMIT 100 
            """
        )
        movies = cursor.fetchall()
        return movies



@coroutine
def transform_data(date: List[dict]) -> List[dict]:
    records = []
    for movie in date:
        records.append(Movie(
            created=movie[0].strftime("%m-%d-%Y:%H:%M:%S"), modified=movie[1].strftime("%m-%d-%Y:%H:%M:%S"),
            id=movie[2], title=movie[3], description=movie[4], create_date=movie[5].strftime("%m-%d-%Y"),
            age_qualification=movie[6], rating=movie[7], file=movie[8], category=movie[9][0], genres=movie[10],
            actors=movie[11], writers=movie[12], directors=movie[13]).dict())
    return records


@coroutine
def load_to_es(index_name: str, rows: List[dict]):
    """
    Отправка запроса в ES и разбор ошибок сохранения данных
    """
    url = 'http://localhost:9200/'
    prepared_query = []
    for row in rows:
        prepared_query.extend([
            json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
            json.dumps(row)
        ])

    str_query = '\n'.join(prepared_query) + '\n'

    response = requests.post(
        urljoin(url, '_bulk'),
        data=str_query,
        headers={'Content-Type': 'application/x-ndjson'}
    )

    json_response = json.loads(response.content.decode())
    for item in json_response['items']:
        error_message = item['index'].get('error')
        if error_message:
            logger.error(error_message)


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('POSTGRES_DB_NAME'), 'user': os.environ.get('POSTGRES_USER'),
           'password': os.environ.get('POSTGRES_PASSWORD'), 'host': os.environ.get('POSTGRES_HOST'),
           'port': os.environ.get('POSTGRES_PORT')}
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        movies = get_updated_data(connection=pg_conn, table_name='movie_movie')
        persons = get_updated_data(connection=pg_conn, table_name='movie_person')
        genres = get_updated_data(connection=pg_conn, table_name='movie_genre')

        transform_data_movies = transform_data(date=movies)
        transform_data_persons = transform_data(date=persons)
        transform_data_genres = transform_data(date=genres)

        load_to_es(index_name='movies', rows=transform_data_movies)
        load_to_es(index_name='movies', rows=transform_data_persons)
        load_to_es(index_name='movies', rows=transform_data_genres)
