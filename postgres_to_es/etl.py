import os
import json
import logging

import requests
import psycopg2 as psycopg2

from psycopg2.extras import DictCursor
from urllib.parse import urljoin

from models import Movie
from postgres_to_es.decos import coroutine, retry

logger = logging.getLogger()


@retry(exception_to_check=Exception)
@coroutine
def get_updated_data(dsl: dict, target):
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn, pg_conn.cursor() as cursor:
        while True:
            table_name = (yield)
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
            values = cursor.fetchall()
            target.send(values)


@coroutine
def transform_data(target):
    """
    Приводит данные к модели
    :param target: generator
    """
    while True:
        values = (yield)
        records = []
        for item in values:
            records.append(Movie(
                created=item[0].strftime("%m-%d-%Y:%H:%M:%S"), modified=item[1].strftime("%m-%d-%Y:%H:%M:%S"),
                id=item[2], title=item[3], description=item[4], create_date=item[5].strftime("%m-%d-%Y"),
                age_qualification=item[6], rating=item[7], file=item[8], category=item[9][0], genres=item[10],
                actors=item[11], writers=item[12], directors=item[13]).dict())
        target.send(records)


@retry(exception_to_check=Exception)
def check_elastic():
    requests.get('http://localhost:9201/')


@coroutine
def load_to_es(index_name: str):
    """
    Отправка запроса в ES и разбор ошибок сохранения данных
    """
    while True:
        check_elastic()
        url = 'http://localhost:9201/'
        rows = (yield)
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
    load_to_es = load_to_es(index_name='movies')
    transform_data_movies = transform_data(target=load_to_es)
    movies = get_updated_data(dsl=dsl, target=transform_data_movies)
    movies.send('movie_movie')
    movies.send('movie_person')
    movies.send('movie_genre')
