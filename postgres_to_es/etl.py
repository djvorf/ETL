import os
import json
import logging
from pprint import pprint

import requests
import psycopg2

from psycopg2.extras import DictCursor
from urllib.parse import urljoin

from models import Movie
from postgres_to_es.decos import coroutine, retry

logger = logging.getLogger()


@retry(exception_to_check=Exception)
@coroutine
def get_updated_data(dsl: dict, target):
    """
    Получение данных из постгрехи
    :param target: generator
    :return:
    """
    with psycopg2.connect(**dsl) as pg_conn, pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
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
                created=item.get('created').strftime("%m-%d-%Y:%H:%M:%S"),
                modified=item.get('modified').strftime("%m-%d-%Y:%H:%M:%S"),
                id=item.get('id'), title=item.get('title'), description=item.get('description'),
                create_date=item.get('create_date').strftime("%m-%d-%Y"),
                age_qualification=item.get('age_qualification'), rating=item.get('rating'), file=item.get('file'),
                category=item.get('type')[0], genres=item.get('genres'),
                actors=item.get('actors'), writers=item.get('writers'), directors=item.get('directors')).dict())
        target.send(records)


@retry(exception_to_check=Exception)
def load_data(rows: list, index_name: str):
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


@coroutine
def load_to_es(index_name: str):
    """
    Отправка запроса в ES и разбор ошибок сохранения данных
    """
    while True:
        rows = (yield)
        load_data(rows=rows, index_name=index_name)


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
