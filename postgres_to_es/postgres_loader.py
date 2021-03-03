import os
import json
import logging
import requests

import psycopg2 as psycopg2

from psycopg2.extras import DictCursor
from typing import List
from urllib.parse import urljoin

from models.movie import Movie

logger = logging.getLogger()


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        '''
        Подготавливает bulk-запрос в Elasticsearch
        '''
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str):
        '''
        Отправка запроса в ES и разбор ошибок сохранения данных
        '''
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)


class ETL:
    def __init__(self, conn: psycopg2.connect):
        self.conn = conn
        self.es_loader = ESLoader(url="http://localhost:9200/")

    def get_all_data(self) -> List[dict]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
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
                GROUP BY "movie_movie"."id"
                """
            )
            movies = cursor.fetchall()
        records = []
        for movie in movies:
            records.append(Movie(
                created=movie[0].strftime("%m-%d-%Y:%H:%M:%S"),
                modified=movie[1].strftime("%m-%d-%Y:%H:%M:%S"),
                id=movie[2],
                title=movie[3],
                description=movie[4],
                create_date=movie[5].strftime("%m-%d-%Y"),
                age_qualification=movie[6],
                rating=movie[7],
                file=movie[8],
                category=movie[9][0],
                genres=movie[10],
                actors=movie[11],
                writers=movie[12],
                directors=movie[13],
            ).dict())
        return records

    def load(self):
        records = self.get_all_data()
        self.es_loader.load_to_es(records=records, index_name='movies')


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('POSTGRES_DB_NAME'), 'user': os.environ.get('POSTGRES_USER'),
           'password': os.environ.get('POSTGRES_PASSWORD'), 'host': os.environ.get('POSTGRES_HOST'),
           'port': os.environ.get('POSTGRES_PORT')}
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        postgres = ETL(conn=pg_conn)
        postgres.load()
