import os
from time import sleep

import backoff
from urllib3.exceptions import HTTPError
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ElasticsearchException
from psycopg2.extras import DictCursor

from state import *
from queries import *
import psycopg2


BATCH_SIZE = 100


class Extraction:
    def __init__(self, conn, query_db):
        self.cursor = conn.cursor()
        self.query = query_db

    @backoff.on_exception(
        wait_gen=backoff.expo, exception=(psycopg2.Error, psycopg2.OperationalError)
    )
    def extract(self, modified):
        self.cursor.execute(self.query % modified)
        while batch := self.cursor.fetchmany(BATCH_SIZE):
            yield from batch


class Transform:
    def __init__(self, conn, ind, data_l):
        self.data = data_l
        self.index = ind
        self.cursor = conn.cursor()

    def transform(self):
        res = []
        d = self.data
        if self.index == 'movies':
            directors = [{'id': el['person_id'],
                          'name': el['person_name']}
                         for el in d[7] if el['person_role'] == 'director']
            writers = [{'id': el['person_id'],
                        'name': el['person_name']}
                       for el in d[7] if el['person_role'] == 'writer']
            actors = [{'id': el['person_id'],
                       'name': el['person_name']}
                      for el in d[7] if el['person_role'] == 'actor']
            res = {'id': d[0],
                   'title': d[1],
                   'description': d[2],
                   'type': d[4],
                   'creation_date': d[5],
                   'rating': d[3],
                   'modified': d[6],
                   'directors': directors,
                   'writers': writers,
                   'actors': actors}
        elif self.index == 'genres':
            res = {'id': d['genre_id'],
                   'name': d['genre_name'],
                   'description': d['genre_description'],
                   'modified': d['modified']}
        elif self.index == 'persons':
            roles = ', '.join(d[4])
            films = [{'id': el["fw_id"],
                      'rating': el['fw_rating'],
                      'title': el['fw_title'],
                      'type': 'fw_type'}
                     for el in d[3]]
            res = {'id': d[0],
                   'name': d[1],
                   'modified': d[2],
                   'roles': roles,
                   'films': films}

        return res


class Load:
    def __init__(self, conn, ind, data_from_t):
        self.data = data_from_t
        self.index = ind
        self.cursor = conn.cursor()
        self.es = Elasticsearch(os.getenv('ES_URL', 'http://127.0.0.1:9200'))

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(ElasticsearchException, HTTPError),
        max_tries=10,
    )
    def load_data(self):
        self.es.index(index=self.index, id=self.data['id'], body=self.data, doc_type='_doc')


if __name__ == '__main__':
    load_dotenv()
    state = State(JsonFileStorage('state.json'))
    dsl = {
        'dbname': os.getenv('DB_NAME', 'movies_database'),
        'user': os.getenv('DB_USER', 'app'),
        'password': os.getenv('DB_PASSWORD', '123qwe'),
        'host': os.getenv('DB_HOST', "127.0.0.1"),
        'port': os.getenv('DB_PORT', 5432),
    }
    queries = {'movies': FW_QUERY, 'genres': GENRE_QUERY, 'persons': PERSON_QUERY}
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        while True:
            for index, query in queries.items():
                e = Extraction(pg_conn, query)
                state_key = f"{index}_modified"
                last_modified = state.get_state(state_key)
                for data in e.extract(last_modified):
                    data_t = Transform(pg_conn, index, data).transform()
                    Load(pg_conn, index, data_t).load_data()
                    state.set_state(state_key, data_t["modified"].isoformat())
            sleep(1)
