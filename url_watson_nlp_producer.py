import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os
import logging


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database %s', config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    logging.info('Connected to AWS in %s', aws_credential['region_name'])

    # Connect to queue for Twitter credentials.
    credentials_queue = sqs.get_queue_by_name(QueueName='watson_credentials')
    credentials_queue.purge()
    # Enqueue all Twitter credentials available on the database.
    cur.execute("""select username, password from watson_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        message_body = json.dumps(credential)
        credentials_queue.send_message(MessageBody=message_body)
    logging.info('Enqueued Watson credentials')

    ids_queue = sqs.get_queue_by_name(QueueName='url_watson_nlp')
    ids_queue.purge()
    logging.info('SQL query')

    cur.execute("""
        select url.project_name, url.url
        from url, project
        where
          url.project_name = project.project_name and
          project.active and
          status_code = 200 and
          headers->>'Content-Type' like '%text/html%' and 
          not exists(
            select *
            from url_watson_nlp
            where
                url_watson_nlp.project_name = url.project_name and 
                url_watson_nlp.url = url.url);
          """)
    no_more_results = False
    while not no_more_results:
        ids = cur.fetchmany(size=100)
        if len(ids) == 0:
            logging.info('No more results')
            no_more_results = True
        else:
            logging.info('Message sent with %d URLs', len(ids))
            ids_queue.send_message(MessageBody=json.dumps(ids))

    conn.close()
    logging.info('Disconnected from database')


if __name__ == '__main__':
    main()
