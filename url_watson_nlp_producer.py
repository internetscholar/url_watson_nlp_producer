import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')

    # Connect to queue for Twitter credentials.
    credentials_queue = sqs.get_queue_by_name(QueueName='watson-credentials')
    credentials_queue.purge()
    # Enqueue all Twitter credentials available on the database.
    cur.execute("""select username, password from watson_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        message_body = json.dumps(credential)
        credentials_queue.send_message(MessageBody=message_body)

    ids_queue = sqs.get_queue_by_name(QueueName='watson-urls')
    ids_queue.purge()

    cur.execute("SELECT * FROM query WHERE status = 'WATSON'")
    queries = cur.fetchall()
    for query in queries:
        cur.execute("""select distinct query.query_alias, url,
                              ((response#>>'{{engagement,share_count}}')::int +
                                  (response#>>'{{engagement,reaction_count}}')::int +
                                  (response#>>'{{engagement,comment_count}}')::int) as facebook_engagement
                        from facebook_url_stats, query
                        where (response#>>'{{engagement,share_count}}')::int +
                              (response#>>'{{engagement,reaction_count}}')::int +
                              (response#>>'{{engagement,comment_count}}')::int >= query.watson_threshold and
                          error is null and
                          query.query_alias = '{0}' and
                          facebook_url_stats.query_alias = '{0}' and
                          not exists(
                              select *
                              from watson
                              where watson.url = facebook_url_stats.url and
                                    watson.query_alias = '{0}')
                        order by facebook_engagement desc;""".format(query['query_alias']))
        no_more_results = False
        while not no_more_results:
            ids = cur.fetchmany(size=100)
            if len(ids) == 0:
                no_more_results = True
            else:
                message_body = {
                    'query_alias': query['query_alias'],
                    'language': query['language'],
                    'urls': [id['url'] for id in ids]
                }
                ids_queue.send_message(MessageBody=json.dumps(message_body))

    conn.close()