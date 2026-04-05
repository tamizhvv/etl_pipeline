import psycopg2
import config
import logging 
logger=logging.getLogger('etl_pipeline')

def load_postgres(data):
    logger.info('Load started')
    connection=None
    try:
        connection=psycopg2.connect(
            host=config.pg_host,
            database=config.pg_database,
            user=config.pg_user,
            password=config.pg_password,
            port=config.pg_port
        )

        cursor=connection.cursor()
        cursor.execute('create table if not exists posts (id integer primary key, userid integer, title text, body text, loaded_at text)')
        count=0
        for d in data:
            try:
                cursor.execute('''
                               insert into posts(id,userId,title,body,loaded_at) values(%s,%s,%s,%s,%s)
                               on conflict(id) do update set 
                               userId=excluded.userId,
                               title=excluded.title,
                               BODY=excluded.body,
                               loaded_at=excluded.loaded_at''',
                               (d['id'], d['userId'], d['title'], d['body'], d['loaded_at']))
                count+=1              
            except Exception as e:
                logger.error(f'the record failed {d}:{e}')
        logger.info(f'{count} records were loaded and {len(data)-count} were failed ')
        return count
    finally:
        if connection:
            connection.commit()
            connection.close()

