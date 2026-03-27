import sqlite3

from logger import logs
logger,run_id= logs('etl_pipeline')
import config
def load_data(data):
    logger.info('Load Started')
    # connection=sqlite3.connect(
    #     'host':local host,
    #     'database':pactice,
    #     'user':admin,
    #     'password':root
    # )
    connection=sqlite3.connect(config.db)
    cursor=connection.cursor()

    cursor.execute('create table if not exists posts (id integer primary key, userid integer, title text, body text, loaded_at text)')
    for d in data:
        cursor.execute('''
                    insert or replace into posts (id,userId,title,body,loaded_at)values (?,?,?,?,?)
                    ''',(d['id'],d['userId'],d['title'],d['body'],d['loaded_at']))
    connection.commit()
    connection.close()
    logger.info(f'{len(data)} were loaded ')