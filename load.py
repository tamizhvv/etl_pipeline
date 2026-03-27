import sqlite3
import config
import logging
logger = logging.getLogger('etl_pipeline')
            

def load_data(data):
    logger.info('Load Started')
  
    connection=sqlite3.connect(config.db)
    cursor=connection.cursor()

    cursor.execute('create table if not exists posts (id integer primary key, userid integer, title text, body text, loaded_at text)')
    count=0
    for d in data:
        try:
            cursor.execute('''
                        insert or replace into posts (id,userId,title,body,loaded_at)values (?,?,?,?,?)
                        ''',(d['id'],d['userId'],d['title'],d['body'],d['loaded_at']))
            count+=1
        except Exception as e:
            logger.error(f'the record failed {d}:{e}')
    connection.commit()
    connection.close()
    logger.info(f'{count} records were loaded and {len(data)-count} were failed ')