from dotenv import load_dotenv
import os
load_dotenv()
api=os.getenv('API_URL')
db=os.getenv('DB_PATH')
log=os.getenv('LOG_FILE')
retry=int(os.getenv('RETRY_ATTEMPTS'))