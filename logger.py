import logging
import uuid
import os
def logs(name):
    logger=logging.getLogger(name)
    logger.setLevel('INFO')

    run_id=str(uuid.uuid4())[:8]

    formatter=logging.Formatter(
        '%(asctime)s - %(name)s - runid=' + run_id + ' - %(levelname)s - %(message)s'
    )

    if logger.handlers:
        logger.handlers.clear()

    file_handler=logging.FileHandler('pipeline.log')
    file_handler.setFormatter(formatter)

    console_handler=logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger,run_id