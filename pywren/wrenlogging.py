import logging
import logging.config

# basically stolen from 
# https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/
def default_config(log_level='INFO'):
    logger = logging.getLogger(__name__)

    # load config from file
    # logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
    # or, for dictConfig
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,  # this fixes the problem
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level':'INFO',
                'class':'logging.StreamHandler',
                'formatter' : 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True
            }
        }
    })
