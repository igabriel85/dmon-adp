import logging
from logging.handlers import TimedRotatingFileHandler
import os
import datetime

logger = logging.getLogger("ADP Log")
logger.setLevel(logging.WARNING)

loggerESt = logging.getLogger('elasticsearch.trace')
loggerESt.setLevel(logging.INFO)
loggerES = logging.getLogger('elasticsearch')
loggerES.setLevel(logging.INFO)
loggerurl3 = logging.getLogger("urllib3")
loggerurl3.setLevel(logging.INFO)


# add a rotating handler
logFile = os.path.join(os.path.dirname(os.path.abspath('')), 'dmonadp.log')
handler = TimedRotatingFileHandler(logFile, when="d", interval=1, backupCount=5)
logger.addHandler(handler)
loggerESt.addHandler(handler)
loggerES.addHandler(handler)
loggerurl3.addHandler(handler)

