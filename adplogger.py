import logging
from logging.handlers import RotatingFileHandler
import os
import datetime

logger = logging.getLogger("ADP Log")
logger.setLevel(logging.WARN)

loggerESt = logging.getLogger('elasticsearch.trace')
loggerESt.setLevel(logging.WARN)
loggerES = logging.getLogger('elasticsearch')
loggerES.setLevel(logging.WARN)
loggerurl3 = logging.getLogger("urllib3")
loggerurl3.setLevel(logging.WARN)


# add a rotating handler
logFile = os.path.join('dmonadp.log')
handler = RotatingFileHandler(logFile, maxBytes=100000000,  backupCount=5)
logger.addHandler(handler)
loggerESt.addHandler(handler)
loggerES.addHandler(handler)
loggerurl3.addHandler(handler)

