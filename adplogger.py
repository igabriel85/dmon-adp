import logging
from logging.handlers import TimedRotatingFileHandler
import os
import datetime

logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.ERROR)

# add a rotating handler
logFile = os.path.join(os.path.dirname(os.path.abspath('')), 'dmonadp.log')
handler = TimedRotatingFileHandler(logFile, when="d", interval=1, backupCount=5)
logger.addHandler(handler)

