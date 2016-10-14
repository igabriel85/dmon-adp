import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)

# add a rotating handler
handler = TimedRotatingFileHandler('dmonadp.log', when="d", interval=1, backupCount=5)
logger.addHandler(handler)

