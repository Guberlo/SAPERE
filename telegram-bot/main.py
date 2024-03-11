import logging
import json

from telegram import Update
from telegram.ext import Application

from elasticsearch import Elasticsearch

from modules.handlers import add_handlers
from modules.data.config import Config

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def main(config: Config):
    """Main method that starts the bot"""
    application = Application.builder().token(config.bot_token).build()

    add_handlers(application)

    application.run_polling(allowed_updates=Update.MESSAGE)


if __name__ == '__main__':
    config = Config()
    main(config)