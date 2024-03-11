from telegram.ext import Application, MessageHandler, filters

from.message_handler import elaborate_message

def add_handlers(app: Application):
    """Adds all the needed handlers to the application
    Args:
        app: bot application
    """

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, elaborate_message))