import logging


class LoggingConfig:
    """Configures the logging settings for the application.

    Sets the logging level to INFO and specifies the log message format to include
    the timestamp, log level, and message. The logs are output to the standard stream.

    Returns:
        logging.Logger: The root logger instance configured with the specified settings.
    """

    def __init__(self, name=None, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def get_logger(self):
        return self.logger

    @staticmethod
    def configure_logging():
        """Logger for the Brewery application."""

        log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[logging.StreamHandler()],
        )

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        return logger
