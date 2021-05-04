"""
Contains Osiris common configuration functions
"""
from configparser import ConfigParser

import logging.config
from logging import Logger


class Configuration:
    """
    Contains methods to obtain configurations for this application.
    """

    def __init__(self, name: str):
        self.config = ConfigParser()
        self.config.read(['conf.ini', '/etc/osiris/conf.ini'])

        logging.config.fileConfig(fname=self.config['Logging']['configuration_file'], disable_existing_loggers=False)

        self.name = name

    def get_config(self) -> ConfigParser:
        """
        The configuration for the application.
        """
        return self.config

    def get_logger(self) -> Logger:
        """
        A customized logger.
        """
        return logging.getLogger(self.name)
