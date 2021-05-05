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

        logging.config.fileConfig(fname=self.config['Logging']['configuration_file'],   # type: ignore
                                  disable_existing_loggers=False)

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


class ConfigurationWithCredentials(Configuration):
    """
    Contains methods to obtain configurations for this application.
    """

    def __init__(self, name: str):
        super().__init__(name)

        self.credentials_config = ConfigParser()
        self.credentials_config.read(['credentials.ini', '/vault/secrets/credentials.ini'])

    def get_credentials_config(self) -> ConfigParser:
        """
        The credential config for the application.
        """
        return self.credentials_config
