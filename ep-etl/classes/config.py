import logging
import configparser
import os

DEFAULT_CONFIG = 'ep-etl.conf'


class Config(object):
    """
    The Config object is used to retrieve and store configuration details from the config file
    """

    # Constructor
    def __init__(self, parent_logger, config_path):
        """
        Config details are read and stored as attributes. The config is loaded from the specified config_path.

        Parameters
        ----------
        parent_logger: str
            Name of the parent logger which from module which calls this class.
        config_path: str
            The directory path of where config files are located.

        """
        config = configparser.ConfigParser()

        self.__logger = logging.getLogger(parent_logger + '.' + __name__)
        self.__logger.debug('Creating %s object', __name__)

        try:
            # Read in the config
            config_file = os.path.join(config_path, DEFAULT_CONFIG)
            self.__logger.debug('Reading config file at %s', config_file)
            config.read(config_file)
            default_config = config.defaults()

            for key in default_config:
                self.__logger.debug("%s = %s", key, default_config.get(key))

            # Set attributes with config values
            self.database_uri = config.get('DEFAULT', 'DATABASE_URI')
            self.source_file = config.get('DEFAULT', 'SOURCE_FILE')
            try:
                self.transaction_size = int(config.get('DEFAULT', 'TRANSACTION_SIZE'))
            except ValueError:
                self.__logger.error('Could not parse integer. Please ensure config value is an integer without commas.')

        except IOError:
            self.__logger.debug('Could not open Config file')
            raise IOError

    @property
    def database_uri(self):
        return self.__database_uri

    @database_uri.setter
    def database_uri(self, var):
        self.__database_uri = var

    @property
    def source_file(self):
        return self.__source_file

    @source_file.setter
    def source_file(self, var):
        self.__source_file = var

    @property
    def transaction_size(self):
        return self.__transaction_size

    @transaction_size.setter
    def transaction_size(self, var):
        self.__transaction_size = var
