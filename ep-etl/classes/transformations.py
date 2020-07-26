import logging
from classes.models import UserEvents


class Transformations:

    def __init__(self, parent_logger, config):
        """
        Parameters
        ----------
        :param parent_logger: Name of the parent logger which from module which calls this class.
        :param config: Config file from the parent.
        """
        self.__config = config
        self.__logger = logging.getLogger(parent_logger + '.' + __name__)
        self.__logger.debug("Creating %s object", __name__)

    def process_records(self, df):
        """
        Takes a Pandas DataFrame as input and performs data transformations. In this case, we just convert binary
        values to strings.

        :param df: The Pandas DataFrame to apply transformations on.
        :return: A list of UserEvents objects to load into Postgres.
        :rtype list[UserEvents]
        """
        self.__logger.info("Start Data Transformations")
        try:

            user_events = []
            for index, row in df.iterrows():
                user_events.append(UserEvents(_id=row[b'id'].decode("utf-8"),
                                              _type=row[b'type'].decode("utf-8"),
                                              date=row[b'date'],
                                              page=row[b'page'].decode("utf-8"),
                                              device=row[b'device'].decode("utf-8"),
                                              operating_system=row[b'operating_system'].decode("utf-8"),
                                              operating_system_version=row[b'operating_system_version'].decode("utf-8"),
                                              browser=row[b'browser'].decode("utf-8"),
                                              browser_version=row[b'bowser_version'].decode("utf-8")))

            self.__logger.info("Finished Data Transformations")
        except KeyError as e:
            self.__logger.error("Could not find keys\n\t%s", e)

        return user_events
