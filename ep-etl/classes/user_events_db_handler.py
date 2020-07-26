import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from classes.models import Base


class UserEventsHandler:

    def __init__(self, parent_logger, config):
        """
        Parameters
        ----------
        :param parent_logger: Name of the parent logger which from module which calls this class.
        :type parent_logger str
        :param config: Config file from the parent.
        :type config Config
        """
        self.__config = config
        self.__logger = logging.getLogger(parent_logger + '.' + __name__)
        self.__logger.debug("Creating %s object", __name__)
        self.__engine = create_engine(config.database_uri)
        self.__session = sessionmaker(bind=self.__engine)

    @contextmanager
    def session_scope(self):
        session = self.__session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.__logger.error("Database error %s", e)
            raise
        finally:
            session.close()

    def recreate_table(self):
        """
        Drops all tables in models.py and then creates them again.
        """
        Base.metadata.drop_all(self.__engine)
        Base.metadata.create_all(self.__engine)
        self.__logger.info("Recreating Table in Postgres")

    def bulk_load(self, user_events):
        """
        Bulk loads records in the DataFrame to Postgres. Uses the transaction_insert function to split large number of
        rows into transaction sizes specified in the config file.

        :param user_events: A list of UserEvents objects that have been extracted from a Parquet file.
        :type user_events list[UserEvents]
        """
        self.__logger.info("Begin Loading to Postgres")

        self.recreate_table()
        self.transaction_insert(user_events)

    def transaction_insert(self, rows):
        """
        Bulk inserts rows into Postgres based on the TRANSACTION_SIZE variable set in the config file.

        :param rows: The list of the rows that need to be inserted into Postgres.
        :type rows list[UserEvents]
        """
        if len(rows) > self.__config.transaction_size:
            with self.session_scope() as s:
                s.bulk_save_objects(rows[:self.__config.transaction_size])
            self.__logger.info("Loaded %s rows to Postgres", f'{self.__config.transaction_size:,}')
            self.transaction_insert(rows[10000:])
        else:
            with self.session_scope() as s:
                s.bulk_save_objects(rows)
            self.__logger.info("Loaded %s rows to Postgres", f'{len(rows):,}')
