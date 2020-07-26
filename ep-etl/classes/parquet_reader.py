import logging
import pyarrow.parquet as pq
import sys


class ParquetReader:

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

    def read(self):
        """
        Reads in a Parquet file using pyarrow and converts it to a Pandas DataFrame.
        :return: A Pandas DataFrame with the rows read in from the Parquet file.
        :rtype DataFrame
        """
        try:
            self.__logger.info("Reading Parquet File")
            table = pq.read_table(self.__config.source_file)
            self.__logger.debug("Converting to Pandas DataFrame")
            df = table.to_pandas(timestamp_as_object=True)  # flag to parse dates as objects
            self.__logger.info("Finished reading file")
        except IOError as e:
            self.__logger.error("Error reading parquet file %s", e)
            raise
        except Exception as e:
            self.__logger.error("Error processing parquet file %s", e)
            raise
        return df
