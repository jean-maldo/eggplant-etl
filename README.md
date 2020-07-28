# Eggplant ETL

## Overview
The purpose of this project is to create an ETL solution that:
- Read in records from a parquet file.
- Writes them to a Postgres table.

## Script requirements
The following libraries have been used in addition to the libraries that ship with Python.

- configparser
- pyscopg
- pyarrow
- sqlalchemy

Optional for data profiling:
- pandas-profiling

## Installation
In order mitigate the risk of breaking your current python environment, It is recommended you create a virtual env with
 python 3.7 and then install the required libraries with:
```pip install -r requirements/req.txt```

## Usage
There's a **config/eggplant.conf** file which needs to be updated. Here is a table of the variables and sample values:

| Variable  | Description | Sample |
| ------------- | ------------- | ------------- |
| DATABASE_URI | The database connection string | postgres+psycopg2://postgres:pass@localhost:5432/db_name |
| SOURCE_FILE  | The local storage path to the source Parquet file | /home/usr/files |
| TRANSACTION_SIZE  | An integer with the number of SQL statements to include in a transaction  | 10000 |
| RECREATE_TABLES  | Boolean value stating whether to recreate the database tables  | True |

A Postgres Database can be quickly 
set up with the following docker command. Make sure to change the password and path for local volume storage:
```
docker run -d ^
	--name dev-postgres ^
	-e POSTGRES_PASSWORD=password ^
	-v /path/to/local/storage:/var/lib/postgresql/data ^
        -p 5432:5432 ^
        postgres
```

Once the config file has been updated, then you can simply run the python 
**ep-etl.py** file from the project root directory as below:

```python ./eggplant/ep-etl/ep_etl.py```

## Unit Testing
Not implemented as of yet

## Logging
The standard Python logging library is used and is configurable via **config/logging.conf**. The console logger is set 
to INFO by default and the file logger is set to DEBUG. These logging levels can be modified in the **handler** section 
of the config file. Log files are saved under the **logs** directory and the project is set up to rotate log files 
sequentially, keeping a history of 5 log files in addition to the current run.

## Results and Next Steps
SQL Select statement and CSV output can be found in the Files directory.

Next Steps:
* Add workflow management e.g. Airflow
* Database error handling
* For larger file processing, look at streaming with Kafka and Spark.
* Add unit testing