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
Update the **config/eggplant.conf** file with the source file path that corresponds to your directory structure.
Also change the database connection details in this config file for your setup. Then simply run the python 
**EggplantETL.py** file from the project root directory as below:


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