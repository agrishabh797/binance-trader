Prerequisites

1. SET THE PYTHONPATH

export PYTHONPATH=~/workspace/python:$PYTHONPATH

2. Create the table from the schema present under /home/tbde/workspace/sql/schema/historical_trade_data.sql

3. create the directory /home/tbde/archive/JAN-22/

4. Run the code -

python3 /home/tbde/workspace/python/Ingestion.py