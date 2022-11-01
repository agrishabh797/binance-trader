import logging
import psycopg2
from utils.read_env import read_env
from utils.db_utils import get_db_details
from datetime import datetime
import os
import queue
import threading
import numpy as np
import subprocess
import re


q = queue.Queue()

def load_csv_to_db(csv_file_name, connections_file, database_identifier, database_table):

    conn_details = get_db_details(connections_file, database_identifier)
    conn = psycopg2.connect(database=conn_details["DATABASE_NAME"],
                            user=conn_details["USER"], password=conn_details["PASSWORD"],
                            host=conn_details["HOST_NAME"], port=conn_details["PORT"]
    )

    sql = '''COPY {}(symbol, timestamp,
    ltp,ltq, oi, bid, bid_qty, ask, ask_qty, expiry_date)
    FROM '{}'
    DELIMITER ','
    CSV;'''.format(database_table, csv_file_name)

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()
    
    # os.remove(csv_file_name)


# Reader Thread Class
class Reader(threading.Thread):

    def __init__(self, name, files_chunks, target_directory, shell_file_name):
        threading.Thread.__init__(self)
        self.files_chunks = files_chunks
        self.target_directory = target_directory
        self.shell_file_name = shell_file_name
        self.name = name

    def run(self):

        global q
        i = 1
        for file_chunk in self.files_chunks:
            logging.info("%s Reading %s Files", self.name, len(file_chunk))
            
            file_list_as_string = "'" + "' '".join(map(str, file_chunk)) + "'"
            target_file_name = self.name + '_thread_' + str(i) + '.csv'
            target_file_name_full_path = self.target_directory + '/' + target_file_name
            command = "bash " +  self.shell_file_name + " '" + self.target_directory + "' '" + target_file_name + "' " + file_list_as_string
            p = subprocess.call(command, shell=True)
            q.put(target_file_name_full_path)
            i = i + 1

            logging.info("%s Created the File %s", self.name, target_file_name_full_path)


# Loader Thread Class
class Loader(threading.Thread):

    def __init__(self, name, connections_file, database_identifier, database_table, event):
        threading.Thread.__init__(self)
        self.connections_file = connections_file
        self.database_identifier = database_identifier
        self.database_table = database_table
        self.name = name
        self.event = event

    def run(self):

        global q
        while True:
            try:
                csv_file_name = q.get(timeout=2)
                logging.info("\t\t%s Loading File %s", self.name, csv_file_name)
                load_csv_to_db(csv_file_name, self.connections_file, self.database_identifier, self.database_table)
                logging.info("\t\t%s Loaded File %s in db", self.name, csv_file_name)
            except queue.Empty:
                if self.event.is_set():
                    break


def create_listfile(source_data_directory, worspace_dir):

    listfile_name = worspace_dir + "/src/historical_trades_files.list"

    with open(listfile_name, "w") as txtfile:
        for dirpath,_,filenames in os.walk(source_data_directory):
            for f in filenames:
                txtfile.write("%s\n" % os.path.abspath(os.path.join(dirpath, f)))
    txtfile.close()
    return listfile_name


def split_files_of_n_chunks_each(files, chunk_size):

    files_chunks = list()
    for i in range(0, len(files), chunk_size):
        files_chunks.append(files[i:i+chunk_size])

    return files_chunks


def split_list_into_n_groups(list_in, size):
    n_splits = np.array_split(list_in, size)
    splitted_list = []
    for array in n_splits:
        splitted_list.append(list(array))

    return splitted_list

def main():

    # Set the config parameters using the config file

    current_time = datetime.utcnow()
    current_date = current_time.strftime('%Y-%m-%d')
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    worspace_dir = '/home/tbde/workspace'

    config_file = worspace_dir + '/config/env_config.yaml'
    connections_file = worspace_dir + '/config/connections.yaml'
    configs = read_env(config_file)

    source_data_directory  = configs["SOURCE_DATA_DIRECTORY"]
    log_base_directory = configs["LOG_BASE_DIRECTORY"]
    database_identifier = configs["DATABASE_IDENTIFIER"]
    database_table = configs["DATABASE_TABLE"]
    archive_directory = configs["ARCHIVE_DIRECTORY"]
    shell_file_name = worspace_dir + '/shell/transform_csv.sh'
    # Setting the Logging

    log_directory = log_base_directory + '/' + current_time.strftime('%Y%m%d')

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_file_name = log_directory + '/Ingestion_' + current_time.strftime('%Y%m%d%H%M%S') + '.log'
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
    logging.debug("Ingestion Script Started at %s", current_timestamp)

    # Creating the list file from source_data_directory

    listfile_name = create_listfile(source_data_directory, worspace_dir)
    logging.info("List File created - %s", listfile_name)

    with open(listfile_name) as f:
        files = f.read().splitlines()

    files_chunks = split_files_of_n_chunks_each(files, 100)

    # Creating Threads
    e = threading.Event()
    readers_count = 5
    readers = []
    index = 1
    grouped_lists = split_list_into_n_groups(files_chunks, readers_count)
    for grouped_list in grouped_lists:
        reader = Reader(("Reader_" + str(index)), grouped_list, archive_directory, shell_file_name)
        reader.start()
        readers.append(reader)
        index = index + 1

    loaders_count = 10
    loaders = []

    for i in range(loaders_count):
        loader = Loader(("Loader_" + str(i + 1)), connections_file, database_identifier, database_table, e)
        loader.start()
        loaders.append(loader)

    # Waiting for threads to complete
    for reader in readers:
        reader.join()

    e.set()

    for loader in loaders:
        loader.join()


if __name__=="__main__":
    main()
