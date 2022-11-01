import yaml

def get_db_details(file_name, db_name):
    with open(file_name) as file:
        db_list = yaml.load(file, yaml.Loader)
    return db_list[db_name]
