import yaml

def read_env(file_name):
    with open(file_name) as file:
        env_details = yaml.load(file, yaml.Loader)
    return env_details
