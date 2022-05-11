from configparser import ConfigParser
import os,inspect

#Specifying directory and file path for config file
current_filename = inspect.getframeinfo(inspect.currentframe()).filename
print(current_filename)
parent_dir_filename = os.path.dirname(os.path.abspath(current_filename))
print("1.",parent_dir_filename)
parent_proj_dir = os.path.dirname(parent_dir_filename)
print(parent_proj_dir)

db_config_path = os.path.join(parent_dir_filename, 'config', 'db.ini')
print(db_config_path)
def config(section,filename=db_config_path):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}

    # Checks to see if section (postgresql) parser exists
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    # Returns an error if a parameter is called that is not listed in the initialization file
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db