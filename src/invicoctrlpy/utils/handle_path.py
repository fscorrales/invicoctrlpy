import os
import inspect


# --------------------------------------------------
def get_utils_path():
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    return dir_path

# --------------------------------------------------
def get_invicoctrlpy_path():
    dir_path = get_utils_path()
    dir_path = os.path.dirname(dir_path)
    return dir_path

# --------------------------------------------------
def get_src_path():
    dir_path = get_invicoctrlpy_path()
    dir_path = os.path.dirname(dir_path)
    return dir_path

# --------------------------------------------------
def get_outside_path():
    dir_path = os.path.dirname(os.path.dirname(get_src_path()))
    return dir_path

# --------------------------------------------------
def get_db_path():
    db_path = (get_outside_path() 
                    + '/Python Output/SQLite Files')
    return db_path

# --------------------------------------------------
def get_update_path_input():
    dir_path = (get_outside_path() 
                + '/invicoDB/Base de Datos')
    return dir_path