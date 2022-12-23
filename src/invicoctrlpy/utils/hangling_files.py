import os
import inspect

def get_utils_path():
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    return dir_path

def get_invicoctrlpy_path():
    dir_path = get_utils_path()
    dir_path = os.path.dirname(dir_path)
    return dir_path

def get_src_path():
    dir_path = get_invicoctrlpy_path()
    dir_path = os.path.dirname(dir_path)
    return dir_path