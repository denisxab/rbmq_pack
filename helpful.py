from os import environ
from os.path import exists
from pprint import pprint
from re import sub

from logsmal import logger


def readAndSetEnv(*path_files: str):
    """
    Чтение переменных окружения из указанного файла,
    и добавление их в ПО `python`
    """
    for _path_file in path_files:
        if exists(_path_file):
            with open(_path_file, 'r', encoding='utf-8') as _file:
                res = {}
                for line in _file:
                    tmp = sub(r'^#[\s\w\d\W\t]*|[\t\s]', '', line)
                    if tmp:
                        k, v = tmp.split('=', 1)
                        # Если значение заключено в двойные кавычки, то нужно эти кавычки убрать
                        if v.startswith('"') and v.endswith('"'):
                            v = v[1:-1]

                        res[k] = v
            environ.update(res)
            pprint(res)
        else:
            logger.warning(f"No search file {_path_file}")
