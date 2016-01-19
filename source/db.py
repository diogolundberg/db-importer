import pymssql
import _mssql
from configparser import ConfigParser
from sys import path
from os.path import join, dirname

DIRETORIO = dirname(path[0])
CONFIG = ConfigParser()


try:
    CONFIG.read(join(dirname(DIRETORIO), 'config.cfg'))
    DB = CONFIG['DB']
except KeyError:
    print('Erro ao ler arquivo: ', str(join(dirname(DIRETORIO), 'config.cfg')))


def conexao(database):
    return pymssql.connect(host=database['host'],
                           user=database['usuario'],
                           password=database['senha'],
                           database=database['database'])


def cursor(database):
    """ docstring """
    return _mssql.connect(server=database['host'],
                          user=database['usuario'],
                          password=database['senha'],
                          database=database['database'])
