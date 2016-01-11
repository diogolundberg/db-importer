""" Funções utilitárias relacionadas a manipulação de arquivos e comandos sql.
"""
from source import db
from sys import path
from os.path import join, dirname
import operator
import re

def select(database, sql):
    """ docstring """
    print(sql)
    conexao = db.conexao(database)
    cursor = conexao.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    conexao.commit()
    conexao.close()
    return result

def select_scalar(database, sql):
    """ docstring """
    print(sql)
    cursor = db.cursor(database)
    return cursor.execute_scalar(sql)

def insert(database, tabela, colunas, tuplas):
    """ Função utilizada internamente pelo módulo, que deve utilizar um cursor
    para inserir uma ou mais tuplas, em uma tabela no banco de dados.
    A ordem das colunas também deve ser informada.

    Args:
        cursor (Cursor): Objeto de um cursor, obtido através da conexão com o
                         banco de dados.
        tabela (String): Nome da tabela aonde os dados serão inseridos.
        colunas (Array[String]): Lista das colunas na ordem que os dados serão
        inseridos.
        tuplas (Array[Tuple]): Uma lista com as tuplas de dados a serem inseridos.

    Yields:
        O cursor executará todas as tuplas em um insert múltiplo.
        Atenção: O número máximo de tuplas para o SQL Server é 65.000.
    """
    conexao_insert = db.conexao(database)
    cursor = conexao_insert.cursor()

    query = ' INSERT INTO ' + tabela
    query += ' (' + ",".join(colunas) + ') '
    query += 'VALUES (' + ('%s, ' * (len(colunas) - 1)) + '%s)'

    cursor.executemany(query, tuplas)
    cursor.close()

    conexao_insert.commit()
    conexao_insert.close()

def executar_arquivo_sql(database, sql):
    """ docstring """
    conexao = db.conexao(database)
    cursor = conexao.cursor()

    file = open(join(dirname(path[0]), 'sql', sql))
    sql = "".join(file.readlines())
    sqls = re.split(r"^GO$", sql, flags=re.MULTILINE | re.IGNORECASE)

    for sql in sqls:
        # print(sql)
        cursor.execute(sql.encode('cp1252'))
    conexao.commit()
    conexao.close()

def executar_sql(database, sql):
    """ docstring """
    print(sql)
    conexao = db.conexao(database)
    cursor = conexao.cursor()
    cursor.execute(sql)
    conexao.commit()
    conexao.close()

def filtro_item(item, **parametros):
    """ docstring """
    resultado = True
    for valor in parametros:
        resultado = operator.and_(resultado, operator.eq(
            str(item[valor]).strip().upper(),
            str(parametros[valor]).strip().upper()))
    return resultado

class Tabela(object):

    """ Classe que representa uma tabela no banco de dados, utilizada para
    armazenar uma tabela na memória.
    """

    def __init__(self, tabela, colunas, fk_='id', database=db.SASC):
        """ Método construtor. Está garantindo que o objeto terá uma tabela,
        quais colunas serão armazenadas na memória, nome da fk e o banco de dados.

        Args:
            tabela (String): Nome da tabela que será selecionada.
            colunas (Array[String]): Lista com as colunas que devem ser gravadas.
            fk_ (String): Nome da coluna que a FK corresponde.
            database (Config): Configurações do banco de dados da tabela.

        Returns:
            Um objeto Tabela.
        """
        self.tabela = tabela
        self.fk_ = fk_
        self.database = database
        self._tabela_mapeada = self.mapear_tabela(colunas)

    def mapear_tabela(self, colunas):
        """ docstring """
        conexao = db.conexao(self.database)
        cursor = conexao.cursor(as_dict=True)
        cursor.execute(
            'SELECT ' + self.fk_ + ' as fk, ' + ", ".join(colunas) +
            ' FROM ' + self.tabela)
        tabela_mapeada = cursor.fetchall()
        cursor.close()
        conexao.close()
        return tabela_mapeada

    def get_fk(self, **parametros):
        """ docstring """
        item = next(
            (item for item in self._tabela_mapeada
             if filtro_item(item, **parametros)), None)
        return item['fk'] if item else None
