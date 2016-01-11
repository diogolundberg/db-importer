""" Esse módulo contém funções de importação, responsáveis por realizar a
transferência de dados e controlar as threads para importação distribuida.
"""
import time
import threading
from source import db
from queue import Queue
from collections import OrderedDict
from api.sql import insert


def usuario_importacao(database=db.SASC):
    """ Esta função é utilizada pelo projeto sempre que o usuario de importação
    precisar ser informado. Ele será sempre selecionado da tb_usuario e no banco
    fornecido.

    Args:
        database (Config): Configurações do banco de dados aonde está a tabela
        de usuários.

    Returns:
        A id do usuário chamado 'importacao' da tabela 'tb_usuario'
    """
    cursor = db.cursor(database)
    return cursor.execute_scalar('select id from tb_usuario where usuario = %s',
                                 'importacao')


def executar_importacao(importavel, offset, limit):
    """ Função que executa a importação. Deve selecionar os dados do database
    origem do importável e inserir no banco de dados destino. As tuplas importadas
    do banco devem estar dentro do limit e offset.

    Args:
        importavel (Importavel): Um objeto da classe Importavel.
        offset (int): Este é o número do ponto de partida da importação.
        limit (int): É o número de tuplas máximo esse método irá importar.

    Yields:
        A importação é executada seguindo esses passos:
            1 - O cursor trará os dados como uma lista de tuplas do banco de dados
            definido em importavel.database_select
            2 - Realizando um iteração para cada tupla, será chamado o método
            'dados()' do importável, para ter o mapa que corresponde ao 'de para'
            desta tupla.
            3 - As chaves desse mapa devem ser armazenadas em ordem, na lista
            declarada como 'colunas'.
            4 - Os valores desse mapa devem ser armazenadas em formato de tupla,
            em ordem, na lista declarada como 'tuplas'.
            5 - Com a lista de colunas e tuplas, pode-se utilizar o método
            'insert' deste módulo, para realizar a inserção dos dados na tabela
            configurada em importavel.tabela.
    """
    tuplas = []
    colunas = []

    cursor_select = importavel.select(offset=offset, limit=limit)
    for row in cursor_select:
        dados = importavel.dados(row)

        for dependencia in importavel.lista_dependencias():
            dados.update(dependencia.dados(row))

        dados = OrderedDict(sorted(dados.items(), key=lambda t: t[0]))
        if len(colunas) < 1:
            colunas = [key for key in dados.keys()]
        tuplas.append(tuple(dados.values()))
    cursor_select.close()

    insert(database=importavel.database_insert,
           tabela=importavel.tabela,
           colunas=colunas,
           tuplas=tuplas)


def thread_importador(importavel, fila):
    """ É uma thread, cada thread criada com esse método deve executar os trabalhos
    a fila, até esvazia-la.

    Args:
        importavel (Importavel): Um objeto da classe Importavel.
        fila (Queue): É uma fila segura para threads, contendo várias faixas de
        importação. Cada faixa indicará qual o trabalho da thread.

    Yields:
        Define como cada thread funcionará. Cada thread deve repetir o processo
        de pegar uma item na fila e resovê-lo.
    """
    while True:
        item = fila.get()
        if item:
            print(item)
            executar_importacao(importavel=importavel,
                                offset=item[0],
                                limit=item[1])
            fila.task_done()


def distribuir_importacao(importavel,
                          primeira_row,
                          ultima_row,
                          numero_threads,
                          tamanho_fila):
    """ Função que cria fila de tarefas e inicia as threads que irão realiza-las.
    O número de threads e o tamanho da fila são dinâmicos, e o começo e o fim dos
    arquivos que serão manipulados.

    Args:
        importavel (Importavel): Um objeto da classe Importavel.
        primeira_row (int): Primeira linha do select no banco de dados a ser
        importada.
        ultima_row (int): Última linha do select no banco de dados a ser
        importada.
        numero_threads (int): O número de threads paralelas.
        tamanho_fila (int): É a quantidade total de tarefas que o importador
                            será divido.

    Yields:
        Este método divide a execução da importação em várias faixas de acordo
        com o tamanho da fila e respeitado a primeira e ultima linha.
        Essas faixas são colocadas em um fila segura para threads.
        Com a fila pronta, cria-se o número de threads informado por parâmetro.
        Essas threads funcionarão até que todas as tarefas estejam feitas.
    """
    offset = primeira_row - 1
    total = ultima_row - offset

    fila = Queue()
    limit_tarefa = total // tamanho_fila
    offset_tarefa = offset

    for _ in range(tamanho_fila):
        fila.put([offset_tarefa, limit_tarefa])
        offset_tarefa += limit_tarefa

    if offset_tarefa < ultima_row:
        limit_tarefa = total % tamanho_fila
        fila.put([offset_tarefa, limit_tarefa])

    start = time.perf_counter()

    for _ in range(numero_threads):
        thread = threading.Thread(target=thread_importador,
                                  args=[importavel, fila])
        thread.daemon = True  # thread morre quando a main acaba.
        thread.start()
    fila.join()

    tempo = int((time.perf_counter() - start))
    if tempo <= 1:
        print('Duração: menos de 1 segundo.')
    elif tempo // 60 > 60:
        print('Duração:', tempo, 'Minutos' if tempo // 60 > 1 else 'Minuto')
    else:
        print('Duração:', tempo, 'segundos')
