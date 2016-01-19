import time
import threading
from source import db
from queue import Queue
from collections import OrderedDict
from api.sql import insert


def usuario_importacao(database=db.SASC):
    cursor = db.cursor(database)
    return cursor.execute_scalar('select id from tb_usuario where usuario = %s',
                                 'importacao')


def executar_importacao(importavel, offset, limit):
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
