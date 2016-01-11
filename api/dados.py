""" Funções utilitárias para tratamento de dados, como formatação de texto e
    conversão de formatos.
"""
import time
import re

def remove_mascara(numero):
    """ Método para remover a máscara de números.

        Args:
            numero (String): Número com máscara.

        Returns:
            A string fornecida por parâmetro, somente com números.
    """
    regex = re.compile(r'[^\d,]+')
    return regex.sub('', numero)


def ajustar_codigo_fator(fator):
    """ Método para remover a máscara de números.

        Args:
            numero (String): Número com máscara.

        Returns:
            A string fornecida por parâmetro, somente com números.
    """
    regex = re.compile(r'[^\dABC]+')
    return regex.sub('', fator.strip()) if fator else None

def data_atual():
    """ Método que fornece a data atual.

        Returns:
            Data atual.
    """
    return time.strftime("%Y-%m-%d")


def formata_hora(hora):
    """ Método para remover a máscara de números.

        Args:
            hora (String): A hora no formato gravado no Sasc-web xx:xx.

        Returns:
            A hora será validada e formatada, para ser inserida no novo banco.

        Raises:
            ValueError: Se a hora for maior do que 24, ou os minutos maiores que
            60.
    """
    hora = [i.strip() for i in hora.split(':') if hora]
    if len(hora) == 2:
        horas = None
        minutos = None
        try:
            horas = int(hora[0])
            minutos = int(hora[1])
            if horas > 24 or minutos > 59:
                raise ValueError('Horário inválido')
        except ValueError:
            print("Erro ao converter as horas de", hora[0], ':', hora[1])
            return None
        return hora[0] + ':' + hora[1]
    else:
        return None


def completa_zero_esquerda(numero, digitos):
    """ Método para completar o numero com zeros a esquerda. Os códigos numerais
        podem estar armazenados como numeros, portanto o banco de dados irá
        eliminar os zeros a esquerda do código.

        Args:
            numero (int): O número que será transformado em uma string com os
            dígitos corretos.
            digitos (int): O número de dígitos total do código.

        Returns:
            String do código com os zeros a esquerda até completar a quantidade
            de dígitos.
    """
    if numero and numero > 0:
        numero = str(numero).rstrip('.0')
        numero = (digitos - len(numero)) * '0' + numero
        return numero
    else:
        return None
