import time
import re

def remove_mascara(numero):
    regex = re.compile(r'[^\d,]+')
    return regex.sub('', numero)


def ajustar_codigo_fator(fator):
    regex = re.compile(r'[^\dABC]+')
    return regex.sub('', fator.strip()) if fator else None

def data_atual():
    return time.strftime("%Y-%m-%d")


def formata_hora(hora):
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
    if numero and numero > 0:
        numero = str(numero).rstrip('.0')
        numero = (digitos - len(numero)) * '0' + numero
        return numero
    else:
        return None
