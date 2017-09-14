import os

def env(name):
    try:
        value = os.environ[name]
    except KeyError:
        print('Environment variable ' + name + ' needs to be set! ')
        exit()
    return value
