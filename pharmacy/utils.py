import codecs


def decode(encoded, _type):
    return codecs.decode(encoded, 'hex').decode()
