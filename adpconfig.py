from ConfigParser import SafeConfigParser


def readConf(file):
    '''
    :param file: location of config file
    :return: conf file as dict
    '''
    parser = SafeConfigParser()
    parser.read(file)
    conf = {}
    for selection in parser.sections():
        inter = {}
        for name, value in parser.items(selection):
            inter[name] = value
        conf[selection] = inter
    return conf

