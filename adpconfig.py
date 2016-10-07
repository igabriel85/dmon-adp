from ConfigParser import SafeConfigParser


def readConf(file):
    '''
    :param file: location of config file
    :return:
    '''
    parser = SafeConfigParser()
    parser.read(file)

  #  print parser.sections()
  #  print parser.get('Connector', 'ESEndpoint')
    conf = {}
    for selection in parser.sections():
        inter = {}
       # print 'Section: ', selection
       # print '    Options:', parser.options(selection)
        for name, value in parser.items(selection):
          #  print '    %s = %s' % (name, value)
            inter[name] = value
        conf[selection] = inter
    return conf

readConf('dmonadp.ini')

# Config = ConfigParser.ConfigParser()
# Config.read('dmonadp.ini')
# section = Config.sections()
# print section
#
# def ConfugSectionMap(section):
#     dict1={}
#     options = Config.options(section)
#     for option in options:
#         try:
#             dict1[options] = Config.get(section, option)
#             if dict1[option] == -1:
#                 print "no option set"
#         except Exception as inst:
#             print type(inst)
#             print "Exception on %s!" % option
#             dict1[option] = None
#     return dict1
#
# print ConfugSectionMap(section[0])