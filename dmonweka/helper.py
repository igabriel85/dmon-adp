import os


def getDataDir():
    """
    Returns the data directory.
    :return: the data directory
    :rtype: str
    """
    rootdir = os.path.dirname(__file__)
    libdir = rootdir + os.sep + "data"
    return libdir


def printTitle(title):
    """
    Prints the title underlined.
    :param title: the title to print
    :type title: str
    """

    print("\n" + title)
    print("=" * len(title))


def printInfo(info):
    """
    Prints the info.
    :param info: the info to print
    :type info: str
    """

    print("\n" + info)


