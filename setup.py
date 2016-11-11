from distutils.core import setup

setup(
    name='dmon-adp',
    version='v0.1.0',
    packages=['misc', 'test', 'service', 'dmonweka', 'adpengine', 'dmonpoint', 'dmonscikit', 'adpformater',
              'dmontensorflow'],
    url='https://github.com/igabriel85/dmon-adp',
    license='Apache-2.0',
    author='Gabriel Iuhasz',
    author_email='iuhasz.gabriel@e-uvt.ro',
    description='DICE Anomaly Detection Platform'
)
