import codecs
import io
import os
import re
import sys
import webbrowser
import platform
import configparser

try:
    from setuptools import setup
except:
    from distutils.core import setup
"""
"""

if sys.version_info.major != 3 or sys.version_info.minor not in [4, 5, 6, 7, 8]:
    print('wrong version, should be 3.4/3.5/3.6/3.7/3.8 version')
    sys.exit()

with io.open('QACTPBeeBroker/__init__.py', 'rt', encoding='utf8') as f:
    context = f.read()
    VERSION = re.search(r'__version__ = \'(.*?)\'', context).group(1)
    AUTHOR = re.search(r'__author__ = \'(.*?)\'', context).group(1)


def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()


NAME = "QACTPBeeBroker"
"""
"""
PACKAGES = ["QACTPBeeBroker"]
"""
"""

DESCRIPTION = "QACTPBeeBroker: broker for ctp bee"

KEYWORDS = ["quantaxis", "quant", "finance", "Backtest", 'Framework']
"""
"""

AUTHOR_EMAIL = "yutiansut@qq.com"

URL = "https://github.com/yutiansut/QACTPBEEBROKER"

LICENSE = "MIT"

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    install_requires=['quantaxis', 'quantaxis_pubsub>=1.6.3', 'ctpbee'],
    entry_points={
        'console_scripts': [
            'QACTPBEE = QACTPBeeBroker.__init__:go',
            'QACTPBEEPRO = QACTPBeeBroker.__init__:gopro'
        ]
    },
    # install_requires=requirements,
    keywords=KEYWORDS,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    license=LICENSE,
    packages=PACKAGES,
    include_package_data=True,
    zip_safe=True
)
