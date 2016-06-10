from setuptools import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

APP_VERSION = "1.0.0"
APP_AUTHOR = "Jed Glazner"
APP_AUTHOR_EMAIL= "jglazner@coldcrow.com"
APP_NAME = "backup"

setup(
    name=APP_NAME,
    version=APP_VERSION,
    description="Misc tools for backing up things to AWS",
    long_description=long_description,
    url="https://github.com/jglazner/S3Backup",
    author=APP_AUTHOR,
    author_email=APP_AUTHOR_EMAIL,
    license="MIT",
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],

    keywords='AWS Backup Tools',

    packages=['backup'],

    package_data={'backup': ['*']},

    install_requires=["pycrypto", "Crypto", "boto", "argparse", "appdirs", "filechunkio"],

    entry_points={
        'console_scripts': [
            'backup=backup:main'
        ],
    },
)
