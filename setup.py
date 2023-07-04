from setuptools import setup, find_packages

setup(
    name='tablemaster',
    version='1.1.3',
    packages=find_packages(),
    install_requires=[
        'PyMySQL',
        'SQLAlchemy==1.4.46',
        'pandas',
        'python-dateutil',
        'gspread',
        'tqdm',
        'numpy',
        'mysql-connector-python',
        'pyyaml',
        'openpyxl',
    ],
    author='Livid',
    author_email='livid.su@gmail.com',
    description='A Python package makes it easy to manage tables anywhere',
    url='https://github.com/ilivid/tablemaster'
)