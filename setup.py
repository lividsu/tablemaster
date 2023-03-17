from setuptools import setup, find_packages

setup(
    name='tablemaster',
    version='1.0.1',
    packages=find_packages(),
    install_requires=[
        'mysql-connector',
        'PyMySQL',
        'SQLAlchemy==1.4.46',
        'pandas',
        'python-dateutil',
        'pygsheets',
        'tqdm',
        'numpy',
    ],
    author='Livid',
    author_email='livid.su@gmail.com',
    description='A Python package makes it easy to manage tables anywhere',
    url='https://github.com/ilivid/tablemaster'
)