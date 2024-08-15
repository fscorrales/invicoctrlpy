from setuptools import setup, find_packages

setup(
    name='invicoctrlpy',
    description="""
    Set of classes, methods and jupyter notebooks to control INVICO’s 
    (Instituto de Vivienda de Corrientes) budget execution. The database 
    it's not provide within this package due to privacy reasons.
    """,
    author='Fernando Corrales',
    author_email='fscpython@gmail.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'nbconvert',
        'ipykernel',
        'itables',
        'pivottablejs',
    ],
    dependency_links=[
        'http://github.com/fscorrales/invicodatpy/tarball/master#egg=invicodatpy#',
        'http://github.com/fscorrales/invicoDB/tarball/master#egg=invicoDB#']
)
