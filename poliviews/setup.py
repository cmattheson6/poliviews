import setuptools

setuptools.setup(
    name='senate_members',
    version='0.0.1',
    install_requires=[
        'pandas',
        'unidecode'
    ],
    packages = ['transforms']
)