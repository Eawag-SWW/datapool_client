from setuptools import find_packages, setup

setup(
    name="datapool_client",
    version="1.1",
    description="Designed to access the datapool software developed by ETH Zurich - SIS and Eawag. "
    "Find out more under https://datapool.readthedocs.io/en/latest/.",
    author="Christian Foerster",
    author_email="christian.foerster@eawag.ch",
    license="MIT Licence",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=[
        "pandas",
        "numpy",
        "psycopg2-binary",
        "matplotlib",
        "cufflinks",
        "plotly",
        "pyparsing==2.4.7",
        "sqlalchemy",
        "tqdm",
    ],
    keywords="datapool_client, eawag, postgres",
    packages=find_packages(),
    include_package_data=True,
)
