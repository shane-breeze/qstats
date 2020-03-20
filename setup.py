from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), 'r') as f:
    long_description = f.read()

setup(
    name="qstats",
    version="0.2.1",
    author="Shane Breeze",
    author_email="sdb15@ic.ac.uk",
    description="SGE qstat info",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shane-breeze/qstats",
    packages=find_packages(),
    scripts=[
        "scripts/QSTAT",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "numpy>=1.18.1",
        "pandas>=1.0.1",
        "xmltodict>=0.12.0",
    ],
)
