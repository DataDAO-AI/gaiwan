# setup.py
from setuptools import setup, find_packages

setup(
    name="gaiwan",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "orjson",
        "numpy",
        "scipy",
        "scikit-learn",
    ],
    python_requires=">=3.7",
)