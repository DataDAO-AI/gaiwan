from setuptools import setup, find_packages

setup(
    name="gaiwan",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "pandas",
        "tqdm",
        "orjson",
        "pytest",  # for running tests
        "urllib3",  # required by requests but good to specify
    ],
) 