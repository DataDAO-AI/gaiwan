from setuptools import setup, find_packages

setup(
    name="gaiwan",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "pandas",
        "aiohttp",
        "beautifulsoup4",
        "tqdm",
        "orjson",
    ],
    python_requires=">=3.6",
) 