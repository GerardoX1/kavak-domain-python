from os import getenv

import setuptools

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

requirements_list = ["pydantic==2.5.3", "haversine==2.8.0"]

LIB_NAME: str = "kavak-domain-python"

env_version = getenv("VERSION", "1.0.0")
VERSION = env_version.split(".")
__version__ = VERSION
__version_str__ = ".".join(map(str, VERSION))

setuptools.setup(
    name=LIB_NAME,
    version=__version_str__,
    author="Luis Gerardo Fosado Baños",
    author_email="yeralway1@gmail.com",
    description="Kavak Domain Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/GerardoX1/{LIB_NAME}.git",
    include_package_data=True,
    keywords="kavak, domain, library, python",
    packages=setuptools.find_packages(),
    package_data={"": ["*.json"]},
    namespace_packages=["kavak"],
    install_requires=requirements_list,
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.9",
    zip_safe=True,
    test_suite="tests",
)
