import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbloader",
    version="0.0.1",
    author="cbemben",
    author_email="chrbems@gmail.com",
    description="A set of utilites to help load external data into db",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cbemben/db-loader",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)