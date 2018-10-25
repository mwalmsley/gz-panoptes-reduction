import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gzreduction",
    version="0.0.1",
    author="Mike Walmsley",
    author_email="mike.walmsley@physics.ox.ac.uk",
    description="Data reduction utilities for Galaxy Zoo on Panoptes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)