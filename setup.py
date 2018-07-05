import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="benchmark",
    version="0.0.1",
    author="Jeremy Smith, Joseph Bochenek",
    author_email="joe.bochenek@uct.ac.za",
    description="A package for measureing resource usage of high-performance scientific code.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/idia-astro/benchmark",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
