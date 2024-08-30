from setuptools import setup, find_packages

setup(
    name="taggerdb",
    version="0.1.1",
    author="Angelo Oliveira Jr.",
    author_email="43245439+angelojrdev@users.noreply.github.com",
    description="A user-friendly tool designed for managing and tagging files",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/angelojrdev/taggerdb",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
