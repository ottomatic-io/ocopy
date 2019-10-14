from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    author="Ben Hagen",
    author_email="ben@ottomatic.io",
    description="Securely copy files to multiple destinations using source and destination verification.",
    entry_points={"console_scripts": ["ocopy = ocopy.cli.ocopy:cli"]},
    include_package_data=True,
    install_requires=[
        "click>=7.0",
        "lxml>=4.4.1",
        "sh>=1.12.14",
        "xxhash>=1.4.2",
    ],
    dependency_links=[],
    long_description=long_description,
    long_description_content_type="text/markdown",
    name="ocopy",
    packages=find_packages(),
    setup_requires=["pytest-runner", "setuptools_scm"],
    tests_require=["pytest"],
    url="https://github.com/ottomatic-io/ocopy",
    use_scm_version=True,
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
    ],
)
