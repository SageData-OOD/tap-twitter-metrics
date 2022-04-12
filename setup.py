#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-twitter-metrics",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_twitter_metrics"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.2",
        "requests",
        "tweepy"
    ],
    entry_points="""
    [console_scripts]
    tap-twitter-metrics=tap_twitter_metrics:main
    """,
    packages=["tap_twitter_metrics"],
    package_data={
        "schemas": ["tap_twitter_metrics/schemas/*.json"]
    },
    include_package_data=True,
)
