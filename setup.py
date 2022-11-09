from setuptools import find_packages, setup

setup(
    name="movie_rec",
    packages=find_packages(exclude=["movie_rec_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
