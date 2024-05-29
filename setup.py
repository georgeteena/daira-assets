from setuptools import find_packages, setup

setup(
    name="daira_assets",
    packages=find_packages(exclude=["daira_assets_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pendulum",
        "dagster-aws",
        "dagster-pandas",
        "pystarburst",
        "PyYAML",
        "pyarrow",
        "dagit",
        "psycopg2-binary",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
