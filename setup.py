from setuptools import setup, find_packages

setup(
    name="remoroo",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "typer>=0.9.0",
        "jsonschema>=4.22.0",
        "requests>=2.31.0",
        "remoroo-core @ git+https://github.com/Remoroo/remoroo_core.git@main",
        "sseclient-py",
    ],
    entry_points={
        "console_scripts": [
            "remoroo = remoroo.cli:app",
        ],
    },
)
