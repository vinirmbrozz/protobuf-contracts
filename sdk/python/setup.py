from setuptools import setup, find_packages

setup(
    name="truther-contracts",
    version="1.0.0",
    description="Python SDK for Truther Contracts",
    author="vinirmbrozz",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "protobuf>=4.24.0",
        "requests>=2.28.0",
    ],
    python_requires=">=3.8",
)
