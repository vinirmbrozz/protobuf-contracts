from setuptools import setup, find_packages

setup(
    name="protobuf-contracts-sdk",
    version="1.0.0",
    description="Python SDK for Protobuf Contracts",
    author="vinirmbrozz",
    packages=find_packages(),
    install_requires=[
        "protobuf>=4.24.0",
    ],
    python_requires=">=3.8",
)
