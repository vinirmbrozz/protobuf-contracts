from setuptools import setup, find_namespace_packages

setup(
    name="protobuf-contracts",
    version="1.0.0",
    description="Python SDK for Protobuf Contracts",
    author="vinirmbrozz",
    # The generated trees are top-level namespace packages (protobuf/*, buf/*),
    # so the absolute imports in *_pb2.py resolve when installed. protobuf_contracts
    # holds the hand-written serde + re-exports.
    packages=find_namespace_packages(
        include=["protobuf*", "buf*", "protobuf_contracts*"],
        exclude=["tests*"],
    ),
    install_requires=[
        "protobuf>=4.24.0",
        "requests>=2.28.0",
    ],
    python_requires=">=3.8",
)
