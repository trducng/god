import setuptools

with open("./README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="god",
    version="0.0.1",
    author="john",
    author_email="trungduc1992@gmail.com",
    description="A collaborative data version control software",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/johntd54/god/",
    packages=setuptools.find_packages(exclude=("tests", "tests.*")),
    install_requires=[
        "pyyaml",
        "click",
        "rich",
        "tqdm",
        "binaryornot",
        "python-magic",
        "boto3",
    ],
    extra_requires=["ipython", "pdbpp", "pytest"],
    entry_points={
        "console_scripts": [
            "god=god.cli:entrypoint",
            "god-files=god.files:main",
            "god-descriptor=god.descriptors.base:main",
            "god-index=god.index.cli:main",
            "god-config=god.configs.cli:main",
        ]
    },
    python_requires=">=3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
)
