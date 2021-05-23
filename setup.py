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
    packages=setuptools.find_packages(),
    install_requires=[
        "pyyaml",
        "fire",
    ],
    scripts=["bin/god"],
    python_requires=">=3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
)
