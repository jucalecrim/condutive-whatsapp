from setuptools import setup, find_packages
import os

# Read the version from __init__.py
def get_version():
    version_file = os.path.join("src/condutive_whatsapp", "__init__.py")
    with open(version_file) as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"')
    raise RuntimeError("Unable to find version string in __init__.py.")


setup(
    name="condutive-whatsapp",  # Your package name
    version=get_version(),
    packages=find_packages(),
    install_requires= ['numpy', 'pandas', 'datetime', 'flask'],  # Add dependencies here, e.g., ["numpy"]
    description="Pacote de formulas para API do bot de whatsapp",
    author="jucalecrim",
    author_email="jucalecrim@outlook.com",
    url="https://github.com/jucalecrim/condutive-whatsapp.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)