import setuptools

with open("README.md", "r", encoding="utf-8") as rm:
    long_description = rm.read()

setuptools.setup(
    name='fastapi-sqlAlchemy',
    version='1',
    description='Testing installation of Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/Vrajesh-Vaghasiya/fastapi-sqlAlchemy',
    packages=['middleware'],
    install_requires=['SQLAlchemy==2.0.36', 'fastapi==0.115.6', 'fastapi-pagination==0.12.32'],
)
