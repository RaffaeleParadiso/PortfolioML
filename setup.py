from setuptools import setup, find_packages

setup(
    name='portfolioML',
    version='0.0.1',
    description='A python portfolio creation tool',
    url='https://github.com/DanieleMDiNosse/PortfolioML.git',
    author='Di Nosse Daniele Maria, Lasala Angelo, Paradiso Raffaele',
    author_email='raffaele05@gmail.com',
    license='gnu general public license',
    packages = find_packages(),
    install_requires=['numpy', 'requests', 'scikit-learn'],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.8',
    ],
)

