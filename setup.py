#from ez_setup import use_setuptools
#use_setuptools()
from setuptools import setup, find_packages

setup(name="bugalo",
      version="0.1dev",
      description="It is great and does many things",
      author="Nick Fisher",
      packages = find_packages(),
      zip_safe = True,
      entry_points = {
          'console_scripts': [
              'findo = bugalo.findo:main',
          ]
      }
     )
