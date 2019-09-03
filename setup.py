import distutils
from distutils.core import setup
import glob

# The main call
setup(name='qcframework',
      version ='2.0.1',
      license = "GPL",
      description = "DESDM QC monitoring scripts",
      author = "Doug Friedel",
      author_email = "friedel@illinois.edu",
      packages = ['qcframework'],
      package_dir = {'': 'python'},
      data_files=[('ups',['ups/QCFramework.table'])],
      )

 
