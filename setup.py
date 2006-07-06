#!/usr/bin/env python2.4

from distutils.core import setup

setup(name='bzr-hg',
      description='Support for Mercurial branches in Bazaar-NG',
      keywords='plugin bzr hg mercurial bazaar',
      version='0.1',
      url='http://bazaar-vcs.org/BzrForeignBranches/Mercurial',
      license='GPL',
      author='Robert Collins',
      author_email='robertc@robertcollins.net',
      long_description="""
      This plugin adds support for branching off Mercurial branches in 
      Bazaar.
      """,
      package_dir={'bzrlib.plugins.hg':'.'},
      packages=['bzrlib.plugins.hg']
      )
