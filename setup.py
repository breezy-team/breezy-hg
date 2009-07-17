#!/usr/bin/env python2.4

from distutils.core import setup

bzr_plugin_name = 'hg'

bzr_compatible_versions = [(1, 13, 0), (1, 14, 0), (1, 15, 0), (1, 16, 0), (1, 17, 0)]

bzr_minimum_version = bzr_compatible_versions[0]

bzr_maximum_version = bzr_compatible_versions[-1]

bzr_plugin_version = (0, 1, 0, 'dev', 0)

bzr_control_formats = {"Mercurial":{'.hg/': None}}


if __name__ == '__main__':
    version_string = ".".join([str(v) for v in bzr_plugin_version[:3]])
    setup(name='bzr-hg',
          description='Support for Mercurial branches in Bazaar',
          keywords='plugin bzr hg mercurial bazaar',
          version=version_string,
          url='http://bazaar-vcs.org/BzrForeignBranches/Mercurial',
          license='GPL',
          author='Robert Collins',
          author_email='robertc@robertcollins.net',
          long_description="""
          This plugin adds limited support for checking out and viewing 
          Mercurial branches in Bazaar.
          """,
          package_dir={'bzrlib.plugins.hg':'.'},
          packages=['bzrlib.plugins.hg']
          )
