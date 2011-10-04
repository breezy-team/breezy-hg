#!/usr/bin/env python

from info import *

if __name__ == '__main__':
    from distutils.core import setup

    command_classes = {}
    try:
        from bzrlib.bzr_distutils import build_mo
    except ImportError:
        pass
    else:
        command_classes['build_mo'] = build_mo

    version_string = ".".join([str(v) for v in bzr_plugin_version[:3]])
    setup(name='bzr-hg',
          description='Support for Mercurial branches in Bazaar',
          keywords='plugin bzr hg mercurial bazaar',
          version=version_string,
          url='http://bazaar-vcs.org/BzrForeignBranches/Mercurial',
          license='GPL',
          maintainer='Jelmer Vernooij',
          maintainer_email='jelmer@samba.org',
          long_description="""
          This plugin adds limited support for checking out and viewing
          Mercurial branches in Bazaar.
          """,
          package_dir={'bzrlib.plugins.hg':'.'},
          packages=['bzrlib.plugins.hg', 'bzrlib.plugins.hg.tests'],
          cmdclass=command_classes,
          )
