# Copyright (C) 2005, 2006 Canonical Ltd
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


"""Mercurial support for Bazaar.

"""

import bzrlib
import bzrlib.api

from info import (
    bzr_compatible_versions,
    hg_compatible_versions,
    bzr_plugin_version as version_info,
    )

bzrlib.api.require_any_api(bzrlib, bzr_compatible_versions)

import bzrlib.bzrdir
from bzrlib import (
    errors,
    trace,
    )
from bzrlib.foreign import (
    foreign_vcs_registry,
    )
import bzrlib.lockable_files
from bzrlib.send import (
    format_registry as send_format_registry,
    )

_mercurial_loaded = False
LockWarner = bzrlib.lockable_files._LockWarner

def lazy_load_mercurial():
    global _mercurial_loaded
    if _mercurial_loaded:
        return
    _mercurial_loaded = True
    import mercurial
    try:
        from mercurial import demandimport
        demandimport.enable = lambda: None
    except ImportError:
        pass
    from mercurial.__version__ import version as hg_version
    hg_major_version = ".".join(hg_version.split(".")[:2])
    if hg_major_version not in hg_compatible_versions and not "+" in hg_version:
        raise errors.DependencyNotPresent("mercurial",
            'bzr-hg: Mercurial version %s not supported.' % hg_version)
    trace.mutter("bzr-hg: using Mercurial %s" % hg_version)


foreign_vcs_registry.register_lazy("hg",
    "bzrlib.plugins.hg.mapping", "foreign_hg", "Mercurial")

class HgDummyLock(object):
    """Lock that doesn't actually lock."""

    def __init__(self, hgrepo):
        self._hgrepo = hgrepo

    def lock_write(self, token=None):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)
        self._lock = self._hgrepo.wlock()

    def lock_read(self):
        self._lock = None

    def peek(self):
        raise NotImplementedError(self.peek)

    def unlock(self):
        if self._lock is not None:
            self._lock.release()

    def validate_token(self, token):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)


class HgLock(object):
    """A lock that thunks through to Hg."""

    def __init__(self, hgrepo):
        self._hgrepo = hgrepo

    def lock_write(self, token=None):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)
        self._lock = self._hgrepo.wlock()

    def lock_read(self):
        self._lock = self._hgrepo.lock()

    def peek(self):
        raise NotImplementedError(self.peek)

    def unlock(self):
        self._lock.release()

    def validate_token(self, token):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)

    def break_lock(self):
        pass


class HgLockableFiles(bzrlib.lockable_files.LockableFiles):
    """Hg specific lockable files abstraction."""

    def __init__(self, lock, transport):
        self._lock = lock
        self._transaction = None
        self._lock_mode = None
        self._transport = transport
        self._lock_warner = LockWarner(repr(self))


class HgDir(bzrlib.bzrdir.BzrDir):
    """An adapter to the '.hg' dir used by mercurial."""

    def __init__(self, hgrepo, transport, lockfiles, format):
        self._format = format
        self.root_transport = transport
        self.transport = transport.clone('.hg')
        self._hgrepo = hgrepo
        self._lockfiles = lockfiles

    def backup_bzrdir(self):
        self.root_transport.copy_tree(".hg", ".hg.backup")
        return (self.root_transport.abspath(".hg"),
                self.root_transport.abspath(".hg.backup"))

    def break_lock(self):
        """Mercurial locks never break."""
        raise NotImplementedError(self.break_lock)

    def clone(self, url, revision_id=None, basis=None, force_new_repo=False):
        """Clone this hg dir to url."""
        self._make_tail(url)
        if url.startswith('file://'):
            url = url[len('file://'):]
        url = url.encode('utf8')
        result = self._format.initialize(url)
        result._hgrepo.pull(self._hgrepo)
        return result

    def create_branch(self, name=None):
        """'create' a branch for this dir."""
        return self.open_branch(name=name)

    def create_repository(self, shared=False):
        """'create' a repository for this dir."""
        if shared:
            # dont know enough about hg yet to do 'shared repositories' in it.
            raise errors.IncompatibleFormat(self._format, self._format)
        return self.open_repository()

    def create_workingtree(self, revision_id=None, from_branch=None,
            accelerator_tree=None, hardlink=False):
        """'create' a workingtree for this dir."""
        if revision_id is not None:
            raise NotImplementedError("revision_id argument not yet supported")
        if from_branch is not None:
            raise NotImplementedError("from_branch argument not yet supported")
        return self.open_workingtree()

    def destroy_branch(self, name=None):
        if name is not None:
            raise errors.NoColocatedBranchSupport(self)
        raise errors.UnsupportedOperation(self.destroy_branch, self)

    def destroy_workingtree(self):
        raise errors.UnsupportedOperation(self.destroy_workingtree, self)

    def destroy_repository(self):
        raise errors.UnsupportedOperation(self.destroy_repository, self)

    def get_branch_transport(self, branch_format, name=None):
        if name is not None:
            raise errors.NoColocatedBranchSupport(self)
        if branch_format is None:
            return self.transport
        if isinstance(branch_format, HgBzrDirFormat):
            return self.transport
        raise errors.IncompatibleFormat(branch_format, self._format)

    get_repository_transport = get_branch_transport
    get_workingtree_transport = get_branch_transport

    def is_supported(self):
        return True

    def needs_format_conversion(self, format=None):
        return (format is not HgBzrDirFormat)

    def open_branch(self, name=None, ignored=None, unsupported=False):
        """'create' a branch for this dir."""
        if name is not None:
            raise errors.NoColocatedBranchSupport(self)
        from bzrlib.plugins.hg.branch import HgLocalBranch, HgRemoteBranch
        if self._hgrepo.local():
            branch_klass = HgLocalBranch
        else:
            branch_klass = HgRemoteBranch
        return branch_klass(self._hgrepo, self, self._lockfiles)

    def open_repository(self, shared=False):
        """'open' a repository for this dir."""
        from bzrlib.plugins.hg.repository import (
            HgLocalRepository,
            HgRemoteRepository,
            )
        if self._hgrepo.local():
            repo_klass = HgLocalRepository
        else:
            repo_klass = HgRemoteRepository
        return repo_klass(self._hgrepo, self, self._lockfiles)

    def open_workingtree(self, shared=False, recommend_upgrade=False):
        """'open' a workingtree for this dir."""
        from bzrlib.plugins.hg.workingtree import HgWorkingTree
        return HgWorkingTree(self._hgrepo, self.open_branch(), self,
                             self._lockfiles)

    def cloning_metadir(self, stacked=False):
        return bzrlib.bzrdir.format_registry.make_bzrdir("default-rich-root")


class HgToSomethingConverter(bzrlib.bzrdir.Converter):
    """A class to upgrade an hg dir to something else."""

    def __init__(self, format):
        self.format = format
        if self.format is None:
            self.format = bzrlib.bzrdir.BzrDirFormat.get_default_format()

    def convert(self, bzrdir, pb):
        source_repo = bzrdir.open_repository()
        source_branch = bzrdir.open_branch()
        target = self.format.initialize_on_transport(bzrdir.root_transport)
        target_repo = target.create_repository()
        target_repo.fetch(source_repo, pb=pb)
        target_branch = target.create_branch()
        target_branch.generate_revision_history(source_branch.last_revision())
        target_wt = target.create_workingtree()
        bzrdir.root_transport.delete_tree(".hg")
        return target


class HgBzrDirFormat(bzrlib.bzrdir.BzrDirFormat):
    """The .hg directory control format."""

    def get_converter(self, format):
        """We should write a converter."""
        return HgToSomethingConverter(format)

    def network_name(self):
        return "hg"

    def get_format_description(self):
        return "Mercurial Branch"

    def initialize_on_transport(self, transport):
        """Initialize a new .not dir in the base directory of a Transport."""
        return self.open(transport, _create=True)

    def __eq__(self, other):
        return type(self) == type(other)

    @classmethod
    def _known_formats(self):
        return set([HgBzrDirFormat()])

    def open(self, transport, _create=False, _found=None):
        """Open this directory.

        :param _create: create the hg dir on the fly. private to HgBzrDirFormat.
        """
        # we dont grok readonly - hg isn't integrated with transport.
        if transport.base.startswith('readonly+'):
            transport = transport._decorated
        if transport.base.startswith('file://'):
            path = transport.local_abspath('.').encode('utf-8')
            lock_class = HgLock
        else:
            path = transport.base
            lock_class = HgDummyLock
        lazy_load_mercurial()
        import mercurial.hg
        from bzrlib.plugins.hg.ui import ui
        repository = mercurial.hg.repository(ui(), path, create=_create)
        lockfiles = HgLockableFiles(lock_class(repository), transport)
        return HgDir(repository, transport, lockfiles, self)

    @classmethod
    def probe_transport(klass, transport):
        """Our format is present if the transport ends in '.not/'."""
        # little ugly, but works
        format = klass()
        from bzrlib.transport.local import LocalTransport
        lazy_load_mercurial()
        from mercurial import error as hg_errors
        if isinstance(transport, LocalTransport) and not transport.has(".hg"):
            # Explicitly check for .hg directories here, so we avoid
            # loading foreign branches through Mercurial.
            raise errors.NotBranchError(path=transport.base)
        import urllib2
        try:
            format.open(transport)
        except hg_errors.RepoError, e:
            raise errors.NotBranchError(path=transport.base)
        except hg_errors.Abort, e:
            trace.mutter('not a hg branch: %s', e)
            raise errors.NotBranchError(path=transport.base)
        except urllib2.HTTPError, e:
            trace.mutter('not a hg branch: %s', e)
            raise errors.NotBranchError(path=transport.base)
        return format


bzrlib.bzrdir.BzrDirFormat.register_control_format(HgBzrDirFormat)

bzrlib.bzrdir.format_registry.register("hg",
    HgBzrDirFormat, "Mercurial repository. ", native=False, hidden=False)

send_format_registry.register_lazy('hg', 'bzrlib.plugins.hg.send',
                                   'send_hg', 'Mecurial bundle format')

from bzrlib.revisionspec import revspec_registry
revspec_registry.register_lazy("hg:", "bzrlib.plugins.hg.revspec",
    "RevisionSpec_hg")

from bzrlib.commands import (
    plugin_cmds,
    )
plugin_cmds.register_lazy('cmd_hg_import', [], 'bzrlib.plugins.hg.commands')

try:
    from bzrlib.revisionspec import dwim_revspecs
except ImportError:
    pass
else:
    from bzrlib.plugins.hg.revspec import RevisionSpec_hg
    dwim_revspecs.append(RevisionSpec_hg)


def test_suite():
    from unittest import TestSuite, TestLoader
    from bzrlib.plugins.hg import tests

    suite = TestSuite()

    suite.addTest(tests.test_suite())

    return suite


# TODO: test that a last-modified in a merged branch is correctly assigned
# TODO: test that we set the per file parents right: that is the rev of the
# last heads, *not* the revs of the revisions parents. (note, this should be
# right already, because if the use of find_previous_heads, but surety is nice.)
# TODO: test that the assignment of .revision to directories works correctly
# in the following case:
# branch A adds dir/file in rev X
# branch B adds dir/file2 in rev Y
# branch B merges branch A: there are *two* feasible revisions that both claim
# to have created 'dir' - X and Y. We want to always choose lower-sorting one.
# Its potentially impossible to guarantee a consistent choice otherwise, and
# having the dir flip-flop in bzr would be unhelpful. This choice is what I
# think has been implemented, but not having got hg merges operating from
# within python yet, its quite hard to produce this scenario to test.
# TODO: file version extraction should elide 'copy' and 'copyrev file headers.
