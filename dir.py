# Copyright (C) 2005-2011 Canonical Ltd
# Copyright (C) 2008-2010 Jelmer Vernooij <jelmer@samba.org>
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

"""Mercurial control directory support.

"""

import bzrlib.bzrdir
import bzrlib.lockable_files
from bzrlib import (
    errors,
    )

from bzrlib.controldir import (
    ControlDir,
    ControlDirFormat,
    )
try:
    from bzrlib.controldir import Converter
except ImportError: # bzr < 2.4
    from bzrlib.bzrdir import Converter
from bzrlib.plugins.hg import (
    lazy_load_mercurial,
    )


class HgControlDirConfig(object):

    def get_default_stack_on(self):
        return None

    def set_default_stack_on(self, value):
        raise errors.BzrError("Cannot set configuration")


class HgDir(ControlDir):
    """An adapter to the '.hg' dir used by mercurial."""

    @property
    def user_transport(self):
        return self.root_transport

    @property
    def control_transport(self):
        return self.transport

    def is_control_filename(self, filename):
        return (filename == ".hg")

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
        raise errors.UnsupportedOperation(self.destroy_branch, self)

    def destroy_workingtree(self):
        raise errors.UnsupportedOperation(self.destroy_workingtree, self)

    def destroy_repository(self):
        raise errors.UnsupportedOperation(self.destroy_repository, self)

    def get_branch_transport(self, branch_format, name=None):
        if branch_format is None:
            return self.transport
        if isinstance(branch_format, HgControlDirFormat):
            return self.transport
        raise errors.IncompatibleFormat(branch_format, self._format)

    get_repository_transport = get_branch_transport
    get_workingtree_transport = get_branch_transport

    def is_supported(self):
        return True

    def needs_format_conversion(self, format=None):
        return (format is not HgControlDirFormat)

    def open_branch(self, name=None, unsupported=False,
            ignore_fallbacks=False):
        """'create' a branch for this dir."""
        if name is None:
            name = 'default'
        from bzrlib.plugins.hg.branch import HgLocalBranch, HgRemoteBranch
        if self._hgrepo.local():
            branch_klass = HgLocalBranch
        else:
            branch_klass = HgRemoteBranch
        return branch_klass(self._hgrepo, name, self, self._lockfiles)

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

    def get_config(self):
        return HgControlDirConfig()


class HgToSomethingConverter(Converter):
    """A class to upgrade an hg dir to something else."""

    def __init__(self, format):
        self.format = format
        if self.format is None:
            self.format = ControlDirFormat.get_default_format()

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


LockWarner = bzrlib.lockable_files._LockWarner


class HgLockableFiles(bzrlib.lockable_files.LockableFiles):
    """Hg specific lockable files abstraction."""

    def __init__(self, lock, transport):
        self._lock = lock
        self._transaction = None
        self._lock_mode = None
        self._transport = transport
        self._lock_warner = LockWarner(repr(self))


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


class HgControlDirFormat(ControlDirFormat):
    """The .hg directory control format."""

    colocated_branches = True

    def __init__(self):
        super(HgControlDirFormat, self).__init__()
        self.workingtree_format = None

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
        return set([HgControlDirFormat()])

    def open(self, transport, _create=False, _found=None):
        """Open this directory.

        :param _create: create the hg dir on the fly. private to
            HgControlDirFormat.
        """
        # we dont grok readonly - hg isn't integrated with transport.
        try:
            url = transport.external_url()
        except errors.InProcessTransport:
            raise errors.NotBranchError(transport.base)
        if url.startswith('file://'):
            path = transport.local_abspath('.').encode('utf-8')
            lock_class = HgLock
        else:
            path = url
            lock_class = HgDummyLock
        lazy_load_mercurial()
        import mercurial.hg
        from bzrlib.plugins.hg.ui import ui
        repository = mercurial.hg.repository(ui(), path, create=_create)
        lockfiles = HgLockableFiles(lock_class(repository), transport)
        return HgDir(repository, transport, lockfiles, self)
