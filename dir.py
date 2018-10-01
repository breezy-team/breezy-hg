# Copyright (C) 2005-2011 Canonical Ltd
# Copyright (C) 2008-2010 Jelmer Vernooij <jelmer@jelmer.uk>
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

import breezy.controldir
from breezy import (
    errors,
    lock,
    transactions,
    urlutils,
    )

from breezy.controldir import (
    ControlDir,
    ControlDirFormat,
    )
from breezy.decorators import (
    only_raises,
    )
from breezy.controldir import Converter
from breezy.plugins.hg import (
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
        return (filename == ".hg" or filename.startswith(".hg/"))

    def __init__(self, hgrepo, transport, lockfiles, format):
        self._format = format
        self.root_transport = transport
        self.transport = transport.clone('.hg')
        self._hgrepo = hgrepo
        self._lockfiles = lockfiles

    def backup_controldir(self):
        self.root_transport.copy_tree(".hg", ".hg.backup")
        return (self.root_transport.abspath(".hg"),
                self.root_transport.abspath(".hg.backup"))

    def break_lock(self):
        """Mercurial locks never break."""
        raise NotImplementedError(self.break_lock)

    def clone_on_transport(self, transport, revision_id=None,
        force_new_repo=False, preserve_stacking=False, stacked_on=None,
        create_prefix=False, use_existing_dir=True, no_tree=False):
        """See ControlDir.clone_on_transport."""
        path = transport.local_abspath(".")
        result = self._format.initialize(path)
        result._hgrepo.pull(self._hgrepo)
        return result

    def create_branch(self, name=None, repository=None,
                      append_revisions_only=None):
        """'create' a branch for this dir."""
        if append_revisions_only:
            raise errors.UpgradeRequired(self.base)
        name = self._get_branch_name(name)
        return self.open_branch(name=name)

    def create_repository(self, shared=False):
        """'create' a repository for this dir."""
        if shared:
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

    def set_branch_reference(self, target_branch, name=None):
        # Sorry, no branch references.
        raise errors.IncompatibleFormat(target_branch._format, self._format)

    def _find_or_create_repository(self, force_new_repo):
        """Create a new repository if needed, returning the repository."""
        return self.open_repository()

    def destroy_branch(self, name=None):
        name = self._get_branch_name(name)
        raise errors.UnsupportedOperation(self.destroy_branch, self)

    def destroy_workingtree(self):
        raise errors.UnsupportedOperation(self.destroy_workingtree, self)

    def destroy_repository(self):
        raise errors.UnsupportedOperation(self.destroy_repository, self)

    def is_supported(self):
        return True

    def needs_format_conversion(self, format=None):
        return (format is not HgControlDirFormat)

    def _get_branch_name(self, name=None):
        if name is None and getattr(self, "_get_selected_branch", False):
            name = self._get_selected_branch()
        if not name:
            name = 'default'
        return name

    def open_branch(self, name=None, unsupported=False,
            ignore_fallbacks=False, possible_transports=None):
        """'create' a branch for this dir."""
        name = self._get_branch_name(name)
        from breezy.plugins.hg.branch import HgLocalBranch, HgRemoteBranch
        if self._hgrepo.local():
            branch_klass = HgLocalBranch
        else:
            branch_klass = HgRemoteBranch
        return branch_klass(self._hgrepo, name, self, self._lockfiles)

    def find_repository(self, shared=False):
        return self.open_repository(shared)

    def open_repository(self, shared=False):
        """'open' a repository for this dir."""
        from breezy.plugins.hg.repository import (
            HgLocalRepository,
            HgRemoteRepository,
            )
        if self._hgrepo.local():
            repo_klass = HgLocalRepository
        else:
            repo_klass = HgRemoteRepository
        return repo_klass(self._hgrepo, self, self._lockfiles)

    def open_workingtree(self, shared=False, recommend_upgrade=False,
                         unsupported=False):
        """'open' a workingtree for this dir."""
        from breezy.plugins.hg.workingtree import HgWorkingTree
        return HgWorkingTree(self._hgrepo, self.open_branch(), self)

    def cloning_metadir(self, stacked=False):
        return breezy.controldir.format_registry.make_controldir("default-rich-root")

    def get_config(self):
        return HgControlDirConfig()

    def sprout(self, url, revision_id=None, force_new_repo=False,
               recurse='down', possible_transports=None,
               accelerator_tree=None, hardlink=False, stacked=False,
               source_branch=None, create_tree_if_local=True):
        from breezy.repository import InterRepository
        from breezy.transport.local import LocalTransport
        from breezy.transport import get_transport
        target_transport = get_transport(url, possible_transports)
        target_transport.ensure_base()
        cloning_format = self.cloning_metadir()
        # Create/update the result branch
        result = cloning_format.initialize_on_transport(target_transport)
        source_branch = self.open_branch()
        source_repository = self.find_repository()
        try:
            result_repo = result.find_repository()
        except errors.NoRepositoryPresent:
            result_repo = result.create_repository()
            target_is_empty = True
        else:
            target_is_empty = None # Unknown
        if stacked:
            raise errors.IncompatibleRepositories(source_repository, result_repo)
        interrepo = InterRepository.get(source_repository, result_repo)
        interrepo.fetch(revision_id=revision_id)
        result_branch = source_branch.sprout(result,
            revision_id=revision_id, repository=result_repo)
        if (create_tree_if_local and isinstance(target_transport, LocalTransport)
            and (result_repo is None or result_repo.make_working_trees())):
            wt = result.create_workingtree(accelerator_tree=accelerator_tree,
                hardlink=hardlink, from_branch=result_branch)
            with wt.lock_write():
                if wt.path2id('') is None:
                    try:
                        wt.set_root_id(self.open_workingtree.get_root_id())
                    except errors.NoWorkingTree:
                        pass
        return result


class HgToSomethingConverter(Converter):
    """A class to upgrade an hg dir to something else."""

    def __init__(self, format):
        self.format = format
        if self.format is None:
            self.format = ControlDirFormat.get_default_format()

    def convert(self, controldir, pb):
        source_repo = controldir.open_repository()
        source_branch = controldir.open_branch()
        target = self.format.initialize_on_transport(controldir.root_transport)
        target_repo = target.create_repository()
        target_repo.fetch(source_repo, pb=pb)
        target_branch = target.create_branch()
        target_branch.generate_revision_history(source_branch.last_revision())
        target_wt = target.create_workingtree()
        controldir.root_transport.delete_tree(".hg")
        return target


class HgLock(object):

    def __init__(self, transport, hgrepo, supports_read_lock):
        self._transport = transport
        self._hgrepo = hgrepo
        self._lock_mode = None
        self._lock_count = 0
        self._supports_read_lock = supports_read_lock

    def lock_write(self, token=None):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)
        if self._lock_mode is not None:
            if self._lock_mode != 'w':
                raise errors.ReadOnlyError(self)
            self._lock_count += 1
        else:
            self._lock_mode = 'w'
            self._lock = self._hgrepo.wlock(wait=False)
            self._transaction = transactions.PassThroughTransaction()
            self._lock_count = 1

    def lock_read(self):
        if self._lock_mode:
            if self._lock_mode not in ('r', 'w'):
                raise ValueError("invalid lock mode %r" % (self._lock_mode,))
            self._lock_count += 1
        else:
            self._lock_mode = 'r'
            if self._supports_read_lock:
                self._lock = self._hgrepo.lock()
            else:
                self._lock = None
            self._transaction = transactions.PassThroughTransaction()
            self._lock_count = 1

    def break_lock(self):
        raise NotImplementedError(self.break_lock)

    def is_locked(self):
        return (self._lock_count > 0)

    def peek(self):
        raise NotImplementedError(self.peek)

    def get_physical_lock_status(self):
        return None

    @only_raises(errors.LockNotHeld, errors.LockBroken)
    def unlock(self):
        if not self._lock_mode:
            return lock.cant_unlock_not_held(self)
        if self._lock_count > 1:
            self._lock_count -= 1
        else:
            if self._lock is not None:
                self._lock.release()
            self._lock_mode = self._lock_count = None
            self._transaction = None

    def validate_token(self, token):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)

    def get_transaction(self):
        """Return the current active transaction.

        If no transaction is active, this returns a passthrough object
        for which all data is immediately flushed and no caching happens.
        """
        if self._transaction is not None:
            return self._transaction
        else:
            return transactions.PassThroughTransaction()


class HgControlDirFormat(ControlDirFormat):
    """The .hg directory control format."""

    colocated_branches = True
    fixed_components = True

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

    def initialize_on_transport_ex(self, transport, use_existing_dir=False,
        create_prefix=False, force_new_repo=False, stacked_on=None,
        stack_on_pwd=None, repo_format_name=None, make_working_trees=None,
        shared_repo=False, vfs_only=False):
        from breezy import trace
        from breezy.controldir import CreateRepository
        from breezy.transport import do_catching_redirections
        def make_directory(transport):
            transport.mkdir('.')
            return transport
        def redirected(transport, e, redirection_notice):
            trace.note(redirection_notice)
            return transport._redirected_to(e.source, e.target)
        try:
            transport = do_catching_redirections(make_directory, transport,
                redirected)
        except errors.FileExists:
            if not use_existing_dir:
                raise
        except errors.NoSuchFile:
            if not create_prefix:
                raise
            transport.create_prefix()
        controldir = self.initialize_on_transport(transport)
        repository = controldir.open_repository()
        repository.lock_write()
        return (repository, controldir, False, CreateRepository(controldir))

    def get_branch_format(self):
        from breezy.plugins.hg.branch import HgBranchFormat
        return HgBranchFormat()

    @property
    def repository_format(self):
        from breezy.plugins.hg.repository import HgRepositoryFormat
        return HgRepositoryFormat()

    def initialize_on_transport(self, transport):
        """Initialize a new .not dir in the base directory of a Transport."""
        return self.open(transport, _create=True)

    def __eq__(self, other):
        return type(self) == type(other)

    def __hash__(self):
        return hash((self.__class__,))

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
        url = urlutils.split_segment_parameters(url)[0]
        try:
            path = transport.local_abspath('.').encode('utf-8')
        except errors.NotLocalUrl:
            path = url.rstrip("/")
            supports_read_lock = False
        else:
            supports_read_lock = True
        lazy_load_mercurial()
        import mercurial.hg
        from breezy.plugins.hg.ui import ui
        repository = mercurial.hg.repository(ui(), path, create=_create)
        lock = HgLock(transport, repository, supports_read_lock)
        return HgDir(repository, transport, lock, self)

    def supports_transport(self, transport):
        try:
            external_url = transport.external_url()
        except errors.InProcessTransport:
            raise errors.NotBranchError(path=transport.base)
        from breezy.plugins.hg import HgProber
        scheme = external_url.split(":")[0]
        if scheme not in HgProber._supported_schemes:
            return False
        return True
