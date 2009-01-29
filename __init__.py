# Copyright (C) 2005, 2006 Canonical Ltd
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


"""Hg support for bzr.

hg and bzr have different restrictions placed n revision ids. Its not possible
with the current hg model to allow bzr to write sanely to hg. However its
possibe to get bzr data out of hg.

The key translations needed are:
 * in bzr we use a revision id of "hg:" + hex(hgrevid) to ensure we can 
   always utf8 encode it.
 * we convert manifests to inventories on the fly.
"""

import copy
import os
import stat

try:
    from mercurial import demandimport
    demandimport.enable = lambda: None
except ImportError:
    pass

import mercurial.node
import mercurial.ui

import bzrlib.branch
import bzrlib.bzrdir
from bzrlib.decorators import *
import bzrlib.errors as errors
from bzrlib.foreign import ForeignRevision
from bzrlib.inventory import Inventory
import bzrlib.lockable_files
from bzrlib.osutils import split_lines, sha_strings
import bzrlib.repository
from bzrlib.transport.local import LocalTransport
from bzrlib.tsort import topo_sort
import bzrlib.urlutils as urlutils
import bzrlib.workingtree

from bzrlib.plugins.hg.mapping import default_mapping, mapping_registry
from bzrlib.foreign import foreign_vcs_registry

foreign_vcs_registry.register_lazy("hg", 
    "bzrlib.plugins.hg.mapping", "foreign_hg", "Mercurial")

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


class HgLockableFiles(bzrlib.lockable_files.LockableFiles):
    """Hg specific lockable files abstraction."""

    def __init__(self, lock, transport):
        self._lock = lock
        self._transaction = None
        self._lock_mode = None
        self._lock_count = 0
        self._transport = transport


class HgDir(bzrlib.bzrdir.BzrDir):
    """An adapter to the '.hg' dir used by mercurial."""

    def __init__(self, hgrepo, transport, lockfiles, format):
        self._format = format
        self.root_transport = transport
        self.transport = transport.clone('.hg')
        self._hgrepo = hgrepo
        self._lockfiles = lockfiles

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

    def create_branch(self):
        """'crate' a branch for this dir."""
        return HgBranch(self._hgrepo, self, self._lockfiles)

    def create_repository(self, shared=False):
        """'create' a repository for this dir."""
        if shared:
            # dont know enough about hg yet to do 'shared repositories' in it.
            raise errors.IncompatibleFormat(self._format, self._format)
        return HgRepository(self._hgrepo, self, self._lockfiles)

    def create_workingtree(self, shared=False):
        """'create' a workingtree for this dir."""
        return HgWorkingTree(self._hgrepo, self, self._lockfiles)

    def get_branch_transport(self, branch_format):
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
        return True

    def open_branch(self, ignored=None):
        """'crate' a branch for this dir."""
        return HgBranch(self._hgrepo, self, self._lockfiles)

    def open_repository(self, shared=False):
        """'open' a repository for this dir."""
        return HgRepository(self._hgrepo, self, self._lockfiles)

    def open_workingtree(self, shared=False, recommend_upgrade=False):
        """'open' a workingtree for this dir."""
        return HgWorkingTree(self._hgrepo, self, self._lockfiles)


class HgBzrDirFormat(bzrlib.bzrdir.BzrDirFormat):
    """The .hg directory control format."""

    def get_converter(self):
        """We should write a converter."""
        return HgToSomethingConverter()

    def get_format_description(self):
        return "Mercurial Branch"

    def initialize_on_transport(self, transport):
        """Initialize a new .not dir in the base directory of a Transport."""
        return self.open(transport, _create=True)

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
        else:
            raise errors.BzrCommandError('cannot use hg on %s transport' % transport)
        ui = mercurial.ui.ui()
        import mercurial.hg
        repository = mercurial.hg.repository(ui, path, create=_create)
        lockfiles = HgLockableFiles(HgLock(repository), transport)
        return HgDir(repository, transport, lockfiles, self)

    @classmethod
    def probe_transport(klass, transport):
        """Our format is present if the transport ends in '.not/'."""
        # little ugly, but works
        format = klass() 
        # try a manual probe first, its a little faster perhaps ?
        if transport.has('.hg'):
            return format
        # delegate to the main opening code. This pays a double rtt cost at the
        # moment, so perhaps we want probe_transport to return the opened thing
        # rather than an openener ? or we could return a curried thing with the
        # dir to open already instantiated ? Needs more thought.
        try:
            format.open(transport)
            return format
        except Exception, e:
            raise errors.NotBranchError(path=transport.base)
        raise errors.NotBranchError(path=transport.base)


bzrlib.bzrdir.BzrDirFormat.register_control_format(HgBzrDirFormat)


class HgToSomethingConverter(bzrlib.bzrdir.Converter):
    """A class to upgrade an hg dir to something else."""


class FromHgRepository(bzrlib.repository.InterRepository):
    """Hg to any repository actions."""

    @classmethod
    def _get_repo_format_to_test(self):
        """The formate to test with - as yet there is no HgRepoFormat."""
        return None

    @staticmethod
    def _get_repo_format_to_test():
        return None

    @needs_write_lock
    def copy_content(self, revision_id=None, basis=None):
        """See InterRepository.copy_content. Partial implementation of that.

        To date the revision_id and basis parameters are not supported.
        """
        assert revision_id is None
        assert basis is None
        self.target.fetch(self.source)

    @needs_write_lock
    def fetch(self, revision_id=None, pb=None, find_ghosts=False):
        """Fetch revisions. This is a partial implementation."""
        # assumes that self is a bzr compatible tree, and that source is hg
        # pull everything for simplicity.
        # TODO: make this somewhat faster - calculate the specific needed
        # file versions and then pull all of those per file, followed by
        # inserting the inventories and revisions, rather than doing 
        # rev-at-a-time.
        needed = {}
        if revision_id is None:
            pending = set()
            for revision_id in self.source._hgrepo.changelog.heads():
                pending.add(self.get_mapping().revision_id_foreign_to_bzr(revision_id))
        else:
            # add what can be reached from revision_id
            pending = set([revision_id])
        # plan it.
        # we build a graph of the revisions we need, and a
        # full graph which we use for topo-sorting (we need a partial-
        # topo-sorter done to avoid this. TODO: a partial_topo_sort.)
        # so needed is the revisions we need, and needed_graph is the entire
        # graph, using 'local' sources for establishing the graph where
        # possible. TODO: also, use the bzr knit graph facility to seed the
        # graph once we encounter revisions we know about.
        needed_graph = {}
        while len(pending) > 0:
            node = pending.pop()
            if self.target.has_revision(node):
                parent_ids = self.target.get_revision(node).parent_ids
            else:
                needed[node] = self.source.get_revision(node)
                parent_ids = needed[node].parent_ids
            needed_graph[node] = parent_ids
            for revision_id in parent_ids:
                if revision_id not in needed_graph:
                    pending.add(revision_id)
        target_repo = self.target
        target_transaction = self.target.get_transaction()
        order = topo_sort(needed_graph.items())
        # order is now too aggressive: filter to just what we need:
        order = [rev_id for rev_id in order if rev_id in needed]
        total = len(order)
        inventories = {}
        pb = bzrlib.ui.ui_factory.nested_progress_bar()
        try:
            for index, revision_id in enumerate(order):
                pb.update('fetching revisions', index, total)
                revision = needed[revision_id]
                inventory = self.source.get_inventory(revision_id)
                inventories[revision_id] = inventory
                hgrevid, mapping = mapping_registry.revision_id_bzr_to_foreign(revision_id)
                log = self.source._hgrepo.changelog.read(hgrevid)
                manifest = self.source._hgrepo.manifest.read(log[0])
                for fileid in inventory:
                    if fileid == bzrlib.inventory.ROOT_ID:
                        continue
                    entry = inventory[fileid]
                    if inventory[fileid].revision == revision_id:
                        # changed in this revision
                        entry = inventory[fileid]
                        # changing the parents-to-insert-as algorithm here will
                        # cause pulls from hg to change the per-file graph.
                        # BEWARE of doing that.
                        previous_inventories = []
                        for parent in revision.parent_ids:
                            try:
                                previous_inventories.append(inventories[parent])
                            except KeyError:
                                # if its not in the cache, its in target already
                                inventories[parent] = self.target.get_inventory(parent)
                                previous_inventories.append(inventories[parent])
                        file_heads = entry.find_previous_heads(
                            previous_inventories,
                            target_repo.weave_store,
                            target_transaction
                            )

                        weave = target_repo.weave_store.get_weave_or_empty(
                                    fileid, target_transaction)

                        if entry.kind == 'directory':
                            # a bit of an abstraction variation, but we dont have a
                            # real tree for entry to read from, and it would be 
                            # mostly dead weight to have a stub tree here.
                            weave.add_lines(revision_id, file_heads, [])
                        else:
                            # extract text and insert it.
                            path = inventory.id2path(fileid)
                            revlog = self.source._hgrepo.file(path)
                            filerev = manifest[path]
                            # TODO: perhaps we should use readmeta here to figure out renames ?
                            text = revlog.read(filerev)
                            lines = split_lines(text)
                            weave.add_lines(revision_id, file_heads, 
                                            split_lines(text))
                self.target.add_inventory(revision_id, inventory, 
                                          revision.parent_ids)
                self.target.add_revision(revision_id, revision)
        finally:
            pb.finished()
        return total, 0

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with HgRepositories."""
        return isinstance(source, HgRepository)

bzrlib.repository.InterRepository.register_optimiser(FromHgRepository)

def test_suite():
    from unittest import TestSuite, TestLoader
    import tests

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
