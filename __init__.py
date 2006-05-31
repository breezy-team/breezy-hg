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

import mercurial.commands
import mercurial.hg
import mercurial.node
from mercurial.node import hex, bin
import mercurial.ui

import bzrlib.branch
import bzrlib.bzrdir
from bzrlib.decorators import *
import bzrlib.errors as errors
from bzrlib.inventory import Inventory
import bzrlib.lockable_files
from bzrlib.osutils import split_lines, sha_strings
import bzrlib.repository
from bzrlib.revision import Revision
from bzrlib.tests import TestLoader, TestCaseWithTransport
from bzrlib.transport.local import LocalTransport
import bzrlib.workingtree


def hgrevid_from_bzr(revision_id):
    return bin(revision_id[3:])


def bzrrevid_from_hg(revision_id):
    return "hg:" + hex(revision_id)


class HgLock(object):
    """A lock that thunks through to Hg."""

    def __init__(self, hgrepo):
        self._hgrepo = hgrepo

    def lock_write(self):
        self._lock = self._hgrepo.wlock()

    def lock_read(self):
        self._lock = self._hgrepo.lock()

    def unlock(self):
        self._lock.release()


class HgLockableFiles(bzrlib.lockable_files.LockableFiles):
    """Hg specific lockable files abstraction."""

    def __init__(self, lock):
        self._lock = lock
        self._transaction = None
        self._lock_mode = None
        self._lock_count = 0


class HgRepository(bzrlib.repository.Repository):
    """An adapter to mercurial repositories for bzr."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles

    def get_inventory(self, revision_id):
        """Synthesize a bzr inventory from an hg manifest...

        how this works:
        we grab the manifest for revision_id
        we create an Inventory
        for each file in the manifest we:
            * if the dirname of the file is not in the inventory, we add it
              recursively, with an id of the path with / replaced by :, and a 
              prefix of 'hg:'.
            * use the files revlog to get the 'linkrev' of the file which 
              takes us to the revision id that introduced that revision. That
              revision becomes the revision_id in the inventory
            * check for executable status in the manifest flags
            * add an entry for the file, of type file, executable if needed,
              and an id of 'hg:path' with / replaced by :.
        """
        hgid = hgrevid_from_bzr(revision_id)
        log = self._hgrepo.changelog.read(hgid)
        manifest = self._hgrepo.manifest.read(log[0])
        manifest_flags = self._hgrepo.manifest.readflags(log[0])
        result = Inventory()
        def path_id(path):
            """Create a synthetic file_id for an hg file."""
            return "hg:" + path.replace('/', ':')
        def add_dir_for(file):
            """ensure that file can be added by adding its parents.

            this is horribly inefficient at the moment, proof of concept.
            """
            path = os.path.dirname(file)
            if result.has_filename(path):
                return
            # the dir is not present. Add its parent too:
            add_dir_for(path)
            entry = result.add_path(path, 'directory', file_id=path_id(path))
            # this causes every revision to see all the dirs as modified on
            # every commit, which is a BAD first approximation.
            # TODO: a much better on is to set the dir revision_id to the
            # oldest last-modified revision id of all the revision ids below
            # the directory - i.e. if you have foo/a @ rev-index-4 and
            # foo/bar/b @ rev-index-2, set the last-modified revision to 2.
            entry.revision = revision_id
        # this can and should be tuned, but for now its just fine - its a 
        # proof of concept. add_path is part of the things to tune, as is
        # the dirname() calls.
        for file, file_revision in manifest.items():
            revlog = self._hgrepo.file(file)
            changelog_index = revlog.linkrev(file_revision)
            modified_revision = self._hgrepo.changelog.index[changelog_index][7]
            add_dir_for(file)
            entry = result.add_path(file, 'file', file_id=path_id(file))
            entry.text_size = revlog.size(revlog.nodemap[file_revision])
            # its a shame we need to pull the text out. is there a better way?
            text = revlog.revision(file_revision)
            entry.text_sha1 = sha_strings(text)
            if manifest_flags[file]:
                entry.executable = True
            entry.revision = bzrrevid_from_hg(modified_revision)
        return result

    def get_revision(self, revision_id):
        result = Revision(revision_id)
        hgchange = self._hgrepo.changelog.read(hgrevid_from_bzr(revision_id))
        hgparents = self._hgrepo.changelog.parents(hgrevid_from_bzr(revision_id))
        result.parent_ids = []
        if hgparents[0] != mercurial.node.nullid:
            result.parent_ids.append(bzrrevid_from_hg(hgparents[0]))
        if hgparents[1] != mercurial.node.nullid:
            result.parent_ids.append(bzrrevid_from_hg(hgparents[1]))
        result.message = hgchange[4]
        result.inventory_sha1 = ""
        result.timestamp = hgchange[2][0]
        result.timezone = hgchange[2][1]
        result.committer = hgchange[1]
        return result

    def get_revision_graph(self, revision_id=None):
        if revision_id is None:
            raise NotImplementedError("get_revision_graph with no parents not implemented yet.")
        else:
            # add what can be reached from revision_id
            result = {}
            pending = set([revision_id])
            while len(pending) > 0:
                node = pending.pop()
                result[node] = self.get_revision(node).parent_ids
                for revision_id in result[node]:
                    if revision_id not in result:
                        pending.add(revision_id)
            return result
    
    def has_revision(self, revision_id):
        return hgrevid_from_bzr(revision_id) in self._hgrepo.changelog.nodemap


class HgBranch(bzrlib.branch.Branch):
    """An adapter to mercurial repositories for bzr Branch objects."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles
        self.repository = HgRepository(hgrepo, hgdir, lockfiles)

    def lock_write(self):
        self.control_files.lock_write()
    
    @needs_read_lock
    def revision_history(self):
        tip = hgrevid_from_bzr(self.last_revision())
        revs = []
        next_rev = tip
        while next_rev != mercurial.node.nullid:
            revs.append(bzrrevid_from_hg(next_rev))
            next_rev = self._hgrepo.changelog.parents(next_rev)[0]
        return revs

    @needs_read_lock
    def last_revision(self):
        # perhaps should escape this ?
        return bzrrevid_from_hg(self._hgrepo.changelog.tip())

    def lock_read(self):
        self.control_files.lock_read()

    def unlock(self):
        self.control_files.unlock()

    def copy_content_into(self, destination, revision_id=None):
        pass

    def clone(self, to_bzrdir, revision_id=None):
        # hg repositories can only clone into hg repos.
        assert isinstance(to_bzrdir, HgDir)
        # and have nothing to do as we follow the hg model.
        return to_bzrdir.open_branch()


class HgWorkingTree(bzrlib.workingtree.WorkingTree):
    """An adapter to mercurial repositories for bzr WorkingTree obejcts."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self._control_files = lockfiles
        self._branch = HgBranch(hgrepo, hgdir, lockfiles)

    @needs_write_lock
    def add(self, files, ids=None):
        # hg does not use ids, toss them away.
        if isinstance(files, basestring):
            files = [files]
        # hg does not canonicalise paths : make them absolute
        paths = [(file).encode('utf8') for file in files]
        self._hgrepo.add(paths, wlock=self._control_files._lock)

    @needs_write_lock
    def commit(self, message, *args, **kwargs):
        # TODO: selected file lists etc.
        files, matchfn, anypats = mercurial.commands.matchpats(self._hgrepo)
        self._hgrepo.commit([], message, None, None, matchfn, wlock=self._control_files._lock)

#    def read_working_inventory(self):
#        """in hg terms, read the manifest."""

    def unlock(self):
        """Overridden to avoid hashcache usage - hg manages that."""
        try:
            return self._control_files.unlock()
        finally:
            self.branch.unlock()


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

    def open_workingtree(self, shared=False):
        """'open' a workingtree for this dir."""
        return HgWorkingTree(self._hgrepo, self, self._lockfiles)


class HgBzrDirFormat(bzrlib.bzrdir.BzrDirFormat):
    """The .hg directory control format."""

    def get_converter(self):
        """We should write a converter."""
        return HgToSomethingConverter()

    def initialize_on_transport(self, transport):
        """Initialize a new .not dir in the base directory of a Transport."""
        return self.open(transport, _create=True)

    @classmethod
    def _known_formats(self):
        return set([HgBzrDirFormat()])

    def open(self, transport, _create=False, _found=None):
        """Open this directory.
        
        :param _create: create the hg dir on the fly. private to HgDirFormat.
        """
        # we dont grok readonly - hg isn't integrated with transport.
        url = transport.base
        if url.startswith('readonly+'):
            url = url[len('readonly+'):]
        if url.startswith('file://'):
            url = url[len('file://'):]
        url = url.encode('utf8')
        ui = mercurial.ui.ui()
        repository = mercurial.hg.repository(ui, url, create=_create)
        lockfiles = HgLockableFiles(HgLock(repository))
        return HgDir(repository, transport, lockfiles, self)

    @classmethod
    def probe_transport(klass, transport):
        """Our format is present if the transport ends in '.not/'."""
        if transport.has('.hg'):
            return klass()
        raise errors.NotBranchError(path=transport.base)


bzrlib.bzrdir.BzrDirFormat.register_control_format(HgBzrDirFormat)


class HgToSomethingConverter(bzrlib.bzrdir.Converter):
    """A class to upgrade an hg dir to something else."""


class InterHgRepository(bzrlib.repository.InterRepository):
    """Hg to Hg repository actions."""

    _matching_repo_format = None 
    """The formate to test with - as yet there is no HgRepoFormat."""

    @needs_write_lock
    def copy_content(self, revision_id=None, basis=None):
        """See InterRepository.copy_content. Partial implemnetaiton of that.

        To date the revision_id and basis parameters are not supported.
        """
        assert revision_id is None
        assert basis is None
        self.target.fetch(self.source)

    @needs_write_lock
    def fetch(self, revision_id=None, pb=None):
        """Fetch revisions. This is a partial implementation."""
        # assumes that self is a bzr compatible tree, and that source is hg
        # pull everything for simplicity.
        # TODO: make this somewhat faster - calculate the specific needed
        # file versions and then pull all of those per file, followed by
        # inserting the inventories and revisions, rather than doing 
        # rev-at-a-time.
        order = []
        needed = {}
        if revision_id is None:
            raise NotImplementedError("fetching of everything not yet implemented.")
        else:
            # add what can be reached from revision_id
            pending = set([revision_id])
        # plan
        while len(pending) > 0:
            node = pending.pop()
            if self.target.has_revision(node):
                continue
            order.append(node)
            needed[node] = self.source.get_revision(node)
            for revision_id in needed[node].parent_ids:
                if revision_id not in needed:
                    pending.add(revision_id)
        target_repo = self.target
        target_transaction = self.target.get_transaction()
        order.reverse()
        inventories = {}
        for revision_id in order:
            revision = needed[revision_id]
            inventory = self.source.get_inventory(revision_id)
            inventories[revision_id] = inventory
            hgrevid = hgrevid_from_bzr(revision_id)
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
                        previous_inventories.append(inventories[parent])
                    file_heads = entry.find_previous_heads(
                        previous_inventories,
                        target_repo.weave_store,
                        target_transaction
                        )

                    if entry.kind == 'directory':
                        # a bit of an abstraction variation, but we dont have a
                        # real tree for entry to read from, and it would be 
                        # mostly dead weight to have a stub tree here.
                        entry._add_text_to_weave([],
                            file_heads,
                            target_repo.weave_store,
                            target_transaction)
                    else:
                        # extract text and insert it.
                        path = inventory.id2path(fileid)
                        revlog = self.source._hgrepo.file(path)
                        filerev = manifest[path]
                        text = revlog.revision(filerev)
                        lines = split_lines(text)
                        entry._add_text_to_weave(
                            split_lines(text),
                            file_heads,
                            target_repo.weave_store,
                            target_transaction)
            self.target.add_inventory(revision_id, inventory, revision.parent_ids)
            self.target.add_revision(revision_id, revision)
        return len(order), 0

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with HgRepositories."""
        return (isinstance(source, HgRepository) or
            isinstance(target, HgRepository))

bzrlib.repository.InterRepository.register_optimiser(InterHgRepository)

def test_suite():
    return TestLoader().loadTestsFromName(__name__)


class TestPulling(TestCaseWithTransport):
    """Tests for pulling from hg to bzr."""

    def setUp(self):
        super(TestPulling, self).setUp()
        self.build_tree(['hg/', 'hg/a', 'hg/b', 'hg/dir/', 'hg/dir/c'])
        hgdir = HgBzrDirFormat().initialize('hg')
        self.tree = hgdir.open_workingtree()
        mode = os.lstat('hg/b').st_mode
        os.chmod('hg/b', mode | stat.S_IEXEC)
        # do not add 'dir' to ensure that we pickup dir/c anyway : if hg
        # changes it behaviour, we want this test to start failing.
        self.tree.add(['a', 'b', 'dir/c'])
        self.tree.commit('foo')
        revone_inventory = Inventory()
        tip = self.tree.last_revision()
        entry = revone_inventory.add_path('a', kind='file', file_id='hg:a')
        entry.revision = tip
        entry.text_size = len('contents of hg/a\n')
        entry.text_sha1 = "72bcea9d6cba6ee7d3241bfa0c5e54506ad81a94"
        entry = revone_inventory.add_path('b', kind='file', file_id='hg:b')
        entry.executable = True
        entry.revision = tip
        entry.text_size = len('contents of hg/b\n')
        entry.text_sha1 = "b4d0c22d126cd0afeeeffa62961fb47c0932835a"
        entry = revone_inventory.add_path('dir', kind='directory',
            file_id='hg:dir')
        entry.revision = tip
        entry = revone_inventory.add_path('dir/c', kind='file',
            file_id='hg:dir:c')
        entry.revision = tip
        entry.text_size = len('contents of hg/dir/c\n')
        entry.text_sha1 = "958be752affac0fee70471331b96fb3fc1809425"
        self.revone_inventory = revone_inventory
        self.revidone = tip
        #====== end revision one
        self.build_tree(['hg/dir/d'])
        self.tree.add(['dir/d'])
        self.tree.commit('bar')
        self.revtwo_inventory = copy.deepcopy(revone_inventory)
        tip = self.tree.last_revision()
        # FIXME: this line is to line up with the wrong dir-versioning logic.
        print self.tree.branch.repository.get_revision(tip).parent_ids
        self.revtwo_inventory['hg:dir'].revision = tip
        entry = self.revtwo_inventory.add_path('dir/d', kind='file',
            file_id='hg:dir:d')
        entry.revision = tip
        entry.text_size = len('contents of hg/dir/d\n')
        entry.text_sha1 = "f48fc342f707bfb4711790e1813c0df4d44e1a23"
        self.revidtwo = tip

    def test_inventory_from_manifest(self):
        repo = self.tree.branch.repository
        left = self.revone_inventory
        right = repo.get_inventory(self.revidone)
        self.assertEqual(left._byid, right._byid)
        left = self.revtwo_inventory
        right = repo.get_inventory(self.revidtwo)
        self.assertEqual(left._byid, right._byid)

    def test_initial_revision_from_changelog(self):
        converted_rev = self.tree.branch.repository.get_revision(self.revidone)
        self.assertEqual([], converted_rev.parent_ids)
        self.assertEqual({}, converted_rev.properties)
        self.assertEqual('foo', converted_rev.message)
        self.assertEqual(self.revidone, converted_rev.revision_id)
        # we dont have a serialised inventory to convert, and the inv sha1 is
        # of reducing meaning now.
        self.assertEqual("", converted_rev.inventory_sha1)
        self.assertNotEqual(None, converted_rev.timestamp)
        self.assertNotEqual(None, converted_rev.timezone)
        self.assertNotEqual(None, converted_rev.committer)

    def test_non_initial_revision_from_changelog(self):
        converted_rev = self.tree.branch.repository.get_revision(self.revidtwo)
        self.assertEqual([self.revidone], converted_rev.parent_ids)
        self.assertEqual({}, converted_rev.properties)
        self.assertEqual('bar', converted_rev.message)
        self.assertEqual(self.revidtwo, converted_rev.revision_id)
        # we dont have a serialised inventory to convert, and the inv sha1 is
        # of reducing meaning now.
        self.assertEqual("", converted_rev.inventory_sha1)
        self.assertNotEqual(None, converted_rev.timestamp)
        self.assertNotEqual(None, converted_rev.timezone)
        self.assertNotEqual(None, converted_rev.committer)
        print "baaaar"


    def test_has_revision(self):
        tip = self.tree.last_revision()
        self.assertTrue(self.tree.branch.repository.has_revision(tip))
        self.assertFalse(self.tree.branch.repository.has_revision('foo'))

    def test_pull_into_bzr(self):
        bzrtree = self.make_branch_and_tree('bzr')
        bzrtree.pull(self.tree.branch)
        self.assertFileEqual('contents of hg/a\n', 'bzr/a')
        self.assertFileEqual('contents of hg/b\n', 'bzr/b')
        self.assertFileEqual('contents of hg/dir/c\n', 'bzr/dir/c')
        

# TODO: test that a last-modified in a merged branch is correctly assigned
# TODO: test that a last-mofified some revisions back from tip is correctly
# assigned.
# TODO: test revid changes when file flags alter
# TODO: test and correct the assignment of revision to InventoryDir instances.
# TODO: test that we set the per file parents right: that is the rev of the
# last heads, *not* the revs of the revisions parents.
