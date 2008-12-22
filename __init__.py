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

import mercurial.commands
import mercurial.cmdutil
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
from bzrlib.transport.local import LocalTransport
from bzrlib.tsort import topo_sort
import bzrlib.urlutils as urlutils
import bzrlib.workingtree


def hgrevid_from_bzr(revision_id):
    return bin(revision_id[3:])


def bzrrevid_from_hg(revision_id):
    return "hg:" + hex(revision_id)


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


class MercurialBranchConfig:
    """Hg branch configuration."""

    def __init__(self, branch):
        # TODO: Read .hgrc
        self.branch = branch


class HgRepositoryFormat(bzrlib.repository.RepositoryFormat):
    """Mercurial Repository Format.

    This is currently not aware of different repository formats,
    but simply relies on the installed copy of mercurial to 
    support the repository format.
    """

    def get_format_description(self):
        """See RepositoryFormat.get_format_description()."""
        return "Mercurial Repository"


class HgRepository(bzrlib.repository.Repository):
    """An adapter to mercurial repositories for bzr."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles
        self._format = HgRepositoryFormat()
        self.base = hgdir.root_transport.base

    def _check(self, revision_ids):
        # TODO: Call out to mercurial for consistency checking?
        return bzrlib.branch.BranchCheckResult(self)

    def get_inventory(self, revision_id):
        """Synthesize a bzr inventory from an hg manifest...

        how this works:
        we grab the manifest for revision_id
        we create an Inventory
        for each file in the manifest we:
            * if the dirname of the file is not in the inventory, we add it
              recursively, with an id of the path with / replaced by :, and a 
              prefix of 'hg:'. The directory gets a last-modified value of the
              topologically oldest file.revision value under it in the 
              inventory. In the event of multiple revisions with no topological
              winner - that is where there is more than one root, alpha-sorting
              is used as a tie-break.
            * use the files revlog to get the 'linkrev' of the file which 
              takes us to the revision id that introduced that revision. That
              revision becomes the revision_id in the inventory
            * check for executable status in the manifest flags
            * add an entry for the file, of type file, executable if needed,
              and an id of 'hg:path' with / replaced by :.
        """
        # TODO: this deserves either _ methods on HgRepository, or a method
        # object. Its too big!
        hgid = hgrevid_from_bzr(revision_id)
        log = self._hgrepo.changelog.read(hgid)
        manifest = self._hgrepo.manifest.read(log[0])
        all_relevant_revisions = self.get_revision_graph(revision_id)
        ancestry_cache = {}
        result = Inventory()
        # each directory is a key - i.e. 'foo'
        # the value is the current chosen revision value for it.
        # we walk up the hierarchy - when a dir changes .revision, its parent
        # must also change if the new value is older than the parents one.
        directories = {}
        def get_ancestry(some_revision_id):
            try:
                return ancestry_cache[some_revision_id]
            except KeyError:
                pass
            ancestry = set()
            # add what can be reached from some_revision_id
            # TODO: must factor this trivial iteration in bzrlib.graph cleanly.
            pending = set([some_revision_id])
            while len(pending) > 0:
                node = pending.pop()
                ancestry.add(node)
                for parent_id in all_relevant_revisions[node]:
                    if parent_id not in ancestry:
                        pending.add(parent_id)
            ancestry_cache[some_revision_id] = ancestry
            return ancestry
        def path_id(path):
            """Create a synthetic file_id for an hg file."""
            return "hg:" + path.replace('/', ':')
        def pick_best_creator_revision(revision_a, revision_b):
            """Picks the best creator revision from a and b.

            If a is an ancestor of b, a wins, and vice verca.
            If neither is an ancestor of the other, the lowest value wins.
            """
            # TODO make this much faster - use a local cache of the ancestry
            # sets.
            if revision_a in get_ancestry(revision_b):
                return revision_a
            elif revision_b in get_ancestry(revision_a):
                return revision_b
            elif revision_a < revision_b:
                return revision_a
            else:
                return revision_b
        def add_dir_for(file, file_revision_id):
            """ensure that file can be added by adding its parents.

            this is horribly inefficient at the moment, proof of concept.

            This is called for every path under each dir, and will update the
            .revision for it older each time as the file age is determined.
            """
            path = os.path.dirname(file)
            if path == '':
                # special case the root node.
                return
            if result.has_filename(path):
                # check for a new revision
                current_best = directories[path]
                new_best = pick_best_creator_revision(current_best, file_revision_id)
                if new_best != current_best:
                    # new revision found, push this up
                    # XXX could hand in our result as a hint?
                    add_dir_for(path, file_revision_id)
                    # and update our chosen one
                    directories[path] = file_revision_id
                return
            # the dir is not present. Add its parent too:
            add_dir_for(path, file_revision_id)
            # add the dir to the directory summary for creation detection
            directories[path] = file_revision_id
            # and put it in the inventory. The revision value is assigned later.
            entry = result.add_path(path, 'directory', file_id=path_id(path))
        # this can and should be tuned, but for now its just fine - its a 
        # proof of concept. add_path is part of the things to tune, as is
        # the dirname() calls.
        known_manifests = {}
        """manifests addressed by changelog."""
        for file, file_revision in manifest.items():
            revlog = self._hgrepo.file(file)
            changelog_index = revlog.linkrev(file_revision)

            # find when the file was modified. 
            # start with the manifest nodeid
            current_log = log
            # we should find all the tails, and then when there are > 2 heads
            # record a new revision id at the join. We can detect this by
            # walking out from each head and assigning ids to them, when two
            # parents have the same manifest assign a new id.
            # TODO currently we just pick *a* tail.
            file_tails = []
            current_manifest = manifest
            # cls - changelogs
            parent_cls = set(self._hgrepo.changelog.parents(hgid))
            good_id = hgid
            done_cls = set()
            # walk the graph, any node at a time to find the last change point.
            while parent_cls:
                current_cl = parent_cls.pop()
                # the nullid isn't useful.
                if current_cl == mercurial.node.nullid:
                    continue
                if current_cl not in known_manifests:
                    current_manifest_id = self._hgrepo.changelog.read(current_cl)[0]
                    known_manifests[current_cl] = self._hgrepo.manifest.read(
                        current_manifest_id)
                current_manifest = known_manifests[current_cl]
                done_cls.add(current_cl)
                if current_manifest.get(file, None) != file_revision:
                    continue
                # unchanged in parent, advance to the parent.
                good_id = current_cl
                for parent_cl in self._hgrepo.changelog.parents(current_cl):
                    if parent_cl not in done_cls:
                        parent_cls.add(parent_cl)
            modified_revision = bzrrevid_from_hg(good_id)
            # dont use the following, it doesn't give the right results consistently.
            # modified_revision = bzrrevid_from_hg(
            #     self._hgrepo.changelog.index[changelog_index][7])
            # now walk to find the introducing revision.
            parent_cl_ids = set([(None, hgid)])
            good_id = hgid
            done_cls = set()
            while parent_cl_ids:
                current_cl_id_child, current_cl_id = parent_cl_ids.pop()
                # the nullid isn't useful.
                if current_cl_id == mercurial.node.nullid:
                    continue
                if current_cl_id not in known_manifests:
                    current_manifest_id = self._hgrepo.changelog.read(current_cl_id)[0]
                    known_manifests[current_cl_id] = self._hgrepo.manifest.read(
                        current_manifest_id)
                current_manifest = known_manifests[current_cl_id]
                done_cls.add(current_cl_id)
                if current_manifest.get(file, None) is None:
                    # file is not in current manifest: its a tail, cut here.
                    good_id = current_cl_id_child
                    continue
                # walk to the parents
                if (mercurial.node.nullid, mercurial.node.nullid) == self._hgrepo.changelog.parents(current_cl_id):
                    # we have reached the root:
                    good_id = current_cl_id
                    continue
                for parent_cl in self._hgrepo.changelog.parents(current_cl_id):
                    if parent_cl not in done_cls:
                        parent_cl_ids.add((current_cl_id, parent_cl))
            introduced_at_path_revision = bzrrevid_from_hg(good_id)
            add_dir_for(file, introduced_at_path_revision)
            entry = result.add_path(file, 'file', file_id=path_id(file))
            entry.text_size = revlog.size(revlog.nodemap[file_revision])
            # its a shame we need to pull the text out. is there a better way?
            # TODO: perhaps we should use readmeta here to figure out renames ?
            text = revlog.read(file_revision)
            entry.text_sha1 = sha_strings(text)
            if manifest.execf(file):
                entry.executable = True
            entry.revision = modified_revision
        for dir, dir_revision_id in directories.items():
            dirid = path_id(dir)
            result[dirid].revision = dir_revision_id
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
        result.timezone = -hgchange[2][1]
        result.timestamp = hgchange[2][0]
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

    def is_shared(self):
        """Whether this repository is being shared between multiple branches. 
        
        Always False for Mercurial for now.
        """
        return False


class HgBranchFormat(bzrlib.branch.BranchFormat):
    """Mercurial Branch Format.

    This is currently not aware of different branch formats,
    but simply relies on the installed copy of mercurial to 
    support the branch format.
    """

    def get_format_description(self):
        """See BranchFormat.get_format_description()."""
        return "Mercurial Branch"


class HgBranchConfig(object):
    """Access Branch Configuration data for an HgBranch.

    This is not fully compatible with bzr yet - but it should be made so.
    """

    def __init__(self, branch):
        self._branch = branch

    def get_nickname(self):
<<<<<<< TREE
        return urlutils.unescape(self._branch.base.split('/')[-2])
=======
        # remove the trailing / and take the basename.
        return basename(self._branch.base[:-1])
>>>>>>> MERGE-SOURCE


class HgBranch(bzrlib.branch.Branch):
    """An adapter to mercurial repositories for bzr Branch objects."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        bzrlib.branch.Branch.__init__(self)
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles
        self.repository = HgRepository(hgrepo, hgdir, lockfiles)
        self.base = hgdir.root_transport.base
        self._format = HgBranchFormat()

    def _check(self):
        # TODO: Call out to mercurial for consistency checking?
        return bzrlib.branch.BranchCheckResult(self)

    def get_parent(self):
        """Return the URL of the parent branch."""
        # TODO: Obtain "repository default" 
        return None

    def get_physical_lock_status(self):
        return self.control_files.get_physical_lock_status()

    def get_push_location(self):
        """Return default push location of this branch."""
        # TODO: Obtain "repository default"
        return None

    def get_config(self):
        """See Branch.get_config().

        We return an HgBranchConfig, which is a stub class with little
        functionality.
        """
        return HgBranchConfig(self)

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
        revs.reverse()
        return revs

    @needs_read_lock
    def last_revision(self):
        # perhaps should escape this ?
        return bzrrevid_from_hg(self._hgrepo.changelog.tip())

    def lock_read(self):
        self.control_files.lock_read()

    def tree_config(self):
        return MercurialBranchConfig(self)
  
    def unlock(self):
        self.control_files.unlock()

    def copy_content_into(self, destination, revision_id=None):
        pass

    def clone(self, to_bzrdir, revision_id=None):
        # hg repositories can only clone into hg repos.
        assert isinstance(to_bzrdir, HgDir)
        # and have nothing to do as we follow the hg model.
        return to_bzrdir.open_branch()


class HgWorkingTreeFormat(bzrlib.workingtree.WorkingTreeFormat):
    """Working Tree format for Mercurial Working Trees.

    This is currently not aware of different working tree formats,
    but simply relies on the installed copy of mercurial to 
    support the working tree format.
    """

    def get_format_description(self):
        """See WorkingTreeFormat.get_format_description()."""
        return "Mercurial Working Tree"


class HgWorkingTree(bzrlib.workingtree.WorkingTree):
    """An adapter to mercurial repositories for bzr WorkingTree obejcts."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self._control_files = lockfiles
        self._branch = HgBranch(hgrepo, hgdir, lockfiles)
        self._format = HgWorkingTreeFormat()

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
<<<<<<< TREE
        self._hgrepo.commit([], message, None, None, wlock=self._control_files._lock)
=======
        files, matchfn, anypats = mercurial.cmdutil.matchpats(self._hgrepo)
        self._hgrepo.commit([], message, None, None, matchfn, wlock=self._control_files._lock)
>>>>>>> MERGE-SOURCE

#    def read_working_inventory(self):
#        """in hg terms, read the manifest."""

    def _reset_data(self):
        """Reset all cached data."""

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
    def fetch(self, revision_id=None, pb=None):
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
                pending.add(bzrrevid_from_hg(revision_id))
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
<<<<<<< TREE
    from unittest import TestSuite, TestLoader
    import tests

    suite = TestSuite()

    suite.addTest(tests.test_suite())

    return suite      
=======
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
        # in revisiontwo we add a new file to dir, which should not change
        # the revision_id on the inventory.
        self.build_tree(['hg/dir/d'])
        self.tree.add(['dir/d'])
        self.tree.commit('bar')
        self.revtwo_inventory = copy.deepcopy(revone_inventory)
        tip = self.tree.last_revision()
        entry = self.revtwo_inventory.add_path('dir/d', kind='file',
            file_id='hg:dir:d')
        entry.revision = tip
        entry.text_size = len('contents of hg/dir/d\n')
        entry.text_sha1 = "f48fc342f707bfb4711790e1813c0df4d44e1a23"
        self.revidtwo = tip
        #====== end revision two
        # in revision three, we reset the exec flag on 'b'
        os.chmod('hg/b', mode)
        self.tree.commit('reset mode on b')
        self.revthree_inventory = copy.deepcopy(self.revtwo_inventory)
        tip = self.tree.last_revision()
        # should be a new file revision with exec reset
        entry = self.revthree_inventory['hg:b']
        entry.revision = tip
        entry.executable = False
        self.revidthree = tip
        #====== end revision three
        # in revision four we change the file dir/c, which should not alter
        # the last-changed field for 'dir'.
        self.build_tree_contents([('hg/dir/c', 'new contents')])
        self.tree.commit('change dir/c')
        self.revfour_inventory = copy.deepcopy(self.revthree_inventory)
        tip = self.tree.last_revision()
        entry = self.revfour_inventory['hg:dir:c']
        entry.revision = tip
        entry.text_size = len('new contents')
        entry.text_sha1 = "7ffa72b76d5d66da37f4b614b7a822c01f23c183"
        self.revidfour = tip
        #====== end revision four

    def test_inventory_from_manifest(self):
        repo = self.tree.branch.repository
        left = self.revone_inventory
        right = repo.get_inventory(self.revidone)
        self.assertEqual(left._byid, right._byid)
        left = self.revtwo_inventory
        right = repo.get_inventory(self.revidtwo)
        self.assertEqual(left._byid, right._byid)
        left = self.revthree_inventory
        right = repo.get_inventory(self.revidthree)
        self.assertEqual(left._byid, right._byid)
        left = self.revfour_inventory
        right = repo.get_inventory(self.revidfour)
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

    def test_get_config_nickname(self):
        # the branch nickname should be hg because the test dir is called hg.
        self.assertEqual("hg", self.tree.branch.get_config().get_nickname())

    def test_has_revision(self):
        self.assertTrue(self.tree.branch.repository.has_revision(self.revidone))
        self.assertTrue(self.tree.branch.repository.has_revision(self.revidtwo))
        self.assertFalse(self.tree.branch.repository.has_revision('foo'))

    def test_pull_into_bzr(self):
        bzrtree = self.make_branch_and_tree('bzr')
        bzrtree.pull(self.tree.branch)
        self.assertFileEqual('contents of hg/a\n', 'bzr/a')
        self.assertFileEqual('contents of hg/b\n', 'bzr/b')
        self.assertFileEqual('new contents', 'bzr/dir/c')
        
>>>>>>> MERGE-SOURCE

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
