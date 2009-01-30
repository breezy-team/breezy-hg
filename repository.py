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

from bzrlib.decorators import needs_write_lock
from bzrlib.foreign import ForeignRevision, ForeignRepository
from bzrlib.inventory import Inventory
from bzrlib.osutils import sha_strings, split_lines
import bzrlib.repository
from bzrlib.revision import NULL_REVISION
from bzrlib.tsort import topo_sort

from bzrlib.plugins.hg.foreign import (
    versionedfiles,
    )
from bzrlib.plugins.hg.mapping import (
    default_mapping,
    mapping_registry,
    )

import mercurial.node


class HgRepositoryFormat(bzrlib.repository.RepositoryFormat):
    """Mercurial Repository Format.

    This is currently not aware of different repository formats,
    but simply relies on the installed copy of mercurial to 
    support the repository format.
    """

    def get_format_description(self):
        """See RepositoryFormat.get_format_description()."""
        return "Mercurial Repository"


class HgRepository(ForeignRepository):
    """An adapter to mercurial repositories for bzr."""

    _serializer = None

    def __init__(self, hgrepo, hgdir, lockfiles):
        ForeignRepository.__init__(self, HgRepositoryFormat(), hgdir, lockfiles)
        self._hgrepo = hgrepo
        self.base = hgdir.root_transport.base
        self._fallback_repositories = []
        self.texts = None
        self.signatures = versionedfiles.VirtualSignatureTexts(self)
        self.revisions = versionedfiles.VirtualRevisionTexts(self)

    def get_parent_map(self, revids):
        ret = {}
        for revid in revids:
            if revid == NULL_REVISION:
                ret[revid] = ()
            else:
                hg_ref, mapping = mapping_registry.revision_id_bzr_to_foreign(revid)
                parents = []
                for r in self._hgrepo.changelog.parents(hg_ref):
                    if r != "\0" * 20:
                        parents.append(mapping.revision_id_foreign_to_bzr(r))
                ret[revid] = tuple(parents)

        return ret

    def _check(self, revision_ids):
        # TODO: Call out to mercurial for consistency checking?
        return bzrlib.branch.BranchCheckResult(self)

    def get_mapping(self):
        return default_mapping # for now

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
        hgid = self.get_mapping().revision_id_foreign_to_bzr(revision_id)
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
            modified_revision = self.get_mapping().revision_id_foreign_to_bzr(good_id)
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
            introduced_at_path_revision = self.get_mapping().revision_id_foreign_to_bzr(good_id)
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

    def get_revisions(self, revids):
        return [self.get_revision(r) for r in revids]

    def get_revision(self, revision_id):
        hgrevid, mapping = mapping_registry.revision_id_bzr_to_foreign(revision_id)
        result = ForeignRevision(hgrevid, mapping, revision_id)
        hgchange = self._hgrepo.changelog.read(hgrevid)
        hgparents = self._hgrepo.changelog.parents(hgrevid)
        result.parent_ids = []
        if hgparents[0] != mercurial.node.nullid:
            result.parent_ids.append(mapping.revision_id_foreign_to_bzr(hgparents[0]))
        if hgparents[1] != mercurial.node.nullid:
            result.parent_ids.append(mapping.revision_id_foreign_to_bzr(hgparents[1]))
        result.message = hgchange[4].decode("utf-8")
        result.inventory_sha1 = ""
        result.timezone = -hgchange[2][1]
        result.timestamp = hgchange[2][0]
        result.committer = hgchange[1].decode("utf-8")
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
        return mapping_registry.revision_id_bzr_to_foreign(revision_id)[0] in self._hgrepo.changelog.nodemap

    def is_shared(self):
        """Whether this repository is being shared between multiple branches. 
        
        Always False for Mercurial for now.
        """
        return False


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
                pending.add(self.source.get_mapping().revision_id_foreign_to_bzr(revision_id))
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


