# Copyright (C) 2005, 2006 Canonical Ltd
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
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

from bzrlib import (
    ui,
    )
from bzrlib.decorators import (
    needs_write_lock,
    )
from bzrlib.repository import (
    InterRepository,
    )
from bzrlib.tsort import (
    topo_sort,
    )
from bzrlib.versionedfile import (
    ChunkedContentFactory,
    FulltextContentFactory,
    )

from bzrlib.plugins.hg.mapping import (
    mapping_registry,
    )

class FromHgRepository(InterRepository):
    """Hg to any repository actions."""

    @classmethod
    def _get_repo_format_to_test(self):
        """The format to test with - as yet there is no HgRepoFormat."""
        return None

    @needs_write_lock
    def copy_content(self, revision_id=None, basis=None):
        """See InterRepository.copy_content. Partial implementation of that.

        To date the revision_id and basis parameters are not supported.
        """
        assert revision_id is None
        assert basis is None
        self.target.fetch(self.source)


class FromLocalHgRepository(FromHgRepository):
    """Local Hg repository to any repository actions."""

    def __init__(self, source, target):
        FromHgRepository.__init__(self, source, target)
        self._inventories = {}

    @needs_write_lock
    def fetch(self, revision_id=None, pb=None, find_ghosts=False, 
              fetch_spec=None):
        """Fetch revisions. This is a partial implementation."""
        # assumes that self is a bzr compatible tree, and that source is hg
        # pull everything for simplicity.
        # TODO: make this somewhat faster - calculate the specific needed
        # file versions and then pull all of those per file, followed by
        # inserting the inventories and revisions, rather than doing 
        # rev-at-a-time.
        needed = {}
        if revision_id is not None:
            # add what can be reached from revision_id
            pending = set([revision_id])
        elif fetch_spec is not None:
            pending = set(fetch_spec.heads)
        else:
            pending = set()
            for revision_id in self.source._hgrepo.changelog.heads():
                pending.add(self.source.get_mapping().revision_id_foreign_to_bzr(revision_id))
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
        order = topo_sort(needed_graph.items())
        # order is now too aggressive: filter to just what we need:
        order = [rev_id for rev_id in order if rev_id in needed]
        self.target.start_write_group()
        try:
            self._fetch_hg_revs(order, needed)
        finally:
            self.target.commit_write_group()

    def _get_inventories(self, revision_ids):
        ret = []
        for revid in revision_ids:
            try:
                ret.append(self._inventories[revid])
            except KeyError:
                # if its not in the cache, its in target already
                self._inventories[revid] = self.target.get_inventory(revid)
                ret.append(self._inventories[revid])
        return ret

    def _fetch_hg_rev(self, revision):
        inventory = self.source.get_inventory(revision.revision_id)
        self._inventories[revision.revision_id] = inventory
        hgrevid, mapping = mapping_registry.revision_id_bzr_to_foreign(revision.revision_id)
        log = self.source._hgrepo.changelog.read(hgrevid)
        manifest = self.source._hgrepo.manifest.read(log[0])
        for fileid in inventory:
            #if fileid == bzrlib.inventory.ROOT_ID:
            #    continue
            entry = inventory[fileid]
            if inventory[fileid].revision == revision.revision_id:
                # changed in this revision
                entry = inventory[fileid]
                # changing the parents-to-insert-as algorithm here will
                # cause pulls from hg to change the per-file graph.
                # BEWARE of doing that.
                previous_inventories = self._get_inventories(
                    revision.parent_ids)
                file_heads = entry.parent_candidates(
                    previous_inventories)

                if entry.kind == 'directory':
                    # a bit of an abstraction variation, but we dont have a
                    # real tree for entry to read from, and it would be 
                    # mostly dead weight to have a stub tree here.
                    records = [
                        ChunkedContentFactory(
                            (fileid, revision.revision_id),
                            tuple([(fileid, revid) for revid in file_heads]),
                            None,
                            [])]
                else:
                    # extract text and insert it.
                    path = inventory.id2path(fileid)
                    revlog = self.source._hgrepo.file(path)
                    filerev = manifest[path]
                    # TODO: perhaps we should use readmeta here to figure out renames ?
                    text = revlog.read(filerev)
                    records = [
                            FulltextContentFactory(
                                (fileid, revision.revision_id),
                                tuple([(fileid, revid) for revid in file_heads]),
                                None, text)]
                self.target.texts.insert_record_stream(records)
        inventory.revision_id = revision.revision_id
        inventory.root.revision = revision.revision_id # Yuck. FIXME
        self.target.add_inventory(revision.revision_id, inventory, 
                                  revision.parent_ids)
        self.target.add_revision(revision.revision_id, revision)

    def _fetch_hg_revs(self, order, revisions):
        total = len(order)
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for index, revision_id in enumerate(order):
                pb.update('fetching revisions', index, total)
                self._fetch_hg_rev(revisions[revision_id])
        finally:
            pb.finished()
        return total, 0

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with HgLocalRepositories."""
        from bzrlib.plugins.hg.repository import HgLocalRepository, HgRepository
        return (isinstance(source, HgLocalRepository) and 
                not isinstance(target, HgRepository))


class FromRemoteHgRepository(FromHgRepository):
    """Remote Hg repository to any repository actions."""

    @needs_write_lock
    def fetch(self, revision_id=None, pb=None, find_ghosts=False, 
              fetch_spec=None):
        """Fetch revisions. This is a partial implementation."""
        raise NotImplementedError(self.fetch)

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with HgRemoteRepositories."""
        from bzrlib.plugins.hg.repository import HgRemoteRepository, HgRepository
        return (isinstance(source, HgRemoteRepository) and
                not isinstance(target, HgRepository))


class InterHgRepository(FromHgRepository):

    @needs_write_lock
    def fetch(self, revision_id=None, pb=None, find_ghosts=False, 
              fetch_spec=None):
        """Fetch revisions. This is a partial implementation."""
        if revision_id is not None:
            raise NotImplementedError("revision_id argument not yet supported")
        if fetch_spec is not None:
            raise NotImplementedError("fetch_spec argument not yet supported")
        if self.target._hgrepo.local():
            self.target._hgrepo.pull(self.source._hgrepo)
        else:
            self.source._hgrepo.push(self.target._hgrepo)

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with HgRemoteRepositories."""
        from bzrlib.plugins.hg.repository import HgRepository
        return (isinstance(source, HgRepository) and 
                isinstance(target, HgRepository))
