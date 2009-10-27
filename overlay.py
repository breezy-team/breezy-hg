# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
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

"""Overlay that allows accessing a Bazaar repository like a Mercurial one."""

import mercurial.node
from mercurial.revlog import (
    hash as hghash,
    )
from mercurial import (
    error as hgerrors,
    )

from bzrlib import (
    ui,
    )

from bzrlib.plugins.hg.idmap import (
    MemoryIdmap,
    )
from bzrlib.plugins.hg.mapping import (
    as_hg_parents,
    default_mapping,
    files_from_delta,
    manifest_and_flags_from_tree,
    )
from bzrlib.plugins.hg.parsers import (
    format_changeset,
    format_manifest,
    )


class changelog_wrapper(object):

    def __init__(self, bzrrepo, mapping):
        self.bzrrepo = bzrrepo
        self.mapping = mapping

    def rev(self, node):
        return None # FIXME


def get_overlay(bzr_repo, mapping=None):
    if mapping is None:
        mapping = default_mapping
    return MercurialRepositoryOverlay(bzr_repo, mapping, MemoryIdmap())


class MercurialRepositoryOverlay(object):
    """Overlay that allows accessing some Mercurialisque properties from a Bazaar repo."""

    def __init__(self, repo, mapping, idmap):
        self.repo = repo
        self.mapping = mapping
        self.idmap = idmap
        self.changelog = changelog_wrapper(self.repo, self.mapping)

    def _update_idmap(self):
        present_revids = self.idmap.revids()
        todo = set(self.repo.all_revision_ids()) - present_revids
        revs = self.repo.get_revisions(todo)
        graph = self.repo.get_graph()
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for i, revid in enumerate(graph.iter_topo_order(todo)):
                pb.update("updating cache", i, len(todo))
                rev = self.repo.get_revision(revid)
                try:
                    manifest_id = rev.properties['manifest']
                except KeyError:
                    manifest_text = self.get_manifest_text_by_revid(revid)
                    manifest_id = hghash(manifest_text, *as_hg_parents(rev.parent_ids[:2], self.lookup_manifest_id_by_revid))
                    pass # no 'manifest' property present
                else:
                    self.idmap.insert_manifest(manifest_id, revid)
        finally:
            pb.finished()

    def __len__(self):
        # Slow...
        return len(self.repo.all_revision_ids())

    def lookup(self, key):
        if key == 'null':
            return mercurial.node.nullid
        if key == 'tip':
            revid = self._bzrdir.open_branch().last_revision()
            return self._overlay.lookup_changeset_id_by_revid(revid)
        if key == '.':
            raise NotImplementedError
        raise hgerrors.RepoLookupError("unknown revision '%s'" % key)

    def get_files_by_revid(self, revid):
        try:
            return self.idmap.get_files_by_revid(revid)
        except KeyError:
            delta = self.repo.get_revision_delta(revid)
            inv = self.repo.get_inventory(revid)
            return files_from_delta(delta, inv, revid)

    def get_manifest_text(self, manifest_id):
        revid = self._lookup_revision_by_manifest_id(manifest_id)
        return self.get_manifest_text_by_revid(revid) 

    def _get_file_fulltext(self, key):
        ret = "".join(self.target.iter_files_bytes([key + (None,)]).next()[1])
        if ret == "": # could be a symlink
            ie = self.target.get_inventory(key[1])[key[0]]
            if ie.kind == "symlink":
                return ie.symlink_target
        return ret

    def get_manifest_text_by_revid(self, revid):
        (manifest, flags) = self.get_manifest_and_flags_by_revid(revid)
        return format_manifest(manifest, flags)

    def get_manifest_and_flags_by_revid(self, revid):
        tree = self.repo.revision_tree(revid)
        lookup_text_node = []
        rev = self.repo.get_revision(revid)
        base_tree = list(self.repo.revision_trees(rev.parent_ids[:2]))
        for p in rev.parent_ids[:2]:
            parent_manifest = self.get_manifest_and_flags_by_revid(p)[0]
            lookup_text_node.append(parent_manifest.__getitem__)
        while len(lookup_text_node) < 2:
            lookup_text_node.append(lambda path: mercurial.node.nullid)
        return manifest_and_flags_from_tree(base_tree, tree, self.mapping, 
                lookup_text_node)[:2]

    def get_manifest_and_flags(self, manifest_id):
        """Return manifest by manifest id.

        :param manifest_id: 20-byte Mercurial revlog id
        :return: Tuple with manifest dictionary and flags dictionary
        """
        if manifest_id == mercurial.node.nullid:
            return {}, {}
        revid = self._lookup_revision_by_manifest_id(manifest_id)
        return self.get_manifest_and_flags_by_revid(revid)

    def lookup_revision_by_changeset_id(self, changeset_id):
        bzr_revid = self.mapping.revision_id_foreign_to_bzr(changeset_id)
        if self.repo.has_revision(bzr_revid):
            return bzr_revid
        return self.idmap.lookup_revision(changeset_id)

    def _lookup_revision_by_manifest_id(self, manifest_id):
        try:
            return self.idmap.lookup_revision_by_manifest_id(manifest_id)
        except KeyError:
            self._update_idmap()
            return self.idmap.lookup_revision_by_manifest_id(manifest_id)

    def has_hgid(self, id):
        """Check whether a Mercurial revision id is present in the repo.
        
        :param id: Mercurial ID
        :return: boolean
        """
        return (len(self.has_hgids([id])) == 1)

    def lookup_text_node_by_revid_and_path(self, revid, path):
        (manifest, flags) = self.get_manifest_and_flags_by_revid(revid)
        return manifest[path]

    def lookup_manifest_id_by_revid(self, revid):
        rev = self.repo.get_revision(revid)
        return mercurial.node.bin(rev.properties['manifest'])

    def get_changeset_text_by_revid(self, revid):
        rev = self.repo.get_revision(revid)
        (manifest, user, (time, timezone), desc, extra) = \
            self.mapping.export_revision(rev)
        if manifest is None:
            # Manifest not in the revision, look it up
            # This could potentially be very expensive, but no way around 
            # that...
            manifest = self.lookup_manifest_id_by_revid(revid)
        files = self.get_files_by_revid(revid)
        return format_changeset(manifest, files, user, (time, timezone),
                                desc, extra)

    def lookup_changeset_id_by_revid(self, revid):
        # TODO: Handle roundtripping
        return self.mapping.revision_id_bzr_to_foreign(revid)

    def has_hgids(self, ids):
        """Check whether the specified Mercurial ids are present.
        
        :param ids: Mercurial revision ids
        :return: Set with the revisions that were present
        """
        # TODO: What about round-tripped revisions?
        revids = set([self.mapping.revision_id_foreign_to_bzr(h) for h in ids])
        return set([
            self.mapping.revision_id_bzr_to_foreign(revid)[0]
            for revid in self.repo.has_revisions(revids)])
