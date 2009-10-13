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

from bzrlib.plugins.hg.mapping import (
    files_from_delta,
    manifest_and_flags_from_tree,
    )


class MercurialRepositoryOverlay(object):
    """Overlay that allows accessing some Mercurialisque properties from a Bazaar repo."""

    def __init__(self, repo, mapping, idmap):
        self.repo = repo
        self.mapping = mapping
        self.idmap = idmap

    def _update_idmap(self):
        present_revids = self.idmap.revids()
        todo = set(self.repo.all_revision_ids()) - present_revids
        revs = self.repo.get_revisions(todo)
        for revid in todo:
            rev = self.repo.get_revision(revid)
            try:
                manifest_id = rev.properties['manifest']
            except KeyError:
                pass # no 'manifest' property present
            else:
                self.idmap.insert_manifest(manifest_id, revid)

    def get_files_by_revid(self, revid):
        try:
            return self.idmap.get_files_by_revid(revid)
        except KeyError:
            delta = self.repo.get_revision_delta(revid)
            inv = self.repo.get_inventory(revid)
            return files_from_delta(delta, inv, revid)

    def get_manifest_and_flags_by_revid(self, revid):
        tree = self.repo.revision_tree(revid)
        lookup_text_node = []
        rev = self.repo.get_revision(revid)
        for p in rev.parent_ids[:2]:
            parent_manifest = self.get_manifest_and_flags_by_revid(p)[0]
            lookup_text_node.append(parent_manifest.__getitem__)
        while len(lookup_text_node) < 2:
            lookup_text_node.append(lambda path: mercurial.node.nullid)
        return manifest_and_flags_from_tree(tree, self.mapping, 
            lookup_text_node) 

    def get_manifest_and_flags(self, manifest_id):
        revid = self.lookup_revision_by_manifest_id(manifest_id)
        return self.get_manifest_and_flags_by_revid(revid)

    def lookup_revision_by_changeset_id(self, changeset_id):
        bzr_revid = self.mapping.revision_id_foreign_to_bzr(changeset_id)
        if self.repo.has_revision(bzr_revid):
            return bzr_revid
        return self.idmap.lookup_revision(changeset_id)

    def lookup_revision_by_manifest_id(self, manifest_id):
        try:
            return self.idmap.lookup_revision_by_manifest_id(manifest_id)
        except KeyError:
            self._update_idmap()
            return self.idmap.lookup_revision_by_manifest_id(manifest_id)
