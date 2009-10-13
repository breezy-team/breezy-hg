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

"""Push support."""

from cStringIO import StringIO

import mercurial.node
from mercurial.revlog import hash as hghash

from bzrlib.plugins.hg.mapping import (
    files_from_delta,
    manifest_and_flags_from_tree,
    )
from bzrlib.plugins.hg.overlay import (
    get_overlay,
    )
from bzrlib.plugins.hg.parsers import (
    format_changeset,
    format_manifest,
    )


def drevisions(repo, mapping, revids, files, manifest_ids):
    """Serialize a series of Bazaar revisions as Mercurial changesets.

    :param repo: Bazaar repository
    :param mapping: Bzr<->Hg Mapping
    :param revids: Iterable over revision ids
    :param files: Dictionary for looking up the set of changed files by revid
    :param manifest_ids: Dictionary for looking up the manifest id by revid
    :return: Iterable over changeset fulltexts
    """
    for revid in revids:
        rev = repo.get_revision(revid)
        (manifest_id, user, date, desc, extra) = mapping.export_revision(rev)
        if manifest_id is None:
            manifest_id = manifest_ids[revid]
        assert manifest_id == manifest_ids[revid]
        yield format_changeset(manifest_id, files, user, date, desc, extra)


def dinventories(repo, mapping, revids, manifest_ids, files, overlay):
    def lookup_manifest_id(revid):
        try:
            return manifest_ids[revid]
        except KeyError:
            return overlay.lookup_manifest_id_by_revid(revid)
    def get_manifest(manifest_id):
        try:
            return manifests[manifest_id]
        except KeyError:
            return overlay.get_manifest_and_flags(manifest_id)
    manifests = {}
    # TODO: Very naive and slow:
    for tree in repo.revision_trees(revids):
        revid = tree.get_revision_id()
        rev = repo.get_revision(revid)
        node_parents = []
        lookup_text_node = []
        for parent in rev.parent_ids[:2]:
            node_parents.append(lookup_manifest_id(parent))
            lookup_text_node.append(get_manifest(lookup_manifest_id(parent))[0].__getitem__)
        while len(node_parents) < 2:
            node_parents.append(mercurial.node.nullid)
            lookup_text_node.append(lambda path: mercurial.node.nullid)
        (manifest, flags) = manifest_and_flags_from_tree(tree, mapping, lookup_text_node)
        manifests[revid] = (manifest, flags)
        # TODO: This refetches the inventory and base inventory while that's not necessary:
        delta = repo.get_revision_delta(revid)
        files[revid] = files_from_delta(delta, tree.inventory, revid)
        text = format_manifest(manifest, flags)
        manifest_ids[revid] = hghash(text, node_parents[0], node_parents[1])
        yield text


def dchangegroup(repo, mapping, revids):
    """Create a changegroup based on (a derivation) of a set of revisions.

    :param repo: Bazaar repository to retrieve the revisions from
    :param revids: Iterable over the revision ids of the revisions to group
    :return: changegroup string
    """
    overlay = get_overlay(repo, mapping)
    files = {}
    manifest_ids = {}
    manifests = list(dinventories(repo, mapping, revids, manifest_ids, files, overlay))
    changesets = drevisions(repo, mapping, revids, files, manifest_ids)
    # TODO: yield 00changeset.i
    # TODO: yield 00manifest.i
    # TODO: yield contents
    return StringIO(), {}
