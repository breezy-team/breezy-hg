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
from collections import (
    defaultdict,
    )

from mercurial.changegroup import (
    chunkheader,
    )
import mercurial.node
from mercurial.revlog import (
    hash as hghash,
    )

from bzrlib import (
    debug,
    revision as _mod_revision,
    )

from bzrlib.plugins.hg.mapping import (
    as_hg_parents,
    files_from_delta,
    manifest_and_flags_from_tree,
    )
from bzrlib.plugins.hg.overlay import (
    get_overlay,
    )
from bzrlib.plugins.hg.parsers import (
    format_changeset,
    format_manifest,
    pack_chunk_iter,
    )
from bzrlib.plugins.hg.util import (
    lazydict,
    )


def drevisions(repo, mapping, revids, files, changelog_ids, manifest_ids,
               overlay):
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
        if revid in manifest_ids and manifest_id != manifest_ids[revid]:
            raise AssertionError
        text = format_changeset(manifest_id, files[revid], user, date, desc,
            extra)
        ps = as_hg_parents(rev.parent_ids, 
            lambda x: overlay.lookup_changeset_id_by_revid(x)[0])
        hgid = hghash(text, ps[0], ps[1])
        changelog_ids[revid] = hgid
        yield text, ps, hgid


def dinventories(repo, mapping, revids, manifest_ids, files, overlay, texts):
    def get_manifest(revid):
        if revid in manifest_ids:
            return manifests[manifest_ids[(revid)]]
        else:
            return overlay.get_manifest_and_flags_by_revid(revid)
    manifests = {}
    # TODO: Very naive and slow:
    for tree in repo.revision_trees(revids):
        revid = tree.get_revision_id()
        rev = repo.get_revision(revid)
        lookup_text_node = []
        for parent in rev.parent_ids[:2]:
            lookup_text_node.append(get_manifest(parent)[0].__getitem__)
        while len(lookup_text_node) < 2:
            lookup_text_node.append(lambda path: mercurial.node.nullid)
        # TODO: This refetches the parent trees, which we'll likely have seen 
        # earlier in this loop.
        parent_trees = list(repo.revision_trees(rev.parent_ids[:2]))
        (manifest, flags) = manifest_and_flags_from_tree(parent_trees, tree, 
            mapping, lookup_text_node)
        manifests[revid] = (manifest, flags)
        try:
            base_tree = parent_trees[0]
        except KeyError:
            base_tree = repo.revision_tree(_mod_revision.NULL_REVISION)
        files[revid] = files_from_delta(tree.changes_from(base_tree), 
            tree.inventory, revid)
        # Avoid sending texts for first revision, it's listed so we get the 
        # base text for the manifest delta's.
        if revid != revids[0]:
            for p in files[revid]:
                fileid = tree.inventory.path2id(p)
                if fileid is not None:
                    texts[p].add((fileid, tree.inventory[fileid].revision))
        text = format_manifest(manifest, flags)
        node_parents = as_hg_parents(rev.parent_ids, manifest_ids.__getitem__)
        manifest_id = hghash(text, node_parents[0], node_parents[1])
        manifest_ids[revid] = manifest_id
        if 'check' in debug.debug_flags:
            assert mapping.export_revision(rev)[0] in (None, manifest_id)
        yield text, node_parents, revid


def text_contents(repo, get_changelog_id, path, keys, overlay):
    def text_as_node((fileid, revision)):
        return overlay.lookup_text_node_by_revid_and_path(revision, path)
    for record in repo.texts.get_record_stream(keys, 'topological', True):
        fulltext = record.get_bytes_as('fulltext')
        yield (fulltext, as_hg_parents(record.parents, text_as_node), 
               get_changelog_id(record.key[1]))


def write_chunk(f, buffer):
    f.write(chunkheader(len(buffer)))
    f.write(buffer)


def write_delta_chunks(f, entries):
    for blob in pack_chunk_iter(entries):
        write_chunk(f, blob)
    write_chunk(f, "")


def dchangegroup(repo, mapping, revids):
    """Create a changegroup based on (a derivation) of a set of revisions.

    :param repo: Bazaar repository to retrieve the revisions from
    :param revids: Iterable over the revision ids of the revisions to group
    :return: changegroup string
    """
    ret = StringIO()
    overlay = get_overlay(repo, mapping)
    files = {}
    manifest_ids = lazydict(overlay.lookup_manifest_id_by_revid)
    texts = defaultdict(set)
    changelog_ids = lazydict(overlay.lookup_changeset_id_by_revid)
    graph = repo.get_graph()
    revids = list(graph.iter_topo_order(revids))
    todo = [repo.get_parent_map([revids[0]])[revids[0]][0]] + revids # add base text revid
    manifests = list(dinventories(repo, mapping, todo, manifest_ids, files, overlay, texts))
    # 00changelog.i
    write_delta_chunks(ret, drevisions(repo, mapping, todo, files, changelog_ids, manifest_ids, overlay))
    del files
    del manifest_ids
    # 00manifest.i
    write_delta_chunks(ret, ((text, ps, changelog_ids[revid]) for (text, ps, revid) in manifests))
    del manifests
    # texts
    for path, keys in texts.iteritems():
        # FIXME: Mangle path in the same way that mercurial does
        write_chunk(ret, path)
        write_delta_chunks(ret, text_contents(repo, changelog_ids.__getitem__,
                           path, keys, overlay))
    write_chunk(ret, "")
    ret.seek(0)
    return ret, {}
