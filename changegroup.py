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
import struct

from bzrlib import (
    debug,
    revision as _mod_revision,
    )

from bzrlib.plugins.hg.mapping import (
    as_hg_parents,
    files_from_delta,
    manifest_and_flags_from_tree,
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
               overlay, fileids={}, lossy=True):
    """Serialize a series of Bazaar revisions as Mercurial changesets.

    :param repo: Bazaar repository
    :param mapping: Bzr<->Hg Mapping
    :param revids: Iterable over revision ids
    :param files: Dictionary for looking up the set of changed files by revid
    :param manifest_ids: Dictionary for looking up the manifest id by revid
    :return: Iterable over changeset fulltexts
    """
    for revid in revids:
        if revid == _mod_revision.NULL_REVISION:
            yield "", (mercurial.node.nullid, mercurial.node.nullid), mercurial.node.nullid
            continue
        rev = repo.get_revision(revid)
        (manifest_id, user, date, desc, extra) = mapping.export_revision(rev,
            lossy=lossy, fileids=fileids.get(revid, {}))
        if manifest_id is None:
            manifest_id = manifest_ids[revid]
        if revid in manifest_ids and manifest_id != manifest_ids[revid]:
            raise AssertionError
        text = format_changeset(manifest_id, files[revid], user, date, desc,
            extra)
        ps = as_hg_parents(rev.parent_ids, changelog_ids.__getitem__)
        hgid = hghash(text, ps[0], ps[1])
        changelog_ids[revid] = hgid
        yield text, ps, hgid


def dinventories(repo, mapping, revids, manifest_ids, files, overlay, texts,
                 fileids, lossy=True):
    """Generate manifests from a series of revision trees.

    :param repo: Bazaar repository to fetch revisions from
    :param revids: Revision ids to yield manifests for (returned in same order)
    :param manifest_ids: Dictionary with revid -> file id mappings for known
        manifests. Used to look up parent manifests not processed
    :param files: Dictionary to store mercurial file dictionaries in, by revid
    :param overlay: Mercurial overlay object for the Bazaar repository
    :param texts: Dictionary with node -> (fileid, revision) tuples
    :param fileids: Dictionary mapping revision ids to file id lookup dictionaries,
        for any "unusual" file ids (not matching that predicted by the mapping).
        (only relevant for non-lossy conversions)
    :param lossy: Whether or not to do a lossy conversion.
    """
    def get_manifest(revid):
        if revid in manifest_ids:
            try:
                return manifests[manifest_ids[revid]]
            except KeyError:
                pass
        return overlay.get_manifest_and_flags_by_revid(revid)
    if revids == []:
        return
    skip_revid = revids[0]
    if revids[0] == _mod_revision.NULL_REVISION:
        yield "", (mercurial.node.nullid, mercurial.node.nullid), revids[0]
        revids = revids[1:]
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
        (manifest, flags, extrafileids) = manifest_and_flags_from_tree(
            parent_trees, tree, mapping, lookup_text_node)
        fileids[revid] = extrafileids
        manifests[revid] = (manifest, flags)
        try:
            base_tree = parent_trees[0]
        except IndexError:
            base_tree = repo.revision_tree(_mod_revision.NULL_REVISION)
        files[revid] = files_from_delta(tree.changes_from(base_tree),
            tree, revid)
        # Avoid sending texts for first revision, it's listed so we get the
        # base text for the manifest delta's.
        if revid != skip_revid:
            for p in files[revid]:
                fileid = tree.inventory.path2id(p)
                if fileid is not None:
                    # FIXME: This is probably not correct, as 'files'
                    # don't include new revisions that don't include changes
                    # (but are e.g. relevant for parents)
                    texts[p].add((fileid, tree.inventory[fileid].revision))
        text = format_manifest(manifest, flags)
        node_parents = as_hg_parents(rev.parent_ids, manifest_ids.__getitem__)
        manifest_id = hghash(text, node_parents[0], node_parents[1])
        manifest_ids[revid] = manifest_id
        if 'check' in debug.debug_flags:
            assert mapping.export_revision(rev)[0] in (None, manifest_id)
        yield text, node_parents, revid


def text_contents(repo, path, keys, overlay):
    """Generate revlog text tuples.

    :param repo: Bazaar repository
    :param path: UTF8 path
    :param keys: (fileid, revision) tuples of texts to convert
    :param overlay: Overlay
    :return: Always yields a base text first, then yields tuples with
        VersionedFileContentFactory, parent nodes, node id for each key
    """
    if not keys:
        yield ""
        return
    def text_as_node((fileid, revision)):
        try:
            return text_nodes[revision]
        except KeyError:
            return overlay.lookup_text_node_by_revid_and_path(revision, path)
    text_nodes = {}
    base_reported = False
    try:
        file_graph = repo.get_file_graph()
    except AttributeError: # bzr < 2.4
        file_graph = repo.texts
    first_parents = file_graph.get_parent_map([keys[0]])[keys[0]]
    if len(first_parents) == 0:
        yield ""
    else:
        base_stream = repo.texts.get_record_stream([first_parents[0]], 'unordered', True)
        yield base_stream.next().get_bytes_as("fulltext")

    for record in repo.texts.get_record_stream(keys, 'topological', True):
        fulltext = record.get_bytes_as('fulltext')
        parents = as_hg_parents(record.parents, text_as_node)
        node = hghash(fulltext, parents[0], parents[1])
        text_nodes[record.key[1]] = node
        yield (record, parents, node)


def chunkify(buffer):
    return chunkheader(len(buffer)) + buffer


def extract_base(entries):
    try:
        return (entries, entries.next()[0])
    except StopIteration:
        return (entries, "")


def bzr_changegroup(repo, overlay, changelog_ids, mapping, revids, lossy=True):
    """Create a changegroup based on (a derivation) of a set of revisions.

    :param repo: Bazaar repository to retrieve the revisions from
    :param revids: Iterable over the revision ids of the revisions to group
    :return: changegroup string
    """
    files = {}
    manifest_ids = lazydict(overlay.lookup_manifest_id_by_revid)
    texts = defaultdict(set)
    graph = repo.get_graph()
    revids = list(graph.iter_topo_order(revids))
    assert revids[0] != _mod_revision.NULL_REVISION
    base_revid = repo.get_parent_map([revids[0]])[revids[0]][0]
    todo = [base_revid] + revids # add base text revid

    fileids = {}
    manifests = list(dinventories(repo, mapping, todo, manifest_ids, files,
        overlay, texts, fileids, lossy=lossy))
    # 00changelog.i
    revs = drevisions(repo, mapping, todo, files, changelog_ids, manifest_ids,
        overlay, fileids=fileids, lossy=lossy)
    (revs, textbase) = extract_base(revs)
    for blob in pack_chunk_iter(revs, textbase):
        yield blob
    yield ""
    del files
    del manifest_ids

    # 00manifest.i
    manifests = ((text, ps, changelog_ids[revid]) for (text, ps, revid) in manifests)
    (manifests, textbase) = extract_base(manifests)
    for blob in pack_chunk_iter(manifests, textbase):
        yield blob
    yield ""

    del manifests
    # texts
    for path, keys in texts.iteritems():
        # FIXME: Mangle path in the same way that mercurial does
        yield path
        dtexts = text_contents(repo, path, keys, overlay)
        textbase = dtexts.next()
        content_chunks = ((record.get_bytes_as('fulltext'), parents,
            changelog_ids[record.key[1]]) for (record, parents, node) in
            dtexts)
        for blob in pack_chunk_iter(content_chunks, textbase):
            yield blob
        yield ""
    yield ""


class ChunkStringIO(object):

    def __init__(self, chunkiter):
        self.chunkiter = chunkiter
        self._stream = StringIO()

    def chunk(self):
        return self.chunkiter.next()

    def parsechunk(self):
        chunk = self.chunk()
        if not chunk:
            return {}
        h = chunk[:80]
        node, p1, p2, cs = struct.unpack("20s20s20s20s", h)
        data = chunk[80:]
        return dict(node=node, p1=p1, p2=p2, cs=cs, data=data)

    def read(self, l):
        while len(self._stream.getvalue()) < l:
            try:
                self._stream.write(chunkify(self.chunk()))
            except StopIteration:
                break
        return self._stream.read(l)

    def seek(self, pos):
        raise NotImplementedError(self.see)

    def tell(self):
        raise NotImplementedError(self.tell)

    def close(self):
        return self._stream.close()


def dchangegroup(repo, mapping, revids, lossy=True):
    from bzrlib.plugins.hg.overlay import get_overlay
    repo.lock_read()
    try:
        overlay = get_overlay(repo, mapping)
        changelog_ids = lazydict(lambda x: overlay.lookup_changeset_id_by_revid(x)[0])
        changelog_ids[_mod_revision.NULL_REVISION] = mercurial.node.nullid
        chunks = bzr_changegroup(repo, overlay, changelog_ids, mapping, revids, lossy)
    finally:
        repo.unlock()
    return ChunkStringIO(chunks), changelog_ids
