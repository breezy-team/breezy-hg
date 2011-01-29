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

# Some of this code was based on code from Mercurial:
#
# InterHgRepository.findmissing is based on
#       mercurial.localrepo.localrepository.findcommonincoming
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
# Published under the GNU GPLv2 or later

"""Inter-repository operations involving Mercurial repositories."""

from collections import (
    defaultdict,
    )
import mercurial.node
import os

from bzrlib import (
    debug,
    errors,
    lru_cache,
    osutils,
    trace,
    ui,
    )
from bzrlib.decorators import (
    needs_write_lock,
    )
from bzrlib.graph import (
    Graph,
    )
from bzrlib.inventory import (
    InventoryDirectory,
    InventoryFile,
    InventoryLink,
    )
from bzrlib.repository import (
    InterRepository,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.versionedfile import (
    FulltextContentFactory,
    )

from bzrlib.plugins.hg.mapping import (
    as_bzr_parents,
    files_from_delta,
    flags_kind,
    manifest_and_flags_from_tree,
    )
from bzrlib.plugins.hg.overlay import (
    get_overlay,
    )
from bzrlib.plugins.hg.parsers import (
    deserialize_file_text,
    parse_changeset,
    parse_manifest,
    unpack_chunk_iter,
    )


def inventory_create_directory(directories, basis_inv, other_inv, path,
                               lookup_file_id, revid):
    """Make sure a directory and its parents exist.

    :param directories: Dictionary with directories that have already been
        created as keys, their id as value
    :param basis_inv: Basis inventory against which directories should be
        created
    :param other_inv: Optional other inventory that could have introduced
        directories
    :param path: Path of the directory
    :param lookup_file_id: Lookup file id
    :param revid: Revision id to use when creating new inventory entries
    :return: Tuple with inventory delta and file id of the specified path.
    """
    if path in directories:
        return ([], directories[path])
    if basis_inv is not None and basis_inv.has_filename(path):
        directories[path] = basis_inv.path2id(path)
        return ([], directories[path])
    if (other_inv is not None and
        basis_inv.has_filename(os.path.dirname(path)) and
        other_inv.has_filename(path)):
        other_fileid = other_inv.path2id(path)
        other_ie = other_inv[other_fileid]
        ie = InventoryDirectory(other_fileid, other_ie.name,
                                other_ie.parent_id)
        ie.revision = other_ie.revision
        directories[path] = other_fileid
        return ([(None, path, other_fileid, ie)], other_fileid)
    if path != "":
        ret, parent_id = inventory_create_directory(directories, basis_inv,
            other_inv, os.path.dirname(path), lookup_file_id, revid)
    else:
        # Root directory doesn't have a parent id
        ret = []
        parent_id = None
    fileid = lookup_file_id(path.encode("utf-8"))
    ie = InventoryDirectory(fileid, os.path.basename(path), parent_id)
    ie.revision = revid
    ret.append((None, path, fileid, ie))
    directories[path] = fileid
    return ret, fileid


def manifest_to_inventory_delta(lookup_file_id, basis_inv, other_inv,
                                (basis_manifest, basis_flags),
                                (manifest, flags),
                                revid, files, lookup_metadata,
                                lookup_symlink):
    """Simple O(n) manifest to inventory converter.

    Does not take renames into account.

    :param lookup_file_id: Lookup a file id
    :param basis_inv: Basis (Bazaar) inventory (None if there are no parents)
    :param other_inv: Optional merge parent inventory
    :param (basis_manifest, basis_flags): Manifest and flags matching basis
        inventory.
    :param (manifest, flags): Manifest and flags to convert
    :param revid: Revision id of the revision for which to convert the manifest
    :param files: List of files changed somehow
    :param lookup_metadata: Function for looking up sha1
        and length for a node by (fileid, revision) tuple.
    :param lookup_symlink: Function to lookup symlink target.
    """
    # Set of directories that have been created in this delta, their file id
    # as value.
    directories = {}
    if basis_inv is None:
        # Root is mandatory
        extra, root_id = inventory_create_directory(directories,
            basis_inv, other_inv, "", lookup_file_id, revid)
        for e in extra:
            yield e
    # Dictionary of directories that could have been made empty in this delta,
    # with the set of removed children as value.
    maybe_empty_dirs = defaultdict(set)
    maybe_empty_dirs[""] = None # Never consider removing the root
    for utf8_path in set(basis_manifest.keys() + manifest.keys()):
        if (basis_manifest.get(utf8_path) == manifest.get(utf8_path) and
            basis_flags.get(utf8_path) == flags.get(utf8_path)):
            continue
        path = utf8_path.decode("utf-8")
        # Does it still exist in manifest ?
        if utf8_path not in manifest:
            # File removed
            file_id = basis_inv.path2id(path)
            if file_id is None:
                raise AssertionError("Removed file %r didn't exist in basis" % path)
            yield (path, None, file_id, None)
            dirname = os.path.dirname(path)
            if maybe_empty_dirs[dirname] is not None:
                maybe_empty_dirs[dirname].add(basis_inv[file_id].name)
        else:
            assert type(utf8_path) is str
            fileid = lookup_file_id(utf8_path)
            parent_path, basename = os.path.split(path)
            maybe_empty_dirs[parent_path] = None
            if basis_inv is not None and basis_inv.has_filename(path):
                old_path = path
                parent_id = basis_inv.path2id(parent_path)
            else:
                old_path = None
                # Make sure parent exists
                extra, parent_id = inventory_create_directory(directories,
                    basis_inv, other_inv, parent_path, lookup_file_id, revid)
                for e in extra:
                    yield e
            f = flags.get(utf8_path, "")
            if 'l' in f:
                entry_factory = InventoryLink
            else:
                entry_factory = InventoryFile
            ie = entry_factory(fileid, basename, parent_id)
            if ie.kind == "file":
                ie.executable = ('x' in f)
            if utf8_path not in files:
                # Not changed in this revision, so pick one of the parents
                if (manifest.get(utf8_path) == basis_manifest.get(utf8_path) and
                    flags.get(utf8_path) == basis_flags.get(utf8_path)):
                    orig_inv = basis_inv
                else:
                    orig_inv = other_inv
                ie.revision = orig_inv[fileid].revision
                if ie.kind == "symlink":
                    ie.symlink_target = orig_inv[fileid].symlink_target
                elif ie.kind == "file":
                    ie.text_sha1 = orig_inv[fileid].text_sha1
                    ie.text_size = orig_inv[fileid].text_size
            else:
                ie.revision = revid
                if ie.kind == "file":
                    ie.text_sha1, ie.text_size = lookup_metadata(
                        (fileid, ie.revision))
                elif ie.kind == "symlink":
                    ie.symlink_target = lookup_symlink((fileid, ie.revision))
            yield (old_path, path, fileid, ie)
    # Remove empty directories
    while maybe_empty_dirs:
        for path in sorted(maybe_empty_dirs.keys(), reverse=True):
            removed_children = maybe_empty_dirs.pop(path)
            if removed_children is None:
                # Stuff was added to this directory in this revision,
                # don't bother
                continue
            file_id = basis_inv.path2id(path)
            # Is this directory really empty ?
            if set(basis_inv[file_id].children.keys()) == removed_children:
                yield (path, None, file_id, None)
                dirname = os.path.dirname(path)
                if maybe_empty_dirs[dirname] is not None:
                    maybe_empty_dirs[dirname].add(basis_inv[file_id].name)


def create_directory_texts(texts, invdelta):
    """Create the texts for directories mentioned in an inventory delta.

    :param texts: VersionedFiles to add entries to
    :param invdelta: Inventory delta
    """
    def generate_stream():
        for (old_path, new_path, fileid, ie) in invdelta:
            if old_path is None and ie.kind == "directory":
                record = FulltextContentFactory((fileid, ie.revision), (), None, "")
                record.parents = ()
                yield record
    texts.insert_record_stream(generate_stream())


def check_roundtrips(repository, mapping, revid, expected_files,
                     (expected_manifest, expected_flags),
                     manifest_parents, inventory=None):
    """Make sure that a revision imported to Bazaar can be re-exported to hg.

    :param repository: Bazaar repository to retrieve revision from
    :param mapping: Bzr<->Hg mapping to use
    :param revid: Bazaar revision id
    :param expected_files: Expected Mercurial-style files list
    :param (expected_manifest, expected_flags): Expected manifest and flags
    :param manifest_parents: Manifests of the parents of revision
    :param inventory: Optional inventory for revid, if the caller already had it
    """
    if inventory is None:
        inventory = repository.get_inventory(revid)
    tree = repository.revision_tree(revid)
    rev = repository.get_revision(revid)
    parent_trees = list(repository.revision_trees(rev.parent_ids[:2]))
    try:
        base_tree = parent_trees[0]
    except IndexError:
        base_tree = repository.revision_tree(NULL_REVISION)
    delta = tree.changes_from(base_tree)
    files = files_from_delta(delta, inventory, revid)
    if expected_files != files:
        raise AssertionError
    lookup = [m.__getitem__ for m, f in manifest_parents[:2]]
    for i in range(2):
        if len(lookup) <= i:
            lookup.append({}.__getitem__)
    (manifest, flags, unusual_fileids) = manifest_and_flags_from_tree(parent_trees, tree,
        mapping, lookup)
    if set(manifest.keys()) != set(expected_manifest.keys()):
        raise AssertionError("Different contents in manifests: %r, %r" %
                (manifest.keys(), expected_manifest.keys()))
    if set(flags.keys()) != set(expected_flags.keys()):
        raise AssertionError("Different flags: %r, %r" %
                (flags, expected_flags))
    for path in manifest:
        if manifest[path] != expected_manifest[path]:
            raise AssertionError("Different version %s: %s, %s" %
                (path, mercurial.node.hex(manifest[path]),
                       mercurial.node.hex(expected_manifest[path])))
    for path in flags:
        if expected_flags[path] != flags[path]:
            raise AssertionError("Different flags for %s: %s != %s" %
                (path, expected_flags[path], flags[path]))


class FromHgRepository(InterRepository):
    """Hg to any repository actions."""

    def __init__(self, source, target):
        InterRepository.__init__(self, source, target)
        self._target_overlay = get_overlay(self.target, self.source.get_mapping())
        self._inventories = lru_cache.LRUCache(25)
        self._revisions = {}
        self._files = {}
        self._text_metadata = {}
        self._symlink_targets = {}
        # Map mapping manifest ids to bzr revision ids
        self._manifest2rev_map = defaultdict(set)

    @classmethod
    def _get_repo_format_to_test(self):
        """The format to test with - as yet there is no HgRepoFormat."""
        return None

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

    def _import_manifest_delta(self, manifest, flags, files, rev,
                               mapping):
        parent_invs = self._get_inventories(rev.parent_ids)
        if not len(rev.parent_ids) in (0, 1, 2):
            raise AssertionError
        if len(rev.parent_ids) == 0:
            basis_inv = None
            other_inv = None
            basis_manifest = {}
            basis_flags = {}
        else:
            basis_inv = parent_invs[0]
            basis_manifest, basis_flags = self._target_overlay.get_manifest_and_flags_by_revid(rev.parent_ids[0])
            if len(rev.parent_ids) == 2:
                other_inv = parent_invs[1]
            else:
                other_inv = None
        invdelta = list(manifest_to_inventory_delta(mapping.generate_file_id,
                basis_inv, other_inv, (basis_manifest, basis_flags),
                (manifest, flags), rev.revision_id, files,
                self._text_metadata.__getitem__,
                self._symlink_targets.__getitem__))
        return basis_inv, invdelta

    def _get_target_fulltext(self, key):
        if key in self._symlink_targets:
            return self._symlink_targets[key]
        return self._target_overlay.get_file_fulltext(key)

    def _unpack_texts(self, cg, mapping, filetext_map, pb):
        i = 0
        # Texts
        while 1:
            f = mercurial.changegroup.getchunk(cg)
            if not f:
                break
            i += 1
            pb.update("fetching texts", i, len(filetext_map))
            fileid = mapping.generate_file_id(f)
            chunkiter = mercurial.changegroup.chunkiter(cg)
            def get_text(node):
                key = iter(filetext_map[fileid][node]).next()
                return self._get_target_fulltext(key)
            for fulltext, hgkey, hgparents, csid in unpack_chunk_iter(chunkiter, get_text):
                for revision, (kind, parents) in filetext_map[fileid][hgkey].iteritems():
                    key = (fileid, revision)
                    if kind == "symlink":
                        self._symlink_targets[key] = fulltext
                        bzr_fulltext = ""
                    else:
                        (meta, bzr_fulltext) = deserialize_file_text(fulltext)
                    record = FulltextContentFactory(key, [(fileid, p) for p in parents], osutils.sha_string(bzr_fulltext), bzr_fulltext)
                    self._text_metadata[key] = (record.sha1, len(bzr_fulltext))
                    yield record

    def _add_inventories(self, todo, mapping, pb):
        assert isinstance(todo, list)
        total = len(self._revisions)
        # add the actual revisions
        for i, (revid, (manifest, flags)) in enumerate(self._target_overlay.get_manifest_and_flags_by_revids(todo)):
            pb.update("adding inventories", i, len(todo))
            rev = self._revisions[revid]
            files = self._files[rev.revision_id]
            del self._files[rev.revision_id]
            if rev.parent_ids == ():
                basis_revid = NULL_REVISION
            else:
                basis_revid = rev.parent_ids[0]
            basis_inv, invdelta = self._import_manifest_delta(
                manifest, flags, files, rev, mapping)
            # FIXME: Add empty directories
            create_directory_texts(self.target.texts, invdelta)
            (validator, new_inv) = self.target.add_inventory_by_delta(
                basis_revid, invdelta, rev.revision_id, rev.parent_ids,
                basis_inv)
            self._inventories[rev.revision_id] = new_inv
            self.target.add_revision(rev.revision_id, rev, new_inv)
            self._target_overlay.idmap.insert_revision(rev.revision_id,
                rev.properties['manifest'], rev.foreign_revid, mapping)
            del self._revisions[rev.revision_id]
            if 'check' in debug.debug_flags:
                check_roundtrips(self.target, mapping, rev.revision_id,
                    files, (manifest, flags),
                    [x[1] for x in self._target_overlay.get_manifest_and_flags_by_revids(rev.parent_ids[:2])],
                    inventory=new_inv,
                    )

    def _unpack_changesets(self, chunkiter, mapping, pb):
        def lookup_foreign_revid(foreign_revid):
            lookup_foreign_revid = getattr(self.source,
                "lookup_foreign_revision_id", None)
            if lookup_foreign_revid is not None:
                return lookup_foreign_revid(foreign_revid, mapping)
            return mapping.revision_id_foreign_to_bzr(foreign_revid)
        def get_hg_revision(hgid):
            revid = lookup_foreign_revid(hgid)
            return self._target_overlay.get_changeset_text_by_revid(revid)
        for i, (fulltext, hgkey, hgparents, csid) in enumerate(
                unpack_chunk_iter(chunkiter, get_hg_revision)):
            pb.update("fetching changesets", i)
            (manifest, user, (time, timezone), files, desc, extra) = \
                parse_changeset(fulltext)
            key = mapping.revision_id_foreign_to_bzr(hgkey)
            parent_ids = as_bzr_parents(hgparents, lookup_foreign_revid)
            rev, fileids = mapping.import_revision(key, parent_ids, hgkey,
                manifest, user, (time, timezone), desc, extra)
            self._files[rev.revision_id] = files
            self._manifest2rev_map[manifest].add(rev.revision_id)
            self._revisions[rev.revision_id] = rev

    def get_parent_map(self, revids):
        ret = {}
        missing = []
        for revid in revids:
            try:
                ret[revid] = self._revisions[revid].parent_ids
            except KeyError:
                missing.append(revid)
        if missing:
            ret.update(self.target.get_parent_map(missing))
        return ret

    def _find_most_recent_ancestor(self, candidates, revid):
        if len(candidates) == 1:
            return candidates[0]
        graph = Graph(self)
        for r, ps in graph.iter_ancestry([revid]):
            if r in candidates:
                return r
        raise AssertionError

    def _unpack_manifests(self, chunkiter, mapping, filetext_map, todo, pb):
        """Unpack the manifest deltas.

        :param chunkiter: Iterator over delta chunks for the manifest.
        :param mapping: Bzr<->Hg mapping
        :param pb: Progress bar
        """
        for i, (fulltext, hgkey, hgparents, csid) in enumerate(
                unpack_chunk_iter(chunkiter, self._target_overlay.get_manifest_text)):
            pb.update("fetching manifests", i, len(self._revisions))
            (manifest, flags) = parse_manifest(fulltext)
            for revid in self._manifest2rev_map[hgkey]:
                todo.append(revid)
                yield (revid, self._revisions[revid].parent_ids, fulltext)
                self._target_overlay.remember_manifest(revid,
                    self._revisions[revid].parent_ids, (manifest, flags))
                if not self._files[revid]:
                    # Avoid fetching inventories and parent manifests
                    # unnecessarily
                    continue
                rev = self._revisions[revid]
                parents = []
                for previd in rev.parent_ids:
                    try:
                        inv = self.target.get_inventory(previd)
                    except errors.NoSuchRevision:
                        parents.append(self._target_overlay.get_manifest_and_flags_by_revid(previd)[0])
                    else:
                        parents.append(inv)
                for path in self._files[revid]:
                    assert type(path) is str
                    fileid = mapping.generate_file_id(path)
                    if not path in manifest:
                        # Path still has to actually exist..
                        continue
                    kind = flags_kind(flags, path)
                    text_parents = []
                    for parent in parents:
                        path2id = getattr(parent, "path2id", None)
                        if path2id is None: # manifest
                            node = parent.get(path)
                            if node is None:
                                continue
                            revisions = filetext_map[fileid][node]
                            tp = self._find_most_recent_ancestor(revisions.keys(), revid)
                            text_parents.append(tp)
                        elif path2id(path) == fileid:
                            # FIXME: Handle situation where path is not actually in parent
                            text_parents.append(parent[fileid].revision)
                    filetext_map[fileid][manifest[path]][revid] = (kind, text_parents)

    def addchangegroup(self, cg, mapping):
        """Import a Mercurial changegroup into the target repository.

        :param cg: Changegroup to add
        :param mapping: Mercurial mapping
        """
        # Changesets
        chunkiter = mercurial.changegroup.chunkiter(cg)
        pb = ui.ui_factory.nested_progress_bar()
        try:
            self._unpack_changesets(chunkiter, mapping, pb)
        finally:
            pb.finished()
        # Manifests
        manifestchunks = mercurial.changegroup.chunkiter(cg)
        filetext_map = defaultdict(lambda: defaultdict(dict))
        todo = []
        pb = ui.ui_factory.nested_progress_bar()
        try:
            self._target_overlay.remember_manifest_texts(
                self._unpack_manifests(manifestchunks, mapping, filetext_map, todo, pb))
        finally:
            pb.finished()
        # Texts
        pb = ui.ui_factory.nested_progress_bar()
        try:
            self.target.texts.insert_record_stream(
                self._unpack_texts(cg, mapping, filetext_map, pb))
        finally:
            pb.finished()
        # Adding actual data
        pb = ui.ui_factory.nested_progress_bar()
        try:
            self._add_inventories(todo, mapping, pb)
        finally:
            pb.finished()

    def heads(self, fetch_spec, revision_id):
        """Determine the Mercurial heads to fetch. """
        if fetch_spec is not None:
            mapping = self.source.get_mapping()
            return [mapping.revision_id_bzr_to_foreign(head)[0] for head in fetch_spec.heads]
        if revision_id is not None:
            mapping = self.source.get_mapping()
            return [mapping.revision_id_bzr_to_foreign(revision_id)[0]]
        return self.source._hgrepo.heads()

    def findmissing(self, heads):
        """Find the set of ancestors of heads missing from target.

        :param heads: Mercurial heads to check for.

        Based on mercurial.localrepo.localrepository.findcommonincoming
        """
        unknowns = list(set(heads) - self._target_overlay.has_hgids(heads))
        if not unknowns:
            return []
        seen = set()
        search = []
        fetch = set()
        seenbranch = set()
        remote = self.source._hgrepo
        req = set(unknowns)

        # search through remote branches
        # a 'branch' here is a linear segment of history, with four parts:
        # head, root, first parent, second parent
        # (a branch always has two parents (or none) by definition)
        unknowns = remote.branches(unknowns)
        while unknowns:
            r = []
            while unknowns:
                n = unknowns.pop(0)
                if n[0] in seen:
                    continue
                trace.mutter("examining %s:%s", mercurial.node.short(n[0]),
                             mercurial.node.short(n[1]))
                if n[0] == mercurial.node.nullid: # found the end of the branch
                    pass
                elif n in seenbranch:
                    trace.mutter("branch already found")
                    continue
                elif n[1] and self._target_overlay.has_hgid(n[1]): # do we know the base?
                    trace.mutter("found incomplete branch %s:%s",
                        mercurial.node.short(n[0]), mercurial.node.short(n[1]))
                    search.append(n[0:2]) # schedule branch range for scanning
                    seenbranch.add(n)
                else:
                    if n[1] not in seen and n[1] not in fetch:
                        if self._target_overlay.has_hgid(n[2]) and self._target_overlay.has_hgid(n[3]):
                            trace.mutter("found new changeset %s",
                                         mercurial.node.short(n[1]))
                            fetch.add(n[1]) # earliest unknowns
                    for p in n[2:4]:
                        if p not in req and not self._target_overlay.has_hgid(p):
                            r.append(p)
                            req.add(p)
                seen.add(n[0])

            if r:
                for p in xrange(0, len(r), 10):
                    for b in remote.branches(r[p:p+10]):
                        trace.mutter("received %s:%s",
                                     mercurial.node.short(b[0]),
                                     mercurial.node.short(b[1]))
                        unknowns.append(b)

        # do binary search on the branches we found
        while search:
            newsearch = []
            for n, l in zip(search, remote.between(search)):
                l.append(n[1])
                p = n[0]
                f = 1
                for i in l:
                    trace.mutter("narrowing %d:%d %s", f, len(l),
                                 mercurial.node.short(i))
                    if self._target_overlay.has_hgid(i):
                        if f <= 2:
                            trace.mutter("found new branch changeset %s",
                                         mercurial.node.short(p))
                            fetch.add(p)
                        else:
                            trace.mutter("narrowed branch search to %s:%s",
                                          mercurial.node.short(p),
                                          mercurial.node.short(i))
                            newsearch.append((p, i))
                        break
                    p, f = i, f * 2
                search = newsearch
        return fetch

    @needs_write_lock
    def copy_content(self, revision_id=None, basis=None):
        """See InterRepository.copy_content. Partial implementation of that.

        To date the basis parameter is not supported.
        """
        if basis is not None:
            trace.mutter('Ignoring basis argument %r', basis)
        self.target.fetch(self.source, revision_id=revision_id)

    @needs_write_lock
    def fetch(self, revision_id=None, pb=None, find_ghosts=False,
              fetch_spec=None):
        """Fetch revisions. """
        heads = self.heads(fetch_spec, revision_id)
        missing = self.findmissing(heads)
        if not missing:
            return
        cg = self.source._hgrepo.changegroup(missing, 'pull')
        mapping = self.source.get_mapping()
        self.target.start_write_group()
        try:
            self.addchangegroup(cg, mapping)
        except:
            self.target.abort_write_group()
            raise
        else:
            self.target.commit_write_group()

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with HgRepositories."""
        from bzrlib.plugins.hg.repository import (
            HgRepository, )
        return (isinstance(source, HgRepository) and
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
        """Be compatible with HgRepositories."""
        from bzrlib.plugins.hg.repository import HgRepository
        return (isinstance(source, HgRepository) and
                isinstance(target, HgRepository))
