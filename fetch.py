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
import itertools
import mercurial.node
import os

from bzrlib import (
    lru_cache,
    osutils,
    trace,
    ui,
    )
from bzrlib.decorators import (
    needs_write_lock,
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
    files_from_delta,
    manifest_and_flags_from_tree,
    )

from bzrlib.plugins.hg.parsers import (
    format_changeset,
    format_manifest,
    parse_changeset,
    unpack_chunk_iter,
    unpack_manifest_chunks,
    )


def inventory_create_directory(directories, basis_inv, other_inv, path,
                               mapping, revid):
    """Make sure a directory and its parents exist.

    :param directories: Dictionary with directories that have already been 
        created as keys, their id as value
    :param basis_inv: Basis inventory against which directories should be 
        created
    :param other_inv: Optional other inventory that could have introduced 
        directories
    :param path: Path of the directory
    :param mapping: Bzr<->Hg mapping to use
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
                            other_inv, os.path.dirname(path), mapping, revid)
    else:
        # Root directory doesn't have a parent id
        ret = []
        parent_id = None
    fileid = mapping.generate_file_id(path)
    ie = InventoryDirectory(fileid, os.path.basename(path), parent_id)
    ie.revision = revid
    ret.append((None, path, fileid, ie))
    directories[path] = fileid
    return ret, fileid


def manifest_to_inventory_delta(mapping, basis_inv, other_inv, 
                                (basis_manifest, basis_flags),
                                (manifest, flags), 
                                revid, files, lookup_metadata,
                                lookup_symlink):
    """Simple O(n) manifest to inventory converter. 

    Does not take renames into account.

    :param mapping: Bzr<->Hg mapping to use
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
    # Set of directories that have been created in this delta
    directories = {}
    # Dictionary of directories that could have been made empty in this delta,
    # with the set of removed children as value.
    maybe_empty_dirs = defaultdict(set)
    for path in set(basis_manifest.keys() + manifest.keys()):
        if (basis_manifest.get(path) == manifest.get(path) and 
            basis_flags.get(path) == flags.get(path)):
            continue
        # Does it still exist in manifest ?
        if path not in manifest:
            # File removed
            file_id = basis_inv.path2id(path)
            if file_id is None:
                raise AssertionError("Removed file %r didn't exist in basis" % path)
            yield (path, None, file_id, None)
            maybe_empty_dirs[os.path.dirname(path)].add(basis_inv[file_id].name)
        else:
            fileid = mapping.generate_file_id(path)
            parent_path, basename = os.path.split(path)
            if basis_inv is not None and basis_inv.has_filename(path):
                old_path = path
                parent_id = basis_inv.path2id(parent_path)
            else:
                old_path = None
                # Make sure parent exists
                extra, parent_id = inventory_create_directory(directories, 
                    basis_inv, other_inv, parent_path, mapping, revid)
                for e in extra:
                    yield e
            f = flags.get(path, "")
            if 'l' in f:
                entry_factory = InventoryLink
            else:
                entry_factory = InventoryFile
            ie = entry_factory(fileid, basename, parent_id)
            ie.executable = ('x' in f)
            if path not in files:
                # Not changed in this revision, so pick one of the parents
                if (manifest.get(path) == basis_manifest.get(path) and 
                    flags.get(path) == basis_flags.get(path)):
                    orig_inv = basis_inv
                else:
                    orig_inv = other_inv
                ie.revision = orig_inv[fileid].revision
                ie.text_sha1 = orig_inv[fileid].text_sha1
                ie.text_size = orig_inv[fileid].text_size
                ie.symlink_target = orig_inv[fileid].symlink_target
            else:
                ie.revision = revid
                ie.text_sha1, ie.text_size = lookup_metadata(
                    (fileid, ie.revision))
                if ie.kind == "symlink":
                    ie.symlink_target = lookup_symlink((fileid, ie.revision))
            yield (old_path, path, fileid, ie)
    # Remove empty directories
    for path in sorted(maybe_empty_dirs.keys(), reverse=True):
        file_id = basis_inv.path2id(path)
        if path == "":
            # Never consider removing the root :-)
            continue
        # Is this directory really empty ?
        if set(basis_inv[file_id].children.keys()) == maybe_empty_dirs[path]:
            yield (path, None, file_id, None)
            maybe_empty_dirs[basis_inv.path2id(os.path.dirname(path))].add(basis_inv[file_id].name)


def create_directory_texts(texts, invdelta):
    """Create the texts for directories mentioned in an inventory delta.

    :param texts: VersionedFiles to add entries to
    :param invdelta: Inventory delta
    """
    stream = []
    for (old_path, new_path, fileid, ie) in invdelta:
        if old_path is None and ie.kind == "directory":
            record = FulltextContentFactory((fileid, ie.revision), None, None,
                                            "")
            record.parents = ()
            stream.append(record)
    texts.insert_record_stream(stream)


class FromHgRepository(InterRepository):
    """Hg to any repository actions."""

    def __init__(self, source, target):
        InterRepository.__init__(self, source, target)
        self._inventories = lru_cache.LRUCache(25)
        self._revisions = {}
        self._files = {}
        self._remember_manifests = defaultdict(lambda: 0)
        self._manifests = {}
        self._text_metadata = {}
        # Map mapping manifest ids to bzr revision ids
        self._manifest2rev_map = defaultdict(set)

    @classmethod
    def _get_repo_format_to_test(self):
        """The format to test with - as yet there is no HgRepoFormat."""
        return None

    def _get_revision(self, revid):
        try:
            return self._revisions[revid]
        except KeyError:
            return self.target.get_revision(revid)

    def _get_manifest_and_flags(self, revid):
        tree = self.target.revision_tree(revid)
        return manifest_and_flags_from_tree(tree, self.source.get_mapping(),
            None) 

    def _get_files(self, revid):
        try:
            return self._files[revid]
        except KeyError:
            delta = self.target.get_revision_delta(revid)
            inv = self.target.get_inventory(revid)
            return files_from_delta(delta, inv, revid)
                

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

    def _import_manifest_delta(self, manifest, manifest_p1, flags, files, rev, 
                               mapping):
        def get_base(node):
            assert self._remember_manifests[node] > 0
            try:
                return self._manifests[node]
            finally:
                self._remember_manifests[node] -= 1
                if self._remember_manifests[node] == 0:
                    del self._manifests[node]
                    del self._remember_manifests[node]
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
            basis_manifest, basis_flags = get_base(manifest_p1)
            if len(rev.parent_ids) == 2:
                other_inv = parent_invs[1]
            else:
                other_inv = None
        return basis_inv, list(manifest_to_inventory_delta(mapping, 
                basis_inv, other_inv, (basis_manifest, basis_flags),
                (manifest, flags), rev.revision_id, files, 
                self._text_metadata.__getitem__,
                self._get_target_fulltext))

    def _get_target_fulltext(self, key):
        stream = self.target.texts.get_record_stream([key], "unordered", True)
        return stream.next().get_bytes_as("fulltext")

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
            for fulltext, hgkey, hgparents in unpack_chunk_iter(chunkiter, get_text):
                for revision, parents in filetext_map[fileid][hgkey].iteritems():
                    key = (fileid, revision)
                    record = FulltextContentFactory(key, None, osutils.sha_string(fulltext), fulltext)
                    record.parents = parents
                    self._text_metadata[key] = (record.sha1, len(fulltext))
                    yield record

    def _add_inventories(self, manifestchunks, mapping, pb):
        total = len(self._revisions)
        # add the actual revisions
        for i, (manifest_id, manifest_parents, manifest, flags) in enumerate(
                unpack_manifest_chunks(manifestchunks, None)):
            pb.update("adding inventories", i, total)
            for revid in self._manifest2rev_map[manifest_id]:
                rev = self._get_revision(revid)
                files = self._get_files(rev.revision_id)
                del self._files[rev.revision_id]
                if rev.parent_ids == []:
                    basis_revid = NULL_REVISION
                else:
                    basis_revid = rev.parent_ids[0]
                basis_inv, invdelta = self._import_manifest_delta(
                    manifest, manifest_parents[0], flags, files, 
                    rev, mapping)
                create_directory_texts(self.target.texts, invdelta)
                (validator, new_inv) = self.target.add_inventory_by_delta(
                    basis_revid, invdelta, rev.revision_id, rev.parent_ids, 
                    basis_inv)
                self._inventories[rev.revision_id] = new_inv
                self.target.add_revision(rev.revision_id, rev, new_inv)
                del self._revisions[rev.revision_id]
                if self._remember_manifests[manifest_id] > 0:
                    self._manifests[manifest_id] = (manifest, flags)
        del self._remember_manifests[mercurial.node.nullid]
        if len([x for x,n in self._remember_manifests.iteritems() if n > 1]) > 0:
            raise AssertionError("%r not empty" % self._remember_manifests)

    def _unpack_changesets(self, chunkiter, mapping, pb):
        def get_hg_revision(hgid):
            revid = mapping.revision_id_foreign_to_bzr(hgid)
            rev = self._get_revision(revid)
            (manifest, user, (time, timezone), desc, extra) = \
                mapping.export_revision(rev)
            # TODO: For now we always know that a manifest id was stored since 
            # we don't support roundtripping into Mercurial yet. When we do, 
            # we need a fallback mechanism to determine the manifest id.
            if manifest is None:
                raise AssertionError
            files = self._get_files(revid)
            return format_changeset(manifest, files, user, (time, timezone),
                                    desc, extra)
        for i, (fulltext, hgkey, hgparents) in enumerate(
                unpack_chunk_iter(chunkiter, get_hg_revision)):
            pb.update("fetching changesets", i)
            (manifest, user, (time, timezone), files, desc, extra) = \
                parse_changeset(fulltext)
            key = mapping.revision_id_foreign_to_bzr(hgkey)
            self._files[key] = files
            rev = mapping.import_revision(key, hgkey,
                hgparents, manifest, user, (time, timezone), desc, extra)
            self._manifest2rev_map[manifest].add(key)
            self._revisions[rev.revision_id] = rev

    def _unpack_manifests(self, chunkiter, mapping, pb):
        """Unpack the manifest deltas.

        :param chunkiter: Iterator over delta chunks for the manifest.
        :param mapping: Bzr<->Hg mapping
        :param pb: Progress bar
        """
        def get_manifest_text(node):
            revid = self._manifest2rev_map[node].next()
            return format_manifest(*self._get_manifest_and_flags(revid))
        filetext_map = defaultdict(lambda: defaultdict(dict))
        for i, (hgkey, hgparents, manifest, flags) in enumerate(
                unpack_manifest_chunks(chunkiter, get_manifest_text)):
            pb.update("fetching manifests", i, len(self._revisions))
            for revid in self._manifest2rev_map[hgkey]:
                for path in self._get_files(revid):
                    fileid = mapping.generate_file_id(path)
                    if not path in manifest:
                        # Path still has to actually exist..
                        continue
                    filetext_map[fileid][manifest[path]][revid] = () #FIXME
                self._remember_manifests[hgparents[0]] += 1
        return filetext_map

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
        manifestchunks1, manifestchunks2 = itertools.tee(
            mercurial.changegroup.chunkiter(cg))
        pb = ui.ui_factory.nested_progress_bar()
        try:
            filetext_map = self._unpack_manifests(manifestchunks1, mapping, pb)
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
            self._add_inventories(manifestchunks2, mapping, pb)
        finally:
            pb.finished()

    def get_target_heads(self):
        """Determine the heads in the target repository."""
        all_revs = self.target.all_revision_ids()
        parent_map = self.target.get_parent_map(all_revs)
        all_parents = set()
        map(all_parents.update, parent_map.itervalues())
        mapping = self.source.get_mapping()
        return set([mapping.revision_id_bzr_to_foreign(revid)[0] for revid in set(all_revs) - all_parents])

    def heads(self, fetch_spec, revision_id):
        """Determine the Mercurial heads to fetch. """
        if fetch_spec is not None:
            mapping = self.source.get_mapping()
            return [mapping.revision_id_bzr_to_foreign(head)[0] for head in fetch_spec.heads]
        if revision_id is not None:
            mapping = self.source.get_mapping()
            return [mapping.revision_id_bzr_to_foreign(revision_id)[0]]
        return self.source._hgrepo.heads()

    def has_hgid(self, id):
        """Check whether a Mercurial revision id is present in the target.
        
        :param id: Mercurial ID
        :return: boolean
        """
        if id == mercurial.node.nullid:
            return True
        if len(self.has_hgids([id])) == 1:
            return True
        return False

    def has_hgids(self, ids):
        """Check whether the specified Mercurial ids are present.
        
        :param ids: Mercurial revision ids
        :return: Set with the revisions that were present
        """
        mapping = self.source.get_mapping()
        revids = set([mapping.revision_id_foreign_to_bzr(h) for h in ids])
        return set([
            mapping.revision_id_bzr_to_foreign(revid) 
            for revid in self.target.has_revisions(revids)])

    def findmissing(self, heads):
        """Find the set of ancestors of heads missing from target.
        
        :param heads: Mercurial heads to check for.

        Based on mercurial.localrepo.localrepository.findcommonincoming
        """
        unknowns = set(heads) - self.has_hgids(heads)
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
                elif n[1] and self.has_hgid(n[1]): # do we know the base?
                    trace.mutter("found incomplete branch %s:%s", 
                        mercurial.node.short(n[0]), mercurial.node.short(n[1]))
                    search.append(n[0:2]) # schedule branch range for scanning
                    seenbranch.add(n)
                else:
                    if n[1] not in seen and n[1] not in fetch:
                        if self.has_hgid(n[2]) and self.has_hgid(n[3]):
                            trace.mutter("found new changeset %s",
                                         mercurial.node.short(n[1]))
                            fetch.add(n[1]) # earliest unknowns
                    for p in n[2:4]:
                        if p not in req and not self.has_hgid(p):
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
                    if self.has_hgid(i):
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

        To date the revision_id and basis parameters are not supported.
        """
        if revision_id is not None:
            raise AssertionError("revision_id not supported")
        if basis is not None:
            trace.mutter('Ignoring basis argument %r', basis)
        self.target.fetch(self.source)

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
        finally:
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
