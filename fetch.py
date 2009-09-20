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
# parse_changeset is based on
#       mercurial.changelog.changelog.read
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
# Published under the GNU GPLv2 or later

"""Inter-repository operations involving Mercurial repositories."""

from collections import defaultdict
import mercurial.encoding
import mercurial.node
import mercurial.util
import os
import struct

from bzrlib import (
    trace,
    )
from bzrlib.decorators import (
    needs_write_lock,
    )
from bzrlib.inventory import (
    Inventory,
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
    if basis_inv.has_filename(path):
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
                                revid, files, lookup_metadata):
    """Simple O(n) manifest to inventory converter. 

    Does not take renames into account.
    """
    # Set of directories that have been created in this delta
    directories = {}
    potential_removable_directories = set()
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
            potential_removable_directories.add(os.path.dirname(path))
        else:
            fileid = mapping.generate_file_id(path)
            parent_path, basename = os.path.split(path)
            if basis_inv.has_filename(path):
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
                ie = InventoryLink(fileid, basename, parent_id)
            else:
                ie = InventoryFile(fileid, basename, parent_id)
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
            else:
                ie.revision = revid
                ie.text_sha1, ie.text_size = lookup_metadata(
                    (fileid, ie.revision))
            yield (old_path, path, fileid, ie)
    # FIXME: Remove empty directories
    for path in sorted(potential_removable_directories, reverse=True):
        # Is this directory really empty ?
        pass # yield (path, None, basis_inv.path2id(path), None)


def format_changeset(manifest, files, user, date, desc, extra):
    """Serialize a Mercurial changeset.

    :param manifest: Manifest ID for this changeset, as 20-byte string
    :param files: Array of the files modified by this changeset
    :param user: Name + email of the committer
    :param date: Date of the commit
    :param desc: Commit message
    :param extra: Dictionary with extra revision properties
    :return: String with formatted revision
    """
    user = user.strip()
    # An empty username or a username with a "\n" will make the
    # revision text contain two "\n\n" sequences -> corrupt
    # repository since read cannot unpack the revision.
    if not user:
        raise AssertionError("empty username")
    if "\n" in user:
        raise AssertionError("username %s contains a newline" % repr(user))

    # strip trailing whitespace and leading and trailing empty lines
    desc = '\n'.join([l.rstrip() for l in desc.splitlines()]).strip('\n')

    user = mercurial.encoding.fromlocal(user)
    desc = mercurial.encoding.fromlocal(desc)

    if not isinstance(date, tuple):
        raise AssertionError("date is not a tuple")
    parseddate = "%d %d" % date
    if extra and extra.get("branch") in ("default", ""):
        del extra["branch"]
    if extra:
        extra = mercurial.changelog.encodeextra(extra)
        parseddate = "%s %s" % (parseddate, extra)
    l = [mercurial.node.hex(manifest), user, parseddate] + \
        sorted(files) + ["", desc]
    return "\n".join(l)


def parse_changeset(text):
    """Parse a Mercurial changeset.
    
    :param text: Text to parse
    :return: Tuple with (manifest, user, (time, timezone), files, desc, extra)
    """
    last = text.index("\n\n")
    desc = mercurial.encoding.tolocal(text[last + 2:])
    l = text[:last].split('\n')
    manifest = mercurial.node.bin(l[0])
    user = mercurial.encoding.tolocal(l[1])

    extra_data = l[2].split(' ', 2)
    if len(extra_data) != 3:
        time = float(extra_data.pop(0))
        try:
            # various tools did silly things with the time zone field.
            timezone = int(extra_data[0])
        except:
            timezone = 0
        extra = {}
    else:
        time, timezone, extra = extra_data
        time, timezone = float(time), int(timezone)
        extra = mercurial.changelog.decodeextra(extra)
    files = l[3:]
    return (manifest, user, (time, timezone), files, desc, extra)


def unpack_chunk_iter(chunks, mapping, lookup_base):
    fulltext_cache = {}
    base = None
    for chunk in chunks:
        node, p1, p2, cs = struct.unpack("20s20s20s20s", chunk[:80])
        if base is None:
            base = p1
        delta = buffer(chunk, 80)
        del chunk
        if base == mercurial.node.nullid:
            textbase = ""
        else:
            try:
                textbase = fulltext_cache[base]
            except KeyError:
                textbase = lookup_base(base)
        fulltext = mercurial.mdiff.patches(textbase, [delta])
        yield fulltext, node, (p1, p2)
        fulltext_cache[node] = fulltext
        base = node


def get_changed_files(inventory):
    ret = []
    for path, entry in inventory.iter_entries():
        if entry.revision == inventory.revision_id:
            ret.append(path)
    return ret


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
        self._inventories = {}
        self._revisions = {}
        self._files = {}
        self._manifest_texts = {}
        self._manifests = {}
        self._manifest_order = []
        self._text_metadata = {}

    @classmethod
    def _get_repo_format_to_test(self):
        """The format to test with - as yet there is no HgRepoFormat."""
        return None

    def _get_revision(self, revid):
        try:
            return self._revisions[revid]
        except KeyError:
            return self.target.get_revision(revid)

    def _get_files(self, revid):
        try:
            return self._files[revid]
        except KeyError:
            inv = self._get_inventory(revid)
            return get_changed_files(inv)

    def _get_inventory(self, revid):
        try:
            return self._inventories[revid]
        except KeyError:
            return self.target.get_inventory(revid)

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

    def _get_manifest(self, node):
        return self._manifests[node]

    def _get_manifest_text(self, node):
        try:
            return self._manifest_texts[node]
        except KeyError:
            raise NotImplementedError(self._get_manifest_text)

    def _import_manifest_delta(self, manifest, flags, files, rev, mapping):
        parent_invs = self._get_inventories(rev.parent_ids)
        assert len(rev.parent_ids) in (0, 1, 2)
        if len(rev.parent_ids) == 0:
            basis_inv = Inventory(root_id=None)
            other_inv = None
            basis_manifest = {}
            basis_flags = {}
        else:
            basis_inv = parent_invs[0]
            basis_manifest, basis_flags = self._get_manifest(
                self._rev2manifest_map[rev.parent_ids[0]])
            if len(rev.parent_ids) == 2:
                other_inv = parent_invs[1]
            else:
                other_inv = None
        return list(manifest_to_inventory_delta(mapping, basis_inv, other_inv,
                (basis_manifest, basis_flags),
                (manifest, flags), rev.revision_id, files, 
                self._text_metadata.__getitem__))

    def _get_hg_revision(self, mapping, hgid):
        revid = mapping.revision_id_foreign_to_bzr(hgid)
        rev = self._get_revision(revid)
        (manifest, user, (time, timezone), desc, extra) = \
            mapping.export_revision(rev)
        # FIXME: For now we always know that a manifest id was stored since 
        # we don't support roundtripping into Mercurial yet. When we do, 
        # we need a fallback mechanism to determine the manifest id.
        assert manifest is not None
        files = self._get_files(revid)
        return format_changeset(manifest, files, user, (time, timezone), desc,
                                extra)

    def addchangegroup(self, cg, mapping):
        """Import a Mercurial changegroup into the target repository.
        
        :param cg: Changegroup to add
        :param mapping: Mercurial mapping
        """
        # Changeset
        chunkiter = mercurial.changegroup.chunkiter(cg)
        # Map mapping manifest ids to bzr revision ids
        self._manifest2rev_map = defaultdict(set)
        self._rev2manifest_map = {}
        for fulltext, hgkey, hgparents in unpack_chunk_iter(chunkiter, mapping,
                lambda node: self._get_hg_revision(mapping, node),
                ):
            (manifest, user, (time, timezone), files, desc, extra) = \
                parse_changeset(fulltext)
            key = mapping.revision_id_foreign_to_bzr(hgkey)
            self._files[key] = files
            rev = mapping.import_revision(key, hgkey,
                hgparents, manifest, user, (time, timezone), desc, extra)
            self._manifest2rev_map[manifest].add(key)
            self._rev2manifest_map[key] = manifest
            self._revisions[rev.revision_id] = rev
        # Manifest
        chunkiter = mercurial.changegroup.chunkiter(cg)
        filetext_map = defaultdict(lambda: defaultdict(set))
        for fulltext, hgkey, hgparents in unpack_chunk_iter(chunkiter, mapping, 
            self._get_manifest_text):
            manifest = mercurial.manifest.manifestdict()
            flags = {}
            mercurial.parsers.parse_manifest(manifest, flags, 
                fulltext)
            self._manifest_texts[hgkey] = fulltext
            self._manifests[hgkey] = (manifest, flags)
            for revid in self._manifest2rev_map[hgkey]:
                for path in self._get_files(revid):
                    fileid = mapping.generate_file_id(path)
                    if path in manifest:
                        # Path still has to actually exist..
                        filetext_map[fileid][manifest[path]].add(revid)
            self._manifest_order.append(hgkey)
        def get_text(fileid, node):
            key = iter(filetext_map[fileid][node]).next()
            stream = self.target.texts.get_record_stream([key],
                "unordered", True)
            return stream.next().get_bytes_as("fulltext")
        # Texts
        while 1:
            f = mercurial.changegroup.getchunk(cg)
            if not f:
                break
            fileid = mapping.generate_file_id(f)
            chunkiter = mercurial.changegroup.chunkiter(cg)
            # FIXME: Iterator rather than list:
            stream = []
            for fulltext, hgkey, hgparents in unpack_chunk_iter(chunkiter,
                mapping, lambda node: get_text(fileid, node)):
                for revision in filetext_map[fileid][hgkey]:
                    key = (fileid, revision)
                    record = FulltextContentFactory(key, None, None, fulltext)
                    record.parents = () #FIXME?
                    self._text_metadata[key] = (record.sha1, len(record.get_bytes_as('fulltext')))
                    stream.append(record)
            self.target.texts.insert_record_stream(stream)
        # add the actual revisions
        for manifest_id in self._manifest_order:
            manifest, flags = self._get_manifest(manifest_id)
            for revid in self._manifest2rev_map[manifest_id]:
                rev = self._get_revision(revid)
                files = self._get_files(rev.revision_id)
                if rev.parent_ids == []:
                    basis_revid = NULL_REVISION
                else:
                    basis_revid = rev.parent_ids[0]
                invdelta = self._import_manifest_delta(manifest, flags, files,
                                                       rev, mapping)
                create_directory_texts(self.target.texts, invdelta)
                trace.mutter("INVDELTA: %r", invdelta)
                (validator, new_inv) = self.target.add_inventory_by_delta(
                    basis_revid, invdelta, rev.revision_id, rev.parent_ids)
                self.target.add_revision(rev.revision_id, rev, new_inv)

    def get_target_heads(self):
        """Determine the heads in the target repository."""
        # FIXME: This should be more efficient
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
