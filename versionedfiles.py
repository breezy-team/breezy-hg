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


from bzrlib.versionedfile import (
    FulltextContentFactory,
    VersionedFile,
    VersionedFiles,
    )

from collections import defaultdict


class RevlogVersionedFile(VersionedFile):
    """Basic VersionedFile interface implementation that wraps a revlog."""

    def __init__(self, revlog):
        self._revlog = revlog

    def _lookup_id(self, key):
        return key

    def _reverse_lookup_id(self, key):
        return key

    def get_record_stream(self, nodes, order, include_delta_closure):
        for (key, ) in nodes:
            hgid = self._lookup_id(key)
            #node = self._revlog.rev(hgid)
            # FIXME: Parents?
            yield FulltextContentFactory((key, ), None, None, self._revlog.revision(hgid))

    def keys(self):
        return list(self.iterkeys())

    def iterkeys(self):
        for x in xrange(len(self)):
            yield (self._reverse_lookup_id(self._revlog.node(x)), )

    def __len__(self):
        return len(self._revlog)


class ChangelogVersionedFile(RevlogVersionedFile):

    def __init__(self, revlog, mapping):
        RevlogVersionedFile.__init__(self, revlog)
        self._mapping = mapping

    def _lookup_id(self, key):
        return self._mapping.revision_id_bzr_to_foreign(key)[0]

    def _reverse_lookup_id(self, key):
        return self._mapping.revision_id_foreign_to_bzr(key)


class ManifestVersionedFile(RevlogVersionedFile):

    def __init__(self, repo, revlog, mapping):
        RevlogVersionedFile.__init__(self, revlog)
        self.repo = repo
        self._mapping = mapping

    def _lookup_id(self, key):
        clid = self._mapping.revision_id_bzr_to_foreign(key)[0]
        return self.repo._hgrepo.changelog.read(clid)[0]

    def _reverse_lookup_id(self, key):
        return self._mapping.revision_id_foreign_to_bzr(self._revlog.linkrev(key))


class RevlogVersionedFiles(VersionedFiles):
    """Basic VersionedFile interface implementation that wraps a revlog."""

    def __init__(self, repo, opener, mapping):
        self.repo = repo
        self._opener = opener
        self._mapping = mapping

    def _get_revlog(self, fileid):
        path = self._mapping.parse_file_id(fileid)
        return self._opener(path)

    def _get_manifest(self, revid):
        hgid, mapping = self.repo.lookup_revision_id(revid)
        manifest_id = self.repo._hgrepo.changelog.read(hgid)[0]
        return self.repo._hgrepo.manifest.read(manifest_id), mapping

    def get_record_stream(self, nodes, ordering, include_delta_closure):
        revisions = defaultdict(set)
        for (fileid, revision) in nodes:
            revisions[revision].add(fileid)
        needed = defaultdict(set)
        for revision, fileids in revisions.iteritems():
            manifest, mapping = self._get_manifest(revision)
            for fileid in fileids:
                path = mapping.parse_file_id(fileid)
                needed[fileid].add((manifest[path], revision))
        for fileid, nodes in needed.iteritems():
            revlog = self._get_revlog(fileid)
            vf = RevlogVersionedFile(revlog)
            for x in vf.get_record_stream([(node, ) for (node, revid) in nodes],
                    'unordered', include_delta_closure):
                x.key = (fileid, revid)
                if x.parents is not None:
                    x.parents = tuple([(fileid, x) for x in x.parents])
                yield x
