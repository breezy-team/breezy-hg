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


class RevlogVersionedFile(VersionedFile):
    """Basic VersionedFile interface implementation that wraps a revlog."""

    def __init__(self, revlog, mapping):
        self._revlog = revlog
        self._mapping = mapping

    def get_record_stream(self, nodes, order, include_delta_closure):
        for (key, ) in nodes:
            hgid, mapping = self._mapping.revision_id_bzr_to_foreign(key)
            node = self._revlog.rev(hgid)
            yield FulltextContentFactory(key, None, None, self._revlog.read(node))

    def keys(self):
        return list(self.iterkeys())

    def iterkeys(self):
        for x in xrange(len(self)):
            yield (self._mapping.revision_id_foreign_to_bzr(self._revlog.node(x)), )

    def __len__(self):
        return len(self._revlog)


class RevlogVersionedFiles(VersionedFiles):
    """Basic VersionedFile interface implementation that wraps a revlog."""

    def __init__(self, opener, mapping):
        self._opener = opener
        self._mapping = mapping

    def _get_revlog(self, fileid):
        path = self._mapping.parse_file_id(fileid)
        return self._opener(path)

    def get_record_stream(self, nodes, ordered, include_delta_closure):
        # FIXME: Sort by fileid ?
        for (fileid, revision) in nodes:
            revlog = self._get_revlog(fileid)
            vf = RevlogVersionedFile(revlog, self._mapping)
            for x in vf.get_record_stream([(revision, )], ordered, 
                                          include_delta_closure):
                yield x


