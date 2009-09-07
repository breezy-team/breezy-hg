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
    VersionedFiles,
    )


class RevlogVersionedFiles(VersionedFiles):

    def __init__(self, revlog, mapping):
        self._revlog = revlog
        self._mapping = mapping

    def keys(self):
        return list(self.iterkeys())

    def iterkeys(self):
        for x in xrange(len(self)):
            yield (self._mapping.revision_id_foreign_to_bzr(self._revlog.node(x)), )

    def __len__(self):
        return len(self._revlog)
