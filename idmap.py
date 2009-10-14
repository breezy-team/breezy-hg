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

"""Access to a map between Bazaar and Mercurial ids."""

import mercurial.node
import os
import threading

from bzrlib import (
    trace,
    )


class Idmap(object):

    def lookup_revision_by_manifest_id(self):
        raise NotImplementedError(self.lookup_revision_by_manifest_id)

    def get_files_by_revid(self, revid):
        raise NotImplementedError(self.get_files_by_revid)

    def revids(self):
        raise NotImplementedError(self.revids)

    def insert_manifest(self, manifest_id, revid):
        raise NotImplementedError(self.insert_manifest)


class MemoryIdmap(Idmap):
    """In-memory idmap implementation."""

    def __init__(self):
        self._manifest_to_revid = {}

    def get_files_by_revid(self, revid):
        raise KeyError(revid)

    def lookup_revision_by_manifest_id(self, manifest_id):
        return self._manifest_to_revid[manifest_id]

    def revids(self):
        return set(self._manifest_to_revid.values())

    def insert_manifest(self, manifest_id, revid):
        if len(manifest_id) == 40:
            manifest_id = mercurial.node.bin(manifest_id)
        self._manifest_to_revid[manifest_id] = revid


_mapdbs = threading.local()
def mapdbs():
    """Get a cache for this thread's db connections."""
    try:
        return _mapdbs.cache
    except AttributeError:
        _mapdbs.cache = {}
        return _mapdbs.cache


TDB_MAP_VERSION = 1
TDB_HASH_SIZE = 50000


class TdbIdmap(Idmap):
    """Idmap that stores in tdb.

    format:
    manifest/<manifest_id> -> revid
    """

    def __init__(self, path=None):
        self.path = path
        if path is None:
            self.db = {}
        else:
            import tdb
            if not mapdbs().has_key(path):
                mapdbs()[path] = tdb.Tdb(path, TDB_HASH_SIZE, tdb.DEFAULT, 
                                          os.O_RDWR|os.O_CREAT)
            self.db = mapdbs()[path]    
        if not "version" in self.db:
            self.db["version"] = str(TDB_MAP_VERSION)
        else:
            if int(self.db["version"]) != TDB_MAP_VERSION:
                trace.warning("SHA Map is incompatible (%s -> %d), rebuilding database.",
                              self.db["version"], TDB_MAP_VERSION)
                self.db.clear()
            self.db["version"] = str(TDB_MAP_VERSION)

    def get_files_by_revid(self, revid):
        raise KeyError(revid)

    def lookup_revision_by_manifest_id(self, manifest_id):
        return self.db["manifest/" + manifest_id]

    def revids(self):
        ret = set()
        for k, v in self.db.iteritems():
            if k.startswith("manifest/"):
                ret.add(v)
        return ret

    def insert_manifest(self, manifest_id, revid):
        if len(manifest_id) == 40:
            manifest_id = mercurial.node.bin(manifest_id)
        self.db["manifest/" + manifest_id] = revid
