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
    errors,
    registry,
    trace,
    )
from bzrlib.transport import (
    get_transport,
    )

from bzrlib.plugins.hg.mapping import (
    mapping_registry,
    )

def get_cache_dir():
    try:
        from xdg.BaseDirectory import xdg_cache_home
    except ImportError:
        from bzrlib.config import config_dir
        ret = os.path.join(config_dir(), "hg")
    else:
        ret = os.path.join(xdg_cache_home, "bazaar", "hg")
    if not os.path.isdir(ret):
        os.makedirs(ret)
    return ret


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


def check_pysqlite_version(sqlite3):
    """Check that sqlite library is compatible.

    """
    if (sqlite3.sqlite_version_info[0] < 3 or
            (sqlite3.sqlite_version_info[0] == 3 and
             sqlite3.sqlite_version_info[1] < 3)):
        trace.warning('Needs at least sqlite 3.3.x')
        raise errors.BzrError("incompatible sqlite library")

try:
    try:
        import sqlite3
        check_pysqlite_version(sqlite3)
    except (ImportError, errors.BzrError), e:
        from pysqlite2 import dbapi2 as sqlite3
        check_pysqlite_version(sqlite3)
except:
    trace.warning('Needs at least Python2.5 or Python2.4 with the pysqlite2 '
            'module')
    raise errors.BzrError("missing sqlite library")


class BzrHgIdmap(object):
    """Caching backend."""

    def lookup_revision_by_manifest_id(self):
        raise NotImplementedError(self.lookup_revision_by_manifest_id)

    def lookup_changeset_id_by_revid(self, revid):
        raise NotImplementedError(self.lookup_changeset_id_by_revid)

    def get_files_by_revid(self, revid):
        raise NotImplementedError(self.get_files_by_revid)

    def revids(self):
        raise NotImplementedError(self.revids)

    def insert_revision(self, revid, manifest_id, changeset_id, mapping):
        raise NotImplementedError(self.insert_revision)


class MemoryIdmap(BzrHgIdmap):
    """In-memory idmap implementation."""

    def __init__(self):
        self._manifest_to_revid = {}
        self._revid_to_changeset_id = {}

    def get_files_by_revid(self, revid):
        raise KeyError(revid)

    def lookup_revision_by_manifest_id(self, manifest_id):
        return self._manifest_to_revid[manifest_id]

    def lookup_changeset_id_by_revid(self, revid):
        return self._revid_to_changeset_id[revid]

    def revids(self):
        return set(self._manifest_to_revid.values())

    def insert_revision(self, revid, manifest_id, changeset_id, mapping):
        if len(manifest_id) == 40:
            manifest_id = mercurial.node.bin(manifest_id)
        if len(changeset_id) == 40:
            changeset_id = mercurial.node.bin(changeset_id)
        self._manifest_to_revid[manifest_id] = revid
        self._revid_to_changeset_id[revid] = changeset_id, mapping


class TdbIdmap(BzrHgIdmap):
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
        try:
            if int(self.db["version"]) != TDB_MAP_VERSION:
                trace.warning("SHA Map is incompatible (%s -> %d), rebuilding database.",
                              self.db["version"], TDB_MAP_VERSION)
                self.db.clear()
                self.db["version"] = str(TDB_MAP_VERSION)
        except KeyError:
            self.db["version"] = str(TDB_MAP_VERSION)

    def get_files_by_revid(self, revid):
        raise KeyError(revid)

    def lookup_revision_by_manifest_id(self, manifest_id):
        return self.db["manifest/" + manifest_id]

    def lookup_changeset_id_by_revid(self, revid):
        text = self.db["revid/" + revid]
        csid = text[:20]
        return csid, mapping_registry.get(text[20:])

    def revids(self):
        ret = set()
        for k in self.db.iterkeys():
            if k.startswith("manifest/"):
                ret.add(self.db[k])
        return ret

    def insert_revision(self, revid, manifest_id, changeset_id, mapping):
        if len(manifest_id) == 40:
            manifest_id = mercurial.node.bin(manifest_id)
        if len(changeset_id) == 40:
            changeset_id = mercurial.node.bin(changeset_id)
        self.db["manifest/" + manifest_id] = revid
        self.db["revid/" + revid] = changeset_id + str(mapping)



class SqliteIdmap(BzrHgIdmap):
    """Idmap that stores in SQLite.
    """

    def __init__(self, path=None):
        if path is None:
            self.db = sqlite3.connect(":memory:")
            self.db.text_factory = str
        else:
            if not mapdbs().has_key(path):
                mapdbs()[path] = sqlite3.connect(path)
                mapdbs()[path].text_factory = str
            self.db = mapdbs()[path]
        self.db.executescript("""
        create table if not exists revision (
            revid text not null,
            csid text not null check(length(csid) == 40),
            manifest_id text not null check(length(manifest_id) == 40),
            mapping text not null
        );
        create unique index if not exists revision_revid on revision(revid);
        create unique index if not exists revision_csid on revision(csid, mapping);
        create index if not exists revision_manifest on revision(manifest_id);
        """)

    def get_files_by_revid(self, revid):
        raise KeyError(revid)

    def lookup_revision_by_manifest_id(self, manifest_id):
        if len(manifest_id) == 20:
            manifest_id = mercurial.node.hex(manifest_id)
        row = self.db.execute("select revid from revision where manifest_id = ?", (manifest_id,)).fetchone()
        if row is not None:
            return row[0]
        raise KeyError

    def lookup_changeset_id_by_revid(self, revid):
        row = self.db.execute("select csid, mapping from revision where revid = ?").fetchone()
        if row is not None:
            return row[0], mapping_registry.get(row[1])
        raise KeyError

    def revids(self):
        ret = set()
        ret.update((row for 
            (row,) in self.db.execute("select revid from revision")))
        return ret

    def insert_revision(self, revid, manifest_id, changeset_id, mapping):
        if len(manifest_id) == 20:
            manifest_id = mercurial.node.hex(manifest_id)
        if len(changeset_id) == 20:
            changeset_id = mercurial.node.hex(changeset_id)
        if len(changeset_id) != 40:
            raise AssertionError
        if len(manifest_id) != 40:
            raise AssertionError
        self.db.execute("insert into revision (revid, csid, manifest_id, mapping) values (?, ?, ?, ?)", (revid, changeset_id, manifest_id, str(mapping)))


class BzrHgCacheFormat(object):
    """Bazaar-Hg Cache Format."""

    def get_format_string(self):
        """Return a single-line unique format string for this cache format."""
        raise NotImplementedError(self.get_format_string)

    def open(self, transport):
        """Open this format on a transport."""
        raise NotImplementedError(self.open)

    def initialize(self, transport):
        """Create a new instance of this cache format at transport."""
        transport.put_bytes('format', self.get_format_string())

    @classmethod
    def from_transport(self, transport):
        """Open a cache file present on a transport, or initialize one.

        :param transport: Transport to use
        :return: A BzHgCache instance
        """
        try:
            format_name = transport.get_bytes('format')
            format = formats.get(format_name)
        except errors.NoSuchFile:
            format = formats.get('default')
            format.initialize(transport)
        return format.open(transport)

    @classmethod
    def from_repository(cls, repository):
        """Open a cache file for a repository.

        This will use the repository's transport to store the cache file, or
        use the users global cache directory if the repository has no 
        transport associated with it.

        :param repository: Repository to open the cache for
        :return: A `BzrHgCache`
        """
        repo_transport = getattr(repository, "_transport", None)
        if repo_transport is not None:
            # Even if we don't write to this repo, we should be able
            # to update its cache.
            repo_transport = remove_readonly_transport_decorator(repo_transport)
            try:
                repo_transport.mkdir('hg')
            except errors.FileExists:
                pass
            transport = repo_transport.clone('hg')
        else:
            transport = get_remote_cache_transport()
        return cls.from_transport(transport)


class SqliteBzrHgCacheFormat(BzrHgCacheFormat):

    def get_format_string(self):
        return 'bzr-hg sqlite cache v1\n'

    def open(self, transport):
        return SqliteIdmap(transport.local_abspath('idmap.db'))


class TdbBzrHgCacheFormat(BzrHgCacheFormat):

    def get_format_string(self):
        return 'bzr-hg tdb cache v1\n'

    def open(self, transport):
        return TdbIdmap(transport.local_abspath('idmap.tdb'))


formats = registry.Registry()
formats.register(TdbBzrHgCacheFormat().get_format_string(),
    TdbBzrHgCacheFormat())
formats.register(SqliteBzrHgCacheFormat().get_format_string(),
    SqliteBzrHgCacheFormat())
try:
    import tdb
except ImportError:
    formats.register('default', SqliteBzrHgCacheFormat())
else:
    formats.register('default', TdbBzrHgCacheFormat())


def get_remote_cache_transport():
    return get_transport(get_cache_dir())


def remove_readonly_transport_decorator(transport):
    if transport.is_readonly():
        return transport._decorated
    return transport


def migrate_ancient_formats(repo_transport):
    # Prefer migrating hg-v2.db over hg.tdb, since the latter may not
    # be openable on some platforms.
    if repo_transport.has("hg-v2.db"):
        SqliteBzrHgCacheFormat().initialize(repo_transport.clone("hg"))
        repo_transport.rename("hg-v2.db", "hg/idmap.db")
    elif repo_transport.has("hg.tdb"):
        TdbBzrHgCacheFormat().initialize(repo_transport.clone("hg"))
        repo_transport.rename("hg.tdb", "hg/idmap.tdb")


def from_repository(repository):
    """Open a cache file for a repository.

    If the repository is remote and there is no transport available from it
    this will use a local file in the users cache directory
    (typically ~/.cache/bazaar/hg/)

    :param repository: A repository object
    """
    repo_transport = getattr(repository, "_transport", None)
    if repo_transport is not None:
        # Migrate older cache formats
        repo_transport = remove_readonly_transport_decorator(repo_transport)
        try:
            repo_transport.mkdir("hg")
        except errors.FileExists:
            pass
        else:
            migrate_ancient_formats(repo_transport)
    return BzrHgCacheFormat.from_repository(repository)
