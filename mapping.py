# Copyright (C) 2005, 2006, 2009 Canonical Ltd
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
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

"""Mappings."""

import base64
import mercurial
from mercurial.node import (
    hex,
    bin,
    )

from bzrlib import (
    errors,
    foreign,
    revision as _mod_revision,
    )


def files_from_delta(delta, inv, revid):
    """Create a Mercurial-style 'files' set from a Bazaar tree delta.

    :param delta: bzrlib.delta.TreeDelta instance
    :param inv: Inventory
    :param revid: Revision id
    :return: Set with changed files
    """
    ret = set()
    for change in delta.added + delta.removed + delta.modified:
        (path, id, kind) = change[:3]
        if kind not in ('file', 'symlink'):
            continue
        if inv[inv.path2id(path)].revision == revid:
            ret.add(path)
    for (path, id, old_kind, new_kind) in delta.kind_changed:
        if old_kind in ('file', 'symlink') or new_kind in ('file', 'symlink'):
            ret.add(path)
    for (oldpath, newpath, id, kind, text_modified, meta_modified) in delta.renamed:
        if kind in ('file', 'symlink'):
            ret.update([oldpath, newpath])
    return ret


def manifest_and_flags_from_tree(tree, mapping, parent_manifests):
    """Generate a manifest from a Bazaar tree.

    :param tree: Tree
    :param mapping: Bzr<->Hg mapping
    :param parent_manifests: 2-tuple with manifests of tree's parents
    """
    def get_text_parents(path):
        return tuple([m.get(path, mercurial.node.nullid) for m in parent_manifests])
    manifest = {}
    flags = {}
    for path, entry in tree.inventory.iter_entries():
        if entry.kind not in ('file', 'symlink'):
            continue
        if entry.kind == 'symlink':
            flags[path] = 'l'
            manifest[path] = hash(entry.symlink_target, *get_text_parents(path))
        else:
            if entry.executable:
                flags[path] = 'x'
            manifest[path] = hash(tree.get_file_text(entry.file_id), *get_text_parents(path))
    return (manifest, flags)


def escape_path(path):
    """Escape a path for use as a file id.

    :param path: path to escape
    :return: file id
    """
    return path.replace('_', '__').replace('/', '_s').replace(' ', '_w')


def unescape_path(file_id):
    """Unescape a file id created with escape_path().
    
    :param file_id: File id to unescape
    :return: Unescaped path
    """
    ret = []
    i = 0
    while i < len(file_id):
        if file_id[i] != '_':
            ret.append(file_id[i])
        else:
            if file_id[i+1] == '_':
                ret.append("_")
            elif file_id[i+1] == 's':
                ret.append("/")
            elif file_id[i+1] == 'w':
                ret.append(" ")
            else:
                raise ValueError("unknown escape character %s" % file_id[i+1])
            i += 1
        i += 1
    return "".join(ret)


class ExperimentalHgMapping(foreign.VcsMapping):
    """Class that maps between Bazaar and Mercurial semantics."""
    experimental = True
    revid_prefix = "hg-experimental"

    def __init__(self):
        super(ExperimentalHgMapping, self).__init__(foreign_hg)

    @classmethod
    def revision_id_foreign_to_bzr(cls, revision_id):
        if revision_id == mercurial.node.nullid:
            return _mod_revision.NULL_REVISION
        return "%s:%s" % (cls.revid_prefix, hex(revision_id))

    @classmethod
    def revision_id_bzr_to_foreign(cls, revision_id):
        if revision_id == _mod_revision.NULL_REVISION:
            return mercurial.node.nullid
        if not revision_id.startswith("%s:" % cls.revid_prefix):
            raise errors.InvalidRevisionId(revision_id, cls)
        return bin(revision_id[len(cls.revid_prefix)+1:]), cls()

    @classmethod
    def generate_file_id(self, path):
        """Create a synthetic file_id for an hg file."""
        return "hg:" + escape_path(path)

    @classmethod
    def parse_file_id(self, fileid):
        """Parse a file id."""
        if not fileid.startswith("hg:"):
            raise ValueError
        return unescape_path(fileid[len("hg:"):])

    def export_revision(self, rev):
        user = rev.committer.encode("utf-8")
        time = rev.timestamp
        timezone = -rev.timezone
        extra = {}
        for name, value in rev.properties.iteritems():
            if name.startswith("hg:"):
                extra[name[len("hg:"):]] = base64.b64decode(value)
        desc = rev.message.encode("utf-8")
        try:
            manifest = mercurial.node.bin(rev.properties['manifest'])
        except KeyError:
            manifest = None
        return (manifest, user, (time, timezone), desc, extra)

    def import_revision(self, revid, hgrevid, hgparents, manifest, user,
                        (time, timezone), desc, extra):
        result = foreign.ForeignRevision(hgrevid, self, revid)
        result.parent_ids = []
        if hgparents[0] != mercurial.node.nullid:
            result.parent_ids.append(self.revision_id_foreign_to_bzr(hgparents[0]))
        if hgparents[1] != mercurial.node.nullid:
            result.parent_ids.append(self.revision_id_foreign_to_bzr(hgparents[1]))
        result.message = desc.decode("utf-8")
        result.inventory_sha1 = ""
        result.timezone = -timezone
        result.timestamp = time
        result.committer = user.decode("utf-8")
        result.properties = {
                'manifest': mercurial.node.hex(manifest)
                }
        for name, value in extra.iteritems():
            result.properties["hg:" + name] = base64.b64encode(value)
        return result


class HgMappingRegistry(foreign.VcsMappingRegistry):
    """Registry of all Bazaar <-> Mercurial mappings."""

    def revision_id_bzr_to_foreign(self, bzr_revid):
        if not bzr_revid.startswith("hg-"):
            raise errors.InvalidRevisionId(bzr_revid, None)
        (mapping_version, hg_ref) = bzr_revid.split(":", 1)
        mapping = self.get(mapping_version)
        return mapping.revision_id_bzr_to_foreign(bzr_revid)

    parse_revision_id = revision_id_bzr_to_foreign


mapping_registry = HgMappingRegistry()
mapping_registry.register_lazy("hg-experimental", "bzrlib.plugins.hg.mapping",
    "ExperimentalHgMapping")

class ForeignHg(foreign.ForeignVcs):
    """Foreign Mercurial."""

    def __init__(self):
        super(ForeignHg, self).__init__(mapping_registry)

    @classmethod
    def show_foreign_revid(cls, foreign_revid):
        return { "hg commit": hex(foreign_revid) }


foreign_hg = ForeignHg()
default_mapping = ExperimentalHgMapping()
