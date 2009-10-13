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

from itertools import izip


def drevisions(repo, mapping, revids, files, manifests):
    """Serialize a series of Bazaar revisions as Mercurial changesets.

    :param repo: Bazaar repository
    :param mapping: Bzr<->Hg Mapping
    :param revids: Iterable over revision ids
    :param files: Dictionary for looking up the set of changed files by revid
    :param manifests: Dictionary for looking up the manifest id by revid
    :return: Iterable over changeset fulltexts
    """
    for revid in revids:
        rev = repo.get_revision(revid)
        (manifest, user, date, desc, extra) = mapping.export_revision(rev)
        if manifest is None:
            manifest = manifests[revid]
        assert manifest == manifests[revid]
        yield format_changeset(manifest, files, user, date, desc, extra)


def dinventories(repo, mapping, revids, manifests, files):
    def store_text(path, fulltext, (p1, p2)):
        return hash(fulltext, p1, p2)
    def get_text_parents(manifests, revids):
        ret = [manifests[revid].get(path, mercurial.node.nullid) for revid in revids[:2]]
        while len(ret) < 2:
            ret.append(mercurial.node.nullid)
        return tuple(ret)
    # TODO: Very naive and slow:
    for revid, inv in izip(revids, repo.iter_inventories(revids)):
        manifest = {}
        flags = {}
        for path, entry in inv.iter_entries():
            if entry.kind not in ('file', 'symlink'):
                continue
            if entry.revision == revid:
                files[revid].add(path)
            if entry.kind == 'symlink':
                flags[path] = 'l'
                manifest[path] = store_text(path, entry.symlink_target, 
                    get_text_parents(manifests, path, repo.revision_parents(revid)))
            else:
                if entry.executable:
                    flags[path] = 'x'
                record = repo.texts.get_record_stream([(entry.fileid, entry.revision)], 
                    'unordered', True).next()
                manifest[path] = store_text(path, record.get_bytes_as('fulltext'), 
                    get_text_parents(manifests, path, repo.revision_parents(revid)))
        manifests[revid] = manifest


def dchangegroup(repo, mapping, revids):
    """Create a changegroup based on (a derivation) of a set of revisions.

    :param repo: Bazaar repository to retrieve the revisions from
    :param revids: Iterable over the revision ids of the revisions to group
    :return: changegroup string
    """
    dinventories(repo, mapping, revids)
          
    # FIXME: walk over inventories
    #  - add manifest
    #  - keep track of what files to fetch
    # Generate changesets
    drevisions(repo, mapping, revids, files, manifests)
    return StringIO(), {}
