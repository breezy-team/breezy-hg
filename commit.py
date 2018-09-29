# Copyright (C) 2011 Canonical Ltd
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

"""Commit creation support for Mercurial."""

from breezy import osutils

from breezy.errors import (
    RootMissing,
    )
from breezy.repository import (
    CommitBuilder,
    )

from mercurial.manifest import manifestdict
import mercurial.node


class HgCommitBuilder(CommitBuilder):
    """Commit builder for Mercurial repositories."""

    supports_record_entry_contents = False

    def __init__(self, *args, **kwargs):
        super(HgCommitBuilder, self).__init__(*args, **kwargs)
        self._any_changes = False
        self._changelist = []
        self._removed = []
        self._changed = []
        self._hgrepo = self.repository._hgrepo
        self._transaction = self._hgrepo.transaction("commit")
        self._parent_changeset_ids = []
        self._parent_manifests = []
        self._parent_manifest_ids = []
        self._extra = {}
        self._linkrev = len(self.repository._hgrepo)
        self._validate_revprops(self._revprops)
        for i in range(2):
            if len(self.parents) > i:
                csid = self.repository.lookup_bzr_revision_id(self.parents[i])[0]
                hgchange = self._hgrepo.changelog.read(csid)
                manifest_id = hgchange[0]
                manifest = self._hgrepo.manifestlog[manifest_id].read()
                self._parent_manifest_ids.append(manifest_id)
                self._parent_manifests.append(manifest)
                self._parent_changeset_ids.append(csid)
            else:
                self._parent_manifests.append(manifestdict())
                self._parent_manifest_ids.append(mercurial.node.nullid)
                self._parent_changeset_ids.append(mercurial.node.nullid)
        self._manifest = self._parent_manifests[0].copy()

    def any_changes(self):
        return self._any_changes

    def record_entry_contents(self, ie, parent_invs, path, tree,
        content_summary):
        raise NotImplementedError(self.record_entry_contents)

    def record_delete(self, path, file_id):
        self._any_changes = True
        self._removed.append(path.encode("utf-8"))
        del self._manifest[path.encode("utf-8")]

    def record_iter_changes(self, workingtree, basis_revid, iter_changes):
        seen_root = False
        for (file_id, path, changed_content, versioned, parent, name, kind,
             executable) in iter_changes:
            if kind[1] in ("directory",):
                if kind[0] in ("file", "symlink"):
                    self.record_delete(path[0], file_id)
                if path[1] == u"":
                    seen_root = True
                continue
            if path[1] is None:
                self.record_delete(path[0], file_id)
                continue
            utf8_path = path[1].encode("utf-8")
            fparents = tuple([m.get(utf8_path, mercurial.node.nullid)
                              for m in self._parent_manifests])
            flog = self._hgrepo.file(utf8_path)
            # FIXME: Support copies
            if (changed_content or
                executable[0] != executable[1] or
                kind[0] != kind[1]):
                self._changelist.append(utf8_path)
            if changed_content:
                if kind[1] == "file":
                    text = workingtree.get_file_text(path[1], file_id)
                elif kind[1] == "symlink":
                    text = workingtree.get_symlink_target(path[1], file_id).encode("utf-8")
                else:
                    raise AssertionError
                meta = {} # for now
                node = flog.add(text, meta, self._transaction, self._linkrev,
                        fparents[0], fparents[1])
                self._manifest[utf8_path] = node
            else:
                self._manifest[utf8_path] = fparents[0]
            self._changed.append(utf8_path)
            if executable[1]:
                self._manifest.setflag(utf8_path, 'x')
            if kind[1] == "symlink":
                self._manifest.setflag(utf8_path, 'l')
            f, st = workingtree.get_file_with_stat(path[1], file_id)
            yield file_id, path[1], (osutils.sha_file(f), st)
        if not seen_root and len(self.parents) == 0:
            raise RootMissing()
        self.new_inventory = None

    def get_basis_delta(self):
        # FIXME
        return []

    def finish_inventory(self):
        self._manifest_id = self._hgrepo.manifestlog._revlog.add(self._manifest, self._transaction,
            self._linkrev, self._parent_manifest_ids[0], self._parent_manifest_ids[1],
            self._changed, self._removed)
        self._hgrepo.changelog.delayupdate(self._transaction)

    def commit(self, message):
        self._validate_unicode_text(message, 'commit message')
        # FIXME: set self._extra from self._revprops
        n = self._hgrepo.changelog.add(self._manifest_id,
            self._changelist + self._removed,
            message.encode("utf-8"),
            self._transaction,
            self._parent_changeset_ids[0], self._parent_changeset_ids[1],
            self._committer, (self._timestamp, self._timezone), self._extra)
        mapping = self.repository.get_mapping()
        self._new_revision_id = mapping.revision_id_foreign_to_bzr(mercurial.node.hex(n))
        self.repository.commit_write_group()
        self._transaction.close()
        return self._new_revision_id

    def abort(self):
        if self._transaction:
            self._transaction.release()
        self.repository.abort_write_group()

    def will_record_deletes(self):
        pass

    def revision_tree(self):
        return self.repository.revision_tree(self._new_revision_id)
