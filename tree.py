# Copyright (C) 2011 Canonical Ltd.
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

"""Mercurial Repository revision tree."""

from breezy import (
    errors,
    osutils,
    )

from breezy.tree import (
    TreeDirectory,
    TreeFile,
    TreeLink,
    )
from breezy.revision import (
    NULL_REVISION,
    )

from breezy.revisiontree import RevisionTree

import mercurial.node

import posixpath


class HgTreeDirectory(TreeDirectory):

    def __init__(self, file_id, basename, parent_id):
        self.file_id = file_id
        self.name = basename
        self.parent_id = parent_id


class HgTreeLink(TreeLink):

    def __init__(self, file_id, basename, parent_id):
        self.file_id = file_id
        self.name = basename
        self.parent_id = parent_id


class HgTreeFile(TreeFile):

    def __init__(self, file_id, basename, parent_id):
        self.file_id = file_id
        self.name = basename
        self.parent_id = parent_id


class HgRevisionTree(RevisionTree):
    """Mercurial Revision tree."""

    def __init__(self, repository, revision_id, hgid, manifest, mapping):
        self._repository = repository
        self._revision_id = revision_id
        self._manifest = manifest
        self._mapping = mapping
        self._ancestry_cache = {}
        self._hgid = hgid
        # each directory is a key - i.e. 'foo'
        # the value is the current chosen revision value for it.
        self._directories = {}
        self._known_manifests = {}
        self._all_relevant_revisions = None

    def _has_directory(self, path):
        # FIXME
        return True

    def get_root_id(self):
        return self.path2id("")

    def get_revision_id(self):
        return self._revision_id

    def _comparison_data(self, entry, path):
        if entry is None:
            return None, False, None
        return entry.kind, entry.executable, None

    def is_executable(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        return ('x' in self._manifest(path.encode("utf-8")))

    def get_symlink_target(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        encoded_path = path.encode("utf-8")
        revlog = self._repository._hgrepo.file(encoded_path)
        return revlog.read(self._manifest[encoded_path])

    def get_file_text(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        revlog = self._repository._hgrepo.file(path)
        return revlog.read(self._manifest[path.encode("utf-8")])

    def get_file_sha1(self, file_id, path=None, stat_value=None):
        return osutils.sha_string(self.get_file_text(file_id, path))

    def get_file_mtime(self, file_id, path=None):
        revid = self.get_file_revision(file_id, path)
        return self._repository.get_revision(revid).timestamp

    def kind(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        if 'l' in self._manifest(path.encode("utf-8")):
            return 'symlink'
        else:
            return 'file'

    def path2id(self, path):
        if self._mapping is None:
            return None
        return self._mapping.generate_file_id(path.encode("utf-8"))

    def id2path(self, file_id):
        if self._mapping is None:
            raise errors.NoSuchId(self, file_id)
        return self._mapping.parse_file_id(file_id).decode("utf-8")

    def _get_dir_ie(self, path):
        if path == "":
            parent_id = None
        else:
            parent_id = self.path2id(posixpath.dirname(path))
        ie = HgTreeDirectory(self.path2id(path), posixpath.basename(path), parent_id)
        ie.revision = self._get_dir_last_modified(path)
        return ie

    def _get_file_ie(self, path, flags):
        file_id = self.path2id(path)
        parent_id = self.path2id(posixpath.dirname(path))
        if 'l' in flags:
            ie = TreeLink(file_id, posixpath.basename(path), parent_id)
            ie.symlink_target = self.get_symlink_target(file_id, path)
        else:
            ie = TreeFile(file_id, posixpath.basename(path), parent_id)
            text = self.get_file_text(file_id, path)
            ie.text_sha1 = osutils.sha_string(text)
            ie.text_size = len(text)
            ie.executable = ('x' in flags)
        ie.revision = self.get_file_revision(file_id, path)
        return ie

    def iter_entries_by_dir(self, specific_files=None, yield_parents=False):
        # FIXME: Support specific_file_ids and yield_parents
        if specific_files is not None:
            raise NotImplementedError(self.iter_entries_by_dir)
        directories = set()
        for p in self._manifest:
            parent = posixpath.dirname(p)
            while not parent in directories:
                decoded_parent = parent.decode("utf-8")
                yield decoded_parent, self._get_dir_ie(decoded_parent)
                directories.add(parent)
                if parent == "":
                    break
                parent = posixpath.dirname(parent)
            fflags = self._manifest.flags(p)
            decoded_path = p.decode("utf-8")
            yield decoded_path, self._get_file_ie(decoded_path, fflags)

    def _pick_best_creator_revision(self, revision_a, revision_b):
        """Picks the best creator revision from a and b.

        If a is an ancestor of b, a wins, and vice verca.
        If neither is an ancestor of the other, the lowest value wins.
        """
        # TODO make this much faster - use a local cache of the ancestry
        # sets.
        if revision_a in self._get_ancestry(revision_b):
            return revision_a
        elif revision_b in self._get_ancestry(revision_a):
            return revision_b
        elif revision_a < revision_b:
            return revision_a
        else:
            return revision_b

    def _get_ancestry(self, some_revision_id):
        try:
            return self._ancestry_cache[some_revision_id]
        except KeyError:
            pass
        if self._all_relevant_revisions is None:
            graph = self._repository.get_graph()
            ancestry = graph.find_unique_ancestors(self._revision_id, [])
            self._all_relevant_revisions = self._repository.get_parent_map(ancestry)
        assert some_revision_id in self._all_relevant_revisions
        ancestry = set()
        # add what can be reached from some_revision_id
        # TODO: must factor this trivial iteration in breezy.graph cleanly.
        pending = set([some_revision_id])
        while len(pending) > 0:
            node = pending.pop()
            ancestry.add(node)
            for parent_id in self._all_relevant_revisions[node]:
                if parent_id not in ancestry:
                    pending.add(parent_id)
        self._ancestry_cache[some_revision_id] = ancestry
        return ancestry

    def get_file_revision(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        utf8_path = path.encode("utf-8")
        if utf8_path in self._manifest:
            # it's a file
            return self._get_file_last_modified_cl(utf8_path)
        elif self._has_directory(utf8_path):
            return self._get_dir_last_modified(utf8_path)
        else:
            raise errors.NoSuchId(self, file_id)

    def _get_dir_last_modified(self, path):
        revision = self._directories.get(path)
        if revision is not None:
            return revision
        utf8_path = path.encode("utf-8")
        revision = self._revision_id
        for p in self._manifest:
            if p.startswith(utf8_path+"/"):
                file_revision = self._get_file_introduced_revision(p)
                revision = self._pick_best_creator_revision(revision, file_revision)
        self._directories[utf8_path] = revision
        return revision

    def _get_file_last_modified_cl(self, path):
        """Find the mercurial changeset in which path was last changed."""
        hg_file_flags = self._manifest.flags(path)
        hg_file_revision = self._manifest.get(path)
        current_manifest = self._manifest
        # cls - changelogs
        parent_cls = set(self._repository._hgrepo.changelog.parents(self._hgid))
        good_id = self._hgid
        done_cls = set()
        # walk the graph, any node at a time to find the last change point.
        while parent_cls:
            current_cl = parent_cls.pop()
            # the nullid isn't useful.
            if current_cl == mercurial.node.nullid:
                continue
            if current_cl not in self._known_manifests:
                current_manifest_id = self._repository._hgrepo.changelog.read(current_cl)[0]
                self._known_manifests[current_cl] = self._repository._hgrepo.manifest.read(
                    current_manifest_id)
            current_manifest = self._known_manifests[current_cl]
            done_cls.add(current_cl)
            if (current_manifest.get(file, None) != hg_file_revision or
                current_manifest.flags(file) != hg_file_flags):
                continue
            # unchanged in parent, advance to the parent.
            good_id = current_cl
            for parent_cl in self._repository._hgrepo.changelog.parents(current_cl):
                if parent_cl not in done_cls:
                    parent_cls.add(parent_cl)
        return self._repository.lookup_foreign_revision_id(good_id, self._mapping)

    def _get_file_introduced_revision(self, path):
        parent_cl_ids = set([(None, self._hgid)])
        good_id = self._hgid
        done_cls = set()
        while parent_cl_ids:
            current_cl_id_child, current_cl_id = parent_cl_ids.pop()
            # the nullid isn't useful.
            if current_cl_id == mercurial.node.nullid:
                continue
            if current_cl_id not in self._known_manifests:
                current_manifest_id = self._repository._hgrepo.changelog.read(current_cl_id)[0]
                self._known_manifests[current_cl_id] = self._repository._hgrepo.manifest.read(
                    current_manifest_id)
            current_manifest = self._known_manifests[current_cl_id]
            done_cls.add(current_cl_id)
            if current_manifest.get(path, None) is None:
                # file is not in current manifest: its a tail, cut here.
                good_id = current_cl_id_child
                continue
            # walk to the parents
            if (mercurial.node.nullid, mercurial.node.nullid) == self._repository._hgrepo.changelog.parents(current_cl_id):
                # we have reached the root:
                good_id = current_cl_id
                continue
            for parent_cl in self._repository._hgrepo.changelog.parents(current_cl_id):
                if parent_cl not in done_cls:
                    parent_cl_ids.add((current_cl_id, parent_cl))
        return self._repository.lookup_foreign_revision_id(good_id, self._mapping)

    def has_id(self, file_id):
        path = self.id2path(file_id)
        return self.has_filename(path)

    def has_filename(self, path):
        if path.encode("utf-8") in self._manifest:
            return True
        # FIXME: What about directories?
        return False

    def find_related_paths_across_trees(self, paths, trees=[],
            require_versioned=True):
        if paths is None:
            return None
        if require_versioned:
            trees = [self] + (trees if trees is not None else [])
            unversioned = set()
            for p in paths:
                for t in trees:
                    if t.is_versioned(p):
                        break
                else:
                    unversioned.add(p)
            if unversioned:
                raise errors.PathsNotVersionedError(unversioned)
        return filter(self.is_versioned, paths)
