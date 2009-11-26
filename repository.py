# Copyright (C) 2005, 2006 Canonical Ltd
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

"""Mercurial Repository handling."""

import os

from bzrlib import (
    errors,
    ui,
    )
from bzrlib.foreign import (
    ForeignRepository,
    )
from bzrlib.inventory import (
    Inventory,
    )
from bzrlib.osutils import (
    sha_strings,
    )
import bzrlib.repository
from bzrlib.revision import (
    NULL_REVISION,
    )

from bzrlib.plugins.hg.mapping import (
    as_bzr_parents,
    default_mapping,
    mapping_registry,
    )
from bzrlib.plugins.hg.versionedfiles import (
    ChangelogVersionedFile,
    ManifestVersionedFile,
    RevlogVersionedFiles,
    )

import mercurial.node

class MercurialSmartRemoteNotSupported(errors.BzrError):
    _fmt = "This operation is not supported by the Mercurial smart server protocol."


class HgRepositoryFormat(bzrlib.repository.RepositoryFormat):
    """Mercurial Repository Format.

    This is currently not aware of different repository formats,
    but simply relies on the installed copy of mercurial to
    support the repository format.
    """
    rich_root_data = True

    def is_supported(self):
        return True

    def get_format_description(self):
        """See RepositoryFormat.get_format_description()."""
        return "Mercurial Repository"

    def network_name(self):
        return "hg"

    def get_foreign_tests_repository_factory(self):
        from bzrlib.plugins.hg.tests.test_repository import ForeignTestsRepositoryFactory
        return ForeignTestsRepositoryFactory()


def manifest_to_inventory(hgrepo, hgid, log, manifest, all_relevant_revisions,
                          mapping, lookup_foreign_revision_id, pb):
    """Convert a Mercurial manifest to a Bazaar inventory.

    :param hgrepo: A local Mercurial repository
    :param hgid: HG ID of the relevant revision
    :param log: Log object of the relevant revision
    :param manifest: The manifest
    :param all_relevant_revisions: Graph will relevant revisions
    :param mapping: Mapping to use
    :param pb: Progress bar
    :return: Inventory object

    how this works:
    we grab the manifest for revision_id
    we create an Inventory
    for each file in the manifest we:
        * if the dirname of the file is not in the inventory, we add it
          recursively, with an id of the path with / replaced by :, and a
          prefix of 'hg:'. The directory gets a last-modified value of the
          topologically oldest file.revision value under it in the
          inventory. In the event of multiple revisions with no topological
          winner - that is where there is more than one root, alpha-sorting
          is used as a tie-break.
        * use the files revlog to get the 'linkrev' of the file which
          takes us to the revision id that introduced that revision. That
          revision becomes the revision_id in the inventory
        * check for executable status in the manifest flags
        * add an entry for the file, of type file, executable if needed,
          and an id of 'hg:path' with / replaced by :.
    """
    ancestry_cache = {}
    result = Inventory()
    # each directory is a key - i.e. 'foo'
    # the value is the current chosen revision value for it.
    # we walk up the hierarchy - when a dir changes .revision, its parent
    # must also change if the new value is older than the parents one.
    directories = {}
    def get_ancestry(some_revision_id):
        try:
            return ancestry_cache[some_revision_id]
        except KeyError:
            pass
        ancestry = set()
        # add what can be reached from some_revision_id
        # TODO: must factor this trivial iteration in bzrlib.graph cleanly.
        pending = set([some_revision_id])
        while len(pending) > 0:
            node = pending.pop()
            ancestry.add(node)
            for parent_id in all_relevant_revisions[node]:
                if parent_id not in ancestry:
                    pending.add(parent_id)
        ancestry_cache[some_revision_id] = ancestry
        return ancestry
    def pick_best_creator_revision(revision_a, revision_b):
        """Picks the best creator revision from a and b.

        If a is an ancestor of b, a wins, and vice verca.
        If neither is an ancestor of the other, the lowest value wins.
        """
        # TODO make this much faster - use a local cache of the ancestry
        # sets.
        if revision_a in get_ancestry(revision_b):
            return revision_a
        elif revision_b in get_ancestry(revision_a):
            return revision_b
        elif revision_a < revision_b:
            return revision_a
        else:
            return revision_b
    def add_dir_for(file, file_revision_id):
        """ensure that file can be added by adding its parents.

        this is horribly inefficient at the moment, proof of concept.

        This is called for every path under each dir, and will update the
        .revision for it older each time as the file age is determined.
        """
        path = os.path.dirname(file)
        if path == '':
            # special case the root node.
            return
        if result.has_filename(path):
            # check for a new revision
            current_best = directories[path]
            new_best = pick_best_creator_revision(current_best, file_revision_id)
            if new_best != current_best:
                # new revision found, push this up
                # XXX could hand in our result as a hint?
                add_dir_for(path, file_revision_id)
                # and update our chosen one
                directories[path] = file_revision_id
            return
        # the dir is not present. Add its parent too:
        add_dir_for(path, file_revision_id)
        # add the dir to the directory summary for creation detection
        directories[path] = file_revision_id
        # and put it in the inventory. The revision value is assigned later.
        entry = result.add_path(path, 'directory',
            file_id=mapping.generate_file_id(path))
    # this can and should be tuned, but for now its just fine - its a
    # proof of concept. add_path is part of the things to tune, as is
    # the dirname() calls.
    known_manifests = {}
    """manifests addressed by changelog."""
    for i, (file, file_revision) in enumerate(manifest.items()):
        pb.update("converting manifest", i, len(manifest))
        revlog = hgrepo.file(file)
        file_flags = manifest.flags(file)

        # find when the file was modified.
        # start with the manifest nodeid
        current_log = log
        # we should find all the tails, and then when there are > 2 heads
        # record a new revision id at the join. We can detect this by
        # walking out from each head and assigning ids to them, when two
        # parents have the same manifest assign a new id.
        # TODO currently we just pick *a* tail.
        file_tails = []
        current_manifest = manifest
        # cls - changelogs
        parent_cls = set(hgrepo.changelog.parents(hgid))
        good_id = hgid
        done_cls = set()
        # walk the graph, any node at a time to find the last change point.
        while parent_cls:
            current_cl = parent_cls.pop()
            # the nullid isn't useful.
            if current_cl == mercurial.node.nullid:
                continue
            if current_cl not in known_manifests:
                current_manifest_id = hgrepo.changelog.read(current_cl)[0]
                known_manifests[current_cl] = hgrepo.manifest.read(
                    current_manifest_id)
            current_manifest = known_manifests[current_cl]
            done_cls.add(current_cl)
            if current_manifest.get(file, None) != file_revision or current_manifest.flags(file) != file_flags:
                continue
            # unchanged in parent, advance to the parent.
            good_id = current_cl
            for parent_cl in hgrepo.changelog.parents(current_cl):
                if parent_cl not in done_cls:
                    parent_cls.add(parent_cl)
        modified_revision = lookup_foreign_revision_id(good_id, mapping)
        # dont use the following, it doesn't give the right results consistently.
        # modified_revision = bzrrevid_from_hg(
        #     self._hgrepo.changelog.index[changelog_index][7])
        # now walk to find the introducing revision.
        parent_cl_ids = set([(None, hgid)])
        good_id = hgid
        done_cls = set()
        while parent_cl_ids:
            current_cl_id_child, current_cl_id = parent_cl_ids.pop()
            # the nullid isn't useful.
            if current_cl_id == mercurial.node.nullid:
                continue
            if current_cl_id not in known_manifests:
                current_manifest_id = hgrepo.changelog.read(current_cl_id)[0]
                known_manifests[current_cl_id] = hgrepo.manifest.read(
                    current_manifest_id)
            current_manifest = known_manifests[current_cl_id]
            done_cls.add(current_cl_id)
            if current_manifest.get(file, None) is None:
                # file is not in current manifest: its a tail, cut here.
                good_id = current_cl_id_child
                continue
            # walk to the parents
            if (mercurial.node.nullid, mercurial.node.nullid) == hgrepo.changelog.parents(current_cl_id):
                # we have reached the root:
                good_id = current_cl_id
                continue
            for parent_cl in hgrepo.changelog.parents(current_cl_id):
                if parent_cl not in done_cls:
                    parent_cl_ids.add((current_cl_id, parent_cl))
        introduced_at_path_revision = lookup_foreign_revision_id(good_id, mapping)
        add_dir_for(file, introduced_at_path_revision)
        if 'l' in file_flags:
            kind = 'symlink'
        else:
            kind = 'file'
        entry = result.add_path(file, kind, file_id=mapping.generate_file_id(file))
        # its a shame we need to pull the text out. is there a better way?
        # TODO: perhaps we should use readmeta here to figure out renames ?
        text = revlog.read(file_revision)
        if "l" in file_flags:
            entry.symlink_target = text
        else:
            entry.text_size = len(text)
            entry.text_sha1 = sha_strings(text)
        if "x" in file_flags:
            entry.executable = True
        entry.revision = modified_revision
    for dir, dir_revision_id in directories.items():
        dirid = mapping.generate_file_id(dir)
        result[dirid].revision = dir_revision_id
    return result


class HgRepository(ForeignRepository):
    """An adapter to mercurial repositories for bzr."""

    _serializer = None

    def __init__(self, hgrepo, hgdir, lockfiles):
        ForeignRepository.__init__(self, HgRepositoryFormat(), hgdir, lockfiles)
        self._hgrepo = hgrepo
        self.base = hgdir.root_transport.base
        self._fallback_repositories = []
        self.signatures = None
        if self._hgrepo.local():
            self.revisions = ChangelogVersionedFile(self._hgrepo.changelog, self)
            self.inventories = ManifestVersionedFile(self, self._hgrepo.manifest)
            self.texts = RevlogVersionedFiles(self, self._hgrepo.file,
                                              self.get_mapping())
        else:
            self.revisions = None
            self.inventories = None
            self.texts = None

    def _warn_if_deprecated(self):
        # This class isn't deprecated
        pass

    def _check(self, revision_ids, callback_refs, check_repo):
        # TODO: Call out to mercurial for consistency checking?
        return bzrlib.branch.BranchCheckResult(self)

    def make_working_trees(self):
        return True # Do bare repositories exist at all in Mercurial?

    def has_signature_for_revision_id(self, revision):
        return False

    def get_mapping(self):
        return default_mapping # for now

    def is_shared(self):
        """Whether this repository is being shared between multiple branches.

        Always False for Mercurial for now.
        """
        return False


class HgLocalRepository(HgRepository):
    """Local Mercurial repository."""

    def get_revisions(self, revids):
        return [self.get_revision(r) for r in revids]

    def lookup_foreign_revision_id(self, hgid, mapping=None):
        if mapping is None:
            mapping = self.get_mapping()
        return mapping.revision_id_foreign_to_bzr(hgid)

    def lookup_bzr_revision_id(self, revision_id):
        """See ForeignRepository.lookup_bzr_revision_id()."""
        # TODO: Handle round-tripped revisions
        try:
            return mapping_registry.revision_id_bzr_to_foreign(revision_id)
        except errors.InvalidRevisionId:
            raise errors.NoSuchRevision(self, revision_id)

    def get_revision(self, revision_id):
        hgrevid, mapping = self.lookup_bzr_revision_id(revision_id)
        hgchange = self._hgrepo.changelog.read(hgrevid)
        hgparents = self._hgrepo.changelog.parents(hgrevid)
        parent_ids = as_bzr_parents(hgparents, self.lookup_foreign_revision_id)
        return mapping.import_revision(revision_id, parent_ids, hgrevid, hgchange[0], hgchange[1], hgchange[2], hgchange[4], hgchange[5])[0]

    def iter_inventories(self, revision_ids, ordering=None):
        for revid in revision_ids:
            yield self.get_inventory(revid)

    def get_inventory(self, revision_id):
        hgid, mapping = self.lookup_bzr_revision_id(revision_id)
        log = self._hgrepo.changelog.read(hgid)
        manifest = self._hgrepo.manifest.read(log[0])
        all_relevant_revisions = self.get_ancestry(revision_id)[1:] + [NULL_REVISION]
        pb = ui.ui_factory.nested_progress_bar()
        try:
            inv = manifest_to_inventory(self._hgrepo, hgid, log, manifest,
                self.get_parent_map(all_relevant_revisions), mapping,
                self.lookup_foreign_revision_id, pb)
            inv.revision_id = revision_id
            return inv
        finally:
            pb.finished()

    def has_foreign_revision(self, foreign_revid):
        return foreign_revid in self._hgrepo.changelog.nodemap

    def has_revision(self, revision_id):
        try:
            return self.has_foreign_revision(self.lookup_bzr_revision_id(revision_id)[0])
        except errors.NoSuchRevision:
            return False


class HgRemoteRepository(HgRepository):

    def get_parent_map(self, revids):
        raise MercurialSmartRemoteNotSupported()

    def get_revisions(self, revision_ids):
        raise MercurialSmartRemoteNotSupported()

    def iter_inventories(self, revision_ids, ordering=None):
        raise MercurialSmartRemoteNotSupported()

    def get_inventory(self, revision_id):
        raise MercurialSmartRemoteNotSupported()

    def get_revision(self, revision_id):
        raise MercurialSmartRemoteNotSupported()

    def has_revision(self, revision_id):
        raise MercurialSmartRemoteNotSupported()


from bzrlib.plugins.hg.fetch import (
    FromHgRepository,
    InterHgRepository,
    )
bzrlib.repository.InterRepository.register_optimiser(InterHgRepository)
bzrlib.repository.InterRepository.register_optimiser(FromHgRepository)
