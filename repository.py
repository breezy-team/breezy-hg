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

from bzrlib import (
    errors,
    graph as _mod_graph,
    )
from bzrlib.foreign import (
    ForeignRepository,
    )
import bzrlib.repository
from bzrlib.revision import (
    NULL_REVISION,
    )

from bzrlib.plugins.hg.commit import (
    HgCommitBuilder,
    )
from bzrlib.plugins.hg.mapping import (
    as_bzr_parents,
    default_mapping,
    mapping_registry,
    )
from bzrlib.plugins.hg.tree import (
    HgRevisionTree,
    )


class MercurialSmartRemoteNotSupported(errors.BzrError):
    _fmt = "This operation is not supported by the Mercurial smart server protocol."


class HgRepositoryFormat(bzrlib.repository.RepositoryFormat):
    """Mercurial Repository Format.

    This is currently not aware of different repository formats,
    but simply relies on the installed copy of mercurial to
    support the repository format.
    """
    rich_root_data = True
    fast_deltas = True
    supports_leaving_lock = False
    supports_funky_characters = True
    supports_external_lookups = False
    supports_full_versioned_files = False
    supports_nesting_repositories = True
    supports_revision_signatures = False
    supports_versioned_directories = False
    revision_graph_can_have_wrong_parents = False

    @property
    def _matchingbzrdir(self):
        from bzrlib.plugins.hg.dir import HgControlDirFormat
        return HgControlDirFormat()

    def initialize(self, controldir, shared=False, _internal=False):
        from bzrlib.plugins.hg.dir import HgDir
        if shared:
            raise errors.IncompatibleFormat(self, controldir._format)
        if isinstance(controldir, HgDir):
            return controldir.open_repository()
        raise errors.UninitializableFormat(self)

    def is_supported(self):
        return True

    def get_format_description(self):
        """See RepositoryFormat.get_format_description()."""
        return "Mercurial Repository"

    def network_name(self):
        return "hg"

    def get_foreign_tests_repository_factory(self):
        from bzrlib.plugins.hg.tests.test_repository import (
            ForeignTestsRepositoryFactory,
            )
        return ForeignTestsRepositoryFactory()


class HgRepository(ForeignRepository):
    """An adapter to mercurial repositories for bzr."""

    chk_bytes = None
    _serializer = None

    def __init__(self, hgrepo, hgdir, lockfiles):
        ForeignRepository.__init__(self, HgRepositoryFormat(), hgdir, lockfiles)
        self._hgrepo = hgrepo
        self.base = hgdir.root_transport.base
        self._fallback_repositories = []

    def add_fallback_repository(self, basis_url):
        raise errors.UnstackableRepositoryFormat(self._format, self.base)

    def revision_graph_can_have_wrong_parents(self):
        return False

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

    def add_signature_text(self, revision_id, signature):
        """Store a signature text for a revision.

        :param revision_id: Revision id of the revision
        :param signature: Signature text.
        """
        raise errors.UnsupportedOperation(self.add_signature_text, self)

    def sign_revision(self, revision_id, gpg_strategy):
        raise errors.UnsupportedOperation(self.add_signature_text, self)


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
        assert type(revision_id) is str, "invalid revid: %r" % revision_id
        # TODO: Handle round-tripped revisions
        try:
            return mapping_registry.revision_id_bzr_to_foreign(revision_id)
        except errors.InvalidRevisionId:
            raise errors.NoSuchRevision(self, revision_id)

    def get_parent_map(self, revids):
        ret = {}
        for revid in revids:
            if revid == NULL_REVISION:
                ret[revid] = ()
                continue
            hgrevid, mapping = self.lookup_bzr_revision_id(revid)
            # FIXME: what about extra (roundtripped) parents?
            hgparents = self._hgrepo.changelog.parents(hgrevid)
            bzrparents = as_bzr_parents(hgparents, self.lookup_foreign_revision_id)
            if bzrparents == ():
                ret[revid] = (NULL_REVISION, )
            else:
                ret[revid] = bzrparents
        return ret

    def get_known_graph_ancestry(self, keys):
        """Get a KnownGraph instance with the ancestry of keys."""
        # most basic implementation is a loop around get_parent_map
        pending = set(keys)
        parent_map = {}
        while pending:
            this_parent_map = self.get_parent_map(pending)
            parent_map.update(this_parent_map)
            pending = set()
            map(pending.update, this_parent_map.itervalues())
            pending = pending.difference(parent_map)
        kg = _mod_graph.KnownGraph(parent_map)
        return kg

    def get_revision(self, revision_id):
        if not type(revision_id) is str:
            raise errors.InvalidRevisionId(revision_id, self)
        hgrevid, mapping = self.lookup_bzr_revision_id(revision_id)
        assert mapping is not None
        hgchange = self._hgrepo.changelog.read(hgrevid)
        hgparents = self._hgrepo.changelog.parents(hgrevid)
        parent_ids = as_bzr_parents(hgparents, self.lookup_foreign_revision_id)
        return mapping.import_revision(revision_id, parent_ids, hgrevid,
            hgchange[0], hgchange[1].decode("utf-8"), hgchange[2],
            hgchange[4].decode("utf-8"), hgchange[5])[0]

    def revision_trees(self, revids):
        for revid in revids:
            yield self.revision_tree(revid)

    def revision_tree(self, revision_id):
        hgid, mapping = self.lookup_bzr_revision_id(revision_id)
        log = self._hgrepo.changelog.read(hgid)
        manifest = self._hgrepo.manifest.read(log[0])
        return HgRevisionTree(self, revision_id, hgid, manifest, mapping)

    def has_foreign_revision(self, foreign_revid):
        return foreign_revid in self._hgrepo.changelog.nodemap

    def has_revisions(self, revids):
        ret = set()
        for revid in revids:
            if self.has_revision(revid):
                ret.add(revid)
        return ret

    def has_revision(self, revision_id):
        if revision_id == NULL_REVISION:
            return True
        try:
            return self.has_foreign_revision(self.lookup_bzr_revision_id(revision_id)[0])
        except errors.NoSuchRevision:
            return False

    def get_commit_builder(self, branch, parents, config, *args, **kwargs):
        self.start_write_group()
        return HgCommitBuilder(self, parents, config, *args, **kwargs)

    def all_revision_ids(self):
        return set([self.lookup_foreign_revision_id(
            self._hgrepo.changelog.node(hgid))
            for hgid in self._hgrepo.changelog])


class HgRemoteRepository(HgRepository):

    def get_parent_map(self, revids):
        raise MercurialSmartRemoteNotSupported()

    def get_revisions(self, revision_ids):
        raise MercurialSmartRemoteNotSupported()

    def get_revision(self, revision_id):
        raise MercurialSmartRemoteNotSupported()

    def has_revision(self, revision_id):
        raise MercurialSmartRemoteNotSupported()


from bzrlib.plugins.hg.fetch import (
    FromHgRepository,
    InterHgRepository,
    ToHgRepository,
    )
bzrlib.repository.InterRepository.register_optimiser(InterHgRepository)
bzrlib.repository.InterRepository.register_optimiser(FromHgRepository)
bzrlib.repository.InterRepository.register_optimiser(ToHgRepository)
