# Copyright (C) 2005, 2006, 2011 Canonical Ltd
# Copyright (C) 2008-2011 Jelmer Vernooij <jelmer@samba.org>
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

"""Mercurial Branch support."""

import os

from bzrlib import (
    errors,
    revision as _mod_revision,
    )
from bzrlib.branch import (
    BranchCheckResult,
    BranchFormat,
    BranchPushResult,
    GenericInterBranch,
    InterBranch,
    PullResult,
    )
from bzrlib.config import GlobalConfig
from bzrlib.decorators import (
    needs_read_lock,
    needs_write_lock,
    )
from bzrlib.foreign import (
    ForeignBranch,
    )
from bzrlib.repository import (
    InterRepository,
    )
from bzrlib.tag import (
    BasicTags,
    DisabledTags,
    )

from bzrlib.plugins.hg.changegroup import (
    dchangegroup,
    )
from bzrlib.plugins.hg.repository import (
    MercurialSmartRemoteNotSupported,
    )

import mercurial.node

class NoPushSupport(errors.BzrError):
    _fmt = "Push is not yet supported for bzr-hg. Try dpush instead."


class HgTags(BasicTags):

    def __init__(self, branch, lookup_foreign_revision_id):
        self.branch = branch
        self.lookup_foreign_revision_id = lookup_foreign_revision_id

    def _get_hg_tags(self):
        raise NotImplementedError(self._get_hg_tags)

    def get_tag_dict(self):
        ret = {}
        hgtags = self._get_hg_tags()
        for name, value in hgtags.iteritems():
            ret[name.decode("utf-8")] = self.lookup_foreign_revision_id(value)
        return ret

    def set_tag(self, name, value):
        self.branch.repository._hgrepo.tag([name],
            self.branch.repository.lookup_bzr_revision_id(value)[0],
            "Create tag %s" % name,
            True,
            self.branch.get_config().username(), None)


class LocalHgTags(HgTags):

    def __init__(self, branch):
        super(LocalHgTags, self).__init__(branch,
            branch.repository.lookup_foreign_revision_id)

    def _get_hg_tags(self):
        return self.branch.repository._hgrepo.tags()


class FileHgTags(HgTags):
    """File hg tags."""

    def __init__(self, branch, revid, source_branch):
        super(FileHgTags, self).__init__(branch,
            getattr(branch.repository, "lookup_foreign_revision_id",
                branch.mapping.revision_id_foreign_to_bzr))
        self.source_branch = source_branch
        self.revid = revid

    def __repr__(self):
        return "<%s(%r, %r)>" % (self.__class__.__name__, self.branch, self.revid)

    def _get_hg_tags(self):
        revtree = self.source_branch.repository.revision_tree(self.revid)
        file_id = revtree.path2id(".hgtags")
        if file_id is None:
            return {}
        revtree.lock_read()
        try:
            f = revtree.get_file(file_id, ".hgtags")
            ret = {}
            for l in f.readlines():
                try:
                    (hgtag, name) = l.strip().split(" ", 1)
                except ValueError:
                    pass # Invalid value, just ignore?
                else:
                    ret[name] = hgtag
        finally:
            revtree.unlock()
        return ret


class HgBranchFormat(BranchFormat):
    """Mercurial Branch Format.

    This is currently not aware of different branch formats,
    but simply relies on the installed copy of mercurial to
    support the branch format.
    """

    @property
    def _matchingbzrdir(self):
        from bzrlib.plugins.hg.dir import HgControlDirFormat
        return HgControlDirFormat()

    def get_format_description(self):
        """See BranchFormat.get_format_description()."""
        return "Mercurial Branch"

    def network_name(self):
        return "hg"

    def get_foreign_tests_branch_factory(self):
        from bzrlib.plugins.hg.tests.test_branch import ForeignTestsBranchFactory
        return ForeignTestsBranchFactory()

    def initialize(self, a_bzrdir, name=None, repository=None):
        from bzrlib.plugins.hg.dir import HgDir
        if name is None:
            name = 'default'
        if not isinstance(a_bzrdir, HgDir):
            raise errors.IncompatibleFormat(self, a_bzrdir._format)
        bm = a_bzrdir._hgrepo.branchmap()
        if name in bm:
            raise errors.AlreadyBranchError(a_bzrdir.user_url)
        return a_bzrdir.open_branch(name=name)

    def make_tags(self, branch):
        """See bzrlib.branch.BranchFormat.make_tags()."""
        if (getattr(branch.repository._hgrepo, "tags", None) is not None and
            getattr(branch.repository, "lookup_foreign_revision_id", None) is not None):
            return LocalHgTags(branch)
        else:
            return DisabledTags(branch)



class HgBranchConfig(object):
    """Access Branch Configuration data for an HgBranch.

    This is not fully compatible with bzr yet - but it should be made so.
    """

    def __init__(self, branch):
        self._branch = branch
        self._ui = branch.repository._hgrepo.ui

    def username(self):
        username = self._ui.config("username", "default")
        if username is not None:
            return username
        return GlobalConfig().username()

    def post_commit(self):
        return None

    def get_nickname(self):
        # remove the trailing / and take the basename.
        return os.path.basename(self._branch.base[:-1])

    def get_parent(self):
        return self._ui.config("paths", "default")

    def set_parent(self, url):
        self._ui.setconfig("paths", "default", url)

    def has_explicit_nickname(self):
        return False

    def get_user_option(self, name):
        return self._ui.config(name, "bazaar")

    def get_user_option_as_bool(self, name):
        return False

    def set_user_option(self, name, value, warn_masked=False):
        self._ui.setconfig(name, "bazaar", value)

    def log_format(self):
        """What log format should be used"""
        return "long"


class HgReadLock(object):

    def __init__(self, unlock):
        self.unlock = unlock


class HgWriteLock(object):

    def __init__(self, unlock):
        self.branch_token = None
        self.unlock = unlock


class HgBranch(ForeignBranch):
    """An adapter to mercurial repositories for bzr Branch objects."""

    @property
    def control_url(self):
        return self.bzrdir.control_url

    @property
    def control_transport(self):
        return self.bzrdir.control_transport

    def __init__(self, hgrepo, name, hgdir, lockfiles):
        self.repository = hgdir.open_repository()
        self.base = hgdir.root_transport.base
        super(HgBranch, self).__init__(self.repository.get_mapping())
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles
        self.name = name

    def _check(self):
        # TODO: Call out to mercurial for consistency checking?
        return BranchCheckResult(self)

    def get_child_submit_format(self):
        """Return the preferred format of submissions to this branch."""
        ret = self.get_config().get_user_option("child_submit_format")
        if ret is not None:
            return ret
        return "hg"

    def get_parent(self):
        """Return the URL of the parent branch."""
        return self.get_config().get_parent()

    def get_physical_lock_status(self):
        return self.control_files.get_physical_lock_status()

    def get_push_location(self):
        """Return default push location of this branch."""
        # TODO: Obtain "repository default"
        return None

    def set_push_location(self, url):
        self.get_config().set_parent(url)

    def get_config(self):
        """See Branch.get_config().

        We return an HgBranchConfig, which is a stub class with little
        functionality.
        """
        return HgBranchConfig(self)

    def lock_write(self, token=None):
        if token is not None:
            raise errors.TokenLockingNotSupported(self)
        self.control_files.lock_write()
        return HgWriteLock(self.unlock)

    @needs_read_lock
    def revision_history(self):
        revs = list(self.repository.iter_reverse_revision_history(self.last_revision()))
        revs.reverse()
        return revs

    def lock_read(self):
        self.control_files.lock_read()
        return HgReadLock(self.unlock)

    def peek_lock_mode(self):
        return self.control_files.peek_lock_mode()

    def is_locked(self):
        return self.control_files.is_locked()

    def unlock(self):
        self.control_files.unlock()

    def get_stacked_on_url(self):
        raise errors.UnstackableBranchFormat(self._format, self.base)

    def _set_parent_location(self, parent_url):
        self.get_config().set_parent(parent_url)

    def _synchronize_history(self, destination, revision_id):
        source_revision_id = self.last_revision()
        if revision_id is None:
            revision_id = source_revision_id
        destination.generate_revision_history(revision_id)

    def _tip(self):
        try:
            return self._hgrepo.branchmap()[self.name][0]
        except KeyError:
            import mercurial.node
            return mercurial.node.nullid


class HgLocalBranch(HgBranch):

    def __init__(self, hgrepo, name, hgdir, lockfiles):
        self._format = HgBranchFormat()
        super(HgLocalBranch, self).__init__(hgrepo, name, hgdir, lockfiles)

    def supports_tags(self):
        return True

    def last_revision(self):
        tip = self._tip()
        return self.repository.lookup_foreign_revision_id(tip,
            mapping=self.mapping)

    def _read_last_revision_info(self):
        last_revid = self.last_revision()
        graph = self.repository.get_graph()
        revno = graph.find_distance_to_null(last_revid, [(_mod_revision.NULL_REVISION, 0)])
        return revno, last_revid

    def _write_last_revision_info(self, revno, revid):
        (hgid, mapping) = self.repository.lookup_bzr_revision_id(revid)
        self.repository._hgrepo.dirstate.setparents(hgid, mercurial.node.nullid)

    def set_last_revision_info(self, revno, revision_id):
        if not revision_id or not isinstance(revision_id, basestring):
            raise errors.InvalidRevisionId(revision_id=revision_id, branch=self)
        revision_id = _mod_revision.ensure_null(revision_id)
        old_revno, old_revid = self.last_revision_info()
        # TODO: Check append_revisions_only ?
        self._run_pre_change_branch_tip_hooks(revno, revision_id)
        self._write_last_revision_info(revno, revision_id)
        self._clear_cached_state()
        self._last_revision_info_cache = revno, revision_id
        self._run_post_change_branch_tip_hooks(old_revno, old_revid)


class HgRemoteBranch(HgBranch):

    def __init__(self, hgrepo, name, hgdir, lockfiles):
        self._format = HgBranchFormat()
        super(HgRemoteBranch, self).__init__(hgrepo, name, hgdir, lockfiles)

    def supports_tags(self):
        return True

    def _read_last_revision_info(self):
        raise MercurialSmartRemoteNotSupported()

    @needs_read_lock
    def last_revision(self):
        tip = self._tip()
        return self.mapping.revision_id_foreign_to_bzr(tip)


class InterHgBranch(GenericInterBranch):
    """InterBranch for two native Mercurial branches."""

    @staticmethod
    def _get_branch_formats_to_test():
        return [(HgBranchFormat(), HgBranchFormat())]

    @staticmethod
    def is_compatible(source, target):
        """See InterBranch.is_compatible()."""
        return (isinstance(source, HgBranch) and isinstance(target, HgBranch))

    def fetch(self, stop_revision=None, fetch_tags=False, limit=None):
        """See InterBranch.fetch."""
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        inter = InterRepository.get(self.source.repository,
                                    self.target.repository)
        # FIXME: handle fetch_tags ?
        inter.fetch(revision_id=stop_revision, limit=limit)

    def pull(self, overwrite=False, stop_revision=None,
             possible_transports=None, local=False):
        """See InterBranch.pull()."""
        result = PullResult()
        result.source_branch = self.source
        result.target_branch = self.target
        result.old_revno, result.old_revid = self.target.last_revision_info()
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        if type(stop_revision) != str:
            raise TypeError(stop_revision)
        self.fetch(stop_revision=stop_revision, fetch_tags=True)
        if overwrite:
            req_base = None
        else:
            req_base = self.target.last_revision()
        self.target.generate_revision_history(stop_revision,
            req_base, self.source)
        result.new_revno, result.new_revid = self.target.last_revision_info()
        return result

    def push(self, overwrite=False, stop_revision=None):
        """See InterBranch.push()."""
        result = BranchPushResult()
        result.source_branch = self.source
        result.target_branch = self.target
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        result.old_revno, result.old_revid = self.target.last_revision_info()
        self.fetch(stop_revision=stop_revision, fetch_tags=True)
        if overwrite:
            req_base = None
        else:
            req_base = self.target.last_revision()
        self.target.generate_revision_history(stop_revision,
            req_base, self.source)
        result.new_revno, result.new_revid = self.target.last_revision_info()
        return result


InterBranch.register_optimiser(InterHgBranch)


class InterFromHgBranch(GenericInterBranch):
    """InterBranch pulling from a Mercurial branch."""

    @staticmethod
    def _get_branch_formats_to_test():
        from bzrlib.branch import format_registry as branch_format_registry
        return [(HgBranchFormat(), branch_format_registry.get_default())]

    @staticmethod
    def is_compatible(source, target):
        """See InterBranch.is_compatible()."""
        return (isinstance(source, HgBranch) and
                not isinstance(target, HgBranch))

    def fetch(self, stop_revision=None, fetch_tags=True, limit=None):
        """See InterBranch.fetch."""
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        inter = InterRepository.get(self.source.repository,
                                    self.target.repository)
        inter.fetch(revision_id=stop_revision, limit=limit)
        # FIXME: Fetch tags (lp:309682) if fetch_tags is True

    def pull(self, overwrite=False, stop_revision=None,
             possible_transports=None, local=False):
        """See InterBranch.pull()."""
        result = PullResult()
        result.source_branch = self.source
        result.target_branch = self.target
        result.old_revno, result.old_revid = self.target.last_revision_info()
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        if type(stop_revision) != str:
            raise TypeError(stop_revision)
        self.fetch(stop_revision=stop_revision, fetch_tags=True)
        if overwrite:
            req_base = None
        else:
            req_base = self.target.last_revision()
        self.target.generate_revision_history(stop_revision,
            req_base, self.source)
        result.new_revno, result.new_revid = self.target.last_revision_info()
        tags = self._get_tags(result.new_revid)
        result.tag_conflicts = tags.merge_to(self.target.tags, overwrite)
        return result

    def push(self, overwrite=False, stop_revision=None):
        """See InterBranch.push()."""
        result = BranchPushResult()
        result.source_branch = self.source
        result.target_branch = self.target
        result.old_revid = self.target.last_revision()
        if stop_revision is not None:
            stop_revision = self.source.last_revision()
        self.fetch(stop_revision, fetch_tags=True)
        if overwrite:
            req_base = None
        else:
            req_base = self.target.last_revision()
        self.target.generate_revision_history(stop_revision, req_base,
            self.source)
        result.new_revid = self.target.last_revision()
        tags = self._get_tags(result.new_revid)
        result.tag_conflicts = tags.merge_to(self.target.tags, overwrite)
        return result

    @needs_write_lock
    def copy_content_into(self, revision_id=None):
        """Copy the content of source into target

        revision_id: if not None, the revision history in the new branch will
                     be truncated to end with revision_id.
        """
        self.source._synchronize_history(self.target, revision_id)
        tags = self._get_tags(revision_id)
        tags.merge_to(self.target.tags, overwrite=True)

    def _get_tags(self, revision_id):
        return FileHgTags(self.source, revision_id, self.target)


class HgBranchPushResult(BranchPushResult):

    def _lookup_revno(self, revid):
        assert isinstance(revid, str), "was %r" % revid
        # Try in source branch first, it'll be faster
        try:
            return self.source_branch.revision_id_to_revno(revid)
        except errors.NoSuchRevision:
            # FIXME: Check using graph.find_distance_to_null() ?
            return self.target_branch.revision_id_to_revno(revid)

    @property
    def old_revno(self):
        return self._lookup_revno(self.old_revid)

    @property
    def new_revno(self):
        return self._lookup_revno(self.new_revid)


class InterToHgBranch(InterBranch):
    """InterBranch implementation that pushes into Hg."""

    @staticmethod
    def _get_branch_formats_to_test():
        from bzrlib.branch import format_registry as branch_format_registry
        return [(branch_format_registry.get_default(), HgBranchFormat())]

    @classmethod
    def is_compatible(self, source, target):
        return (not isinstance(source, HgBranch) and
                isinstance(target, HgBranch))

    def _push_helper(self, stop_revision=None, overwrite=False,
            lossy=False):
        graph = self.source.repository.get_graph()
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        revs = graph.find_difference(self.target.last_revision(),
                                     stop_revision)[1]
        cg, revidmap = dchangegroup(self.source.repository,
                                    self.target.mapping, revs, lossy=lossy)
        heads = [revidmap[stop_revision]]
        remote = self.target.repository._hgrepo
        if remote.capable('unbundle'):
            remote.unbundle(cg, heads, None)
        else:
            remote.addchangegroup(cg, 'push', self.source.base)
            # TODO: Set heads
        if lossy:
            return dict((k, self.target.mapping.revision_id_foreign_to_bzr(v)) for (k, v) in revidmap.iteritems())

    @needs_read_lock
    def push(self, overwrite=True, stop_revision=None,
             lossy=False, _override_hook_source_branch=None):
        result = HgBranchPushResult()
        result.source_branch = self.source
        result.target_branch = self.target
        result.old_revid = self.target.last_revision()
        if stop_revision is None:
            stop_revision = self.source.last_revision()
        self._push_helper(stop_revision=stop_revision, overwrite=overwrite,
            lossy=False)
        # FIXME: Check for diverged branches
        if not lossy:
            result.new_revid = stop_revision
        else:
            if stop_revision != result.old_revid:
                revidmap = self._push_helper(stop_revision=stop_revision,
                    lossy=True)
                result.new_revid = revidmap.get(stop_revision, result.old_revid)
            else:
                result.new_revid = result.old_revid
            # FIXME: Check for diverged branches
            result.revidmap = revidmap
        return result

    @needs_read_lock
    def lossy_push(self, stop_revision=None):
        # For compatibility with bzr < 2.4
        return self.push(lossy=True, stop_revision=stop_revision)


InterBranch.register_optimiser(InterFromHgBranch)
InterBranch.register_optimiser(InterToHgBranch)
