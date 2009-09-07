# Copyright (C) 2005, 2006 Canonical Ltd
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
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

import mercurial.node
import os

from bzrlib import (
    errors,
    )
from bzrlib.branch import (
    BranchCheckResult,
    BranchFormat,
    BranchPushResult,
    InterBranch,
    PullResult,
    )
from bzrlib.decorators import (
    needs_read_lock,
    )
from bzrlib.foreign import (
    ForeignBranch,
    )
from bzrlib.repository import (
    InterRepository,
    )

from bzrlib.plugins.hg.mapping import (
    mapping_registry,
    )
from bzrlib.plugins.hg.repository import (
    HgRepository,
    )


class HgBranchFormat(BranchFormat):
    """Mercurial Branch Format.

    This is currently not aware of different branch formats,
    but simply relies on the installed copy of mercurial to 
    support the branch format.
    """

    def get_format_description(self):
        """See BranchFormat.get_format_description()."""
        return "Mercurial Branch"


class HgBranchConfig(object):
    """Access Branch Configuration data for an HgBranch.

    This is not fully compatible with bzr yet - but it should be made so.
    """

    def __init__(self, branch):
        self._branch = branch
        self._ui = branch.repository._hgrepo.ui

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
        return None

    def log_format(self):
        """What log format should be used"""
        return "long"


class HgBranch(ForeignBranch):
    """An adapter to mercurial repositories for bzr Branch objects."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._format = HgBranchFormat()
        self.repository = hgdir.open_repository()
        ForeignBranch.__init__(self, self.repository.get_mapping())
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles
        self.base = hgdir.root_transport.base

    def _check(self):
        # TODO: Call out to mercurial for consistency checking?
        return BranchCheckResult(self)

    def get_parent(self):
        """Return the URL of the parent branch."""
        # TODO: Obtain "repository default" 
        return self.get_config().get_parent()

    def get_physical_lock_status(self):
        return self.control_files.get_physical_lock_status()

    def get_push_location(self):
        """Return default push location of this branch."""
        # TODO: Obtain "repository default"
        return None

    def get_config(self):
        """See Branch.get_config().

        We return an HgBranchConfig, which is a stub class with little
        functionality.
        """
        return HgBranchConfig(self)

    def lock_write(self):
        self.control_files.lock_write()
    
    @needs_read_lock
    def revision_history(self):
        revs = list(self.repository.iter_reverse_revision_history(self.last_revision()))
        revs.reverse()
        return revs

    @needs_read_lock
    def last_revision(self):
        # FIXME: Check for "lookup" capability?
        tip = self._hgrepo.lookup("tip")
        return self.mapping.revision_id_foreign_to_bzr(tip)

    def lock_read(self):
        self.control_files.lock_read()

    def is_locked(self):
        return self.control_files.is_locked()

    def unlock(self):
        self.control_files.unlock()

    def clone(self, to_bzrdir, revision_id=None):
        # hg repositories can only clone into hg repos.
        # and have nothing to do as we follow the hg model.
        return to_bzrdir.open_branch()

    def get_stacked_on_url(self):
        raise errors.UnstackableBranchFormat(self._format, self.base)

    def _set_parent_location(self, parent_url):
        self.get_config().set_parent(parent_url)


class InterHgBranch(InterBranch):
    """InterBranch for two native Mercurial branches."""

    @staticmethod
    def is_compatible(source, target):
        return (isinstance(source, HgBranch) and isinstance(target, HgBranch))

    def pull(self, overwrite=False, stop_revision=None, 
             possible_transports=None, local=False):
        result = PullResult()
        result.source_branch = self.source
        result.target_branch = self.target
        result.old_revid = self.target.last_revision()
        # TODO: Just use Mercurial API here directly rather than going via 
        # InterRepository ?
        inter = InterRepository.get(self.source.repository, 
                                    self.target.repository)
        inter.fetch(revision_id=stop_revision)
        result.new_revid = self.target.last_revision()
        return result

    def push(self, overwrite=False, stop_revision=None):
        result = BranchPushResult()
        result.source_branch = self.source
        result.target_branch = self.target
        result.old_revid = self.target.last_revision()
        # TODO: Just use Mercurial API here directly rather than going via 
        # InterRepository ?
        inter = InterRepository.get(self.source.repository, 
                                    self.target.repository)
        inter.fetch(revision_id=stop_revision)
        result.new_revid = self.target.last_revision()
        return result


InterBranch.register_optimiser(InterHgBranch)
