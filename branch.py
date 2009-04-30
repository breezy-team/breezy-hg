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

import mercurial.node
import os

import bzrlib.branch
from bzrlib.decorators import (
    needs_read_lock,
    )
from bzrlib.foreign import (
    ForeignBranch,
    )

from bzrlib.plugins.hg.mapping import (
    mapping_registry,
    )
from bzrlib.plugins.hg.repository import (
    HgRepository,
    )


class MercurialBranchConfig(object):
    """Hg branch configuration."""

    def __init__(self, branch):
        # TODO: Read .hgrc
        self.branch = branch


class HgBranchFormat(bzrlib.branch.BranchFormat):
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

    def get_nickname(self):
        # remove the trailing / and take the basename.
        return os.path.basename(self._branch.base[:-1])

    def has_explicit_nickname(self):
        return True

    def log_format(self):
        """What log format should be used"""
        return "long"


class HgBranch(ForeignBranch):
    """An adapter to mercurial repositories for bzr Branch objects."""

    def __init__(self, hgrepo, hgdir, lockfiles):
        self._format = HgBranchFormat()
        self.repository = HgRepository(hgrepo, hgdir, lockfiles)
        ForeignBranch.__init__(self, self.repository.get_mapping())
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.control_files = lockfiles
        self.base = hgdir.root_transport.base

    def _check(self):
        # TODO: Call out to mercurial for consistency checking?
        return bzrlib.branch.BranchCheckResult(self)

    def get_parent(self):
        """Return the URL of the parent branch."""
        # TODO: Obtain "repository default" 
        return None

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
        tip, mapping = mapping_registry.revision_id_bzr_to_foreign(self.last_revision())
        revs = []
        next_rev = tip
        while next_rev != mercurial.node.nullid:
            revs.append(mapping.revision_id_foreign_to_bzr(next_rev))
            next_rev = self._hgrepo.changelog.parents(next_rev)[0]
        revs.reverse()
        return revs

    @needs_read_lock
    def last_revision(self):
        # perhaps should escape this ?
        return self.mapping.revision_id_foreign_to_bzr(self._hgrepo.changelog.tip())

    def lock_read(self):
        self.control_files.lock_read()

    def tree_config(self):
        return MercurialBranchConfig(self)
  
    def unlock(self):
        self.control_files.unlock()

    def copy_content_into(self, destination, revision_id=None):
        pass

    def clone(self, to_bzrdir, revision_id=None):
        # hg repositories can only clone into hg repos.
        assert isinstance(to_bzrdir, HgDir)
        # and have nothing to do as we follow the hg model.
        return to_bzrdir.open_branch()
