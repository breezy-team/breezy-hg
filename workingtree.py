# Copyright (C) 2005, 2006 Canonical Ltd
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

"""Mercurial working tree support."""

from bzrlib.inventory import (
    Inventory,
    )
import bzrlib.workingtree
from bzrlib.decorators import (
    needs_write_lock,
    )


class HgWorkingTreeFormat(bzrlib.workingtree.WorkingTreeFormat):
    """Working Tree format for Mercurial Working Trees.

    This is currently not aware of different working tree formats,
    but simply relies on the installed copy of mercurial to 
    support the working tree format.
    """

    def get_format_description(self):
        """See WorkingTreeFormat.get_format_description()."""
        return "Mercurial Working Tree"


class HgWorkingTree(bzrlib.workingtree.WorkingTree):
    """An adapter to mercurial repositories for bzr WorkingTree obejcts."""

    def __init__(self, hgrepo, hgbranch, hgdir, lockfiles):
        self._inventory = Inventory()
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self._control_files = lockfiles
        self._branch = hgbranch
        self._format = HgWorkingTreeFormat()
        self._transport = hgdir.get_workingtree_transport(None)
        self.basedir = hgdir.root_transport.local_abspath(".")
        self.views = self._make_views()

    @needs_write_lock
    def add(self, files, ids=None):
        # hg does not use ids, toss them away.
        if isinstance(files, basestring):
            files = [files]
        # hg does not canonicalise paths : make them absolute
        paths = [(file).encode('utf8') for file in files]
        self._hgrepo.add(paths)

    @needs_write_lock
    def commit(self, message, *args, **kwargs):
        # TODO: selected file lists etc.
        self._hgrepo.commit(message)

    def _reset_data(self):
        """Reset all cached data."""

    def unlock(self):
        """Overridden to avoid hashcache usage - hg manages that."""
        try:
            return self._control_files.unlock()
        finally:
            self.branch.unlock()
