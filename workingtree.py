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

from bzrlib import osutils

from bzrlib.errors import (
    IncompatibleFormat,
    )
from bzrlib.inventory import (
    Inventory,
    )
import bzrlib.workingtree
from bzrlib.decorators import (
    needs_write_lock,
    )


from mercurial.hg import (
    update as hg_update,
    )


class HgWorkingTreeFormat(bzrlib.workingtree.WorkingTreeFormat):
    """Working Tree format for Mercurial Working Trees.

    This is currently not aware of different working tree formats,
    but simply relies on the installed copy of mercurial to
    support the working tree format.
    """

    @property
    def _matchingbzrdir(self):
        from bzrlib.plugins.hg.dir import HgControlDirFormat
        return HgControlDirFormat()

    def get_format_description(self):
        """See WorkingTreeFormat.get_format_description()."""
        return "Mercurial Working Tree"

    def initialize(self, to_bzrdir):
        from bzrlib.plugins.hg.dir import HgDir
        if not isinstance(to_bzrdir, HgDir):
            raise IncompatibleFormat(self, to_bzrdir._format)
        return to_bzrdir.create_workingtree()


class HgWorkingTree(bzrlib.workingtree.WorkingTree):
    """An adapter to mercurial repositories for bzr WorkingTree obejcts."""

    def __init__(self, hgrepo, hgbranch, hgdir, lockfiles):
        self._inventory = Inventory()
        self._hgrepo = hgrepo
        self.bzrdir = hgdir
        self.repository = hgdir.open_repository()
        self._control_files = lockfiles
        self._branch = hgbranch
        self._format = HgWorkingTreeFormat()
        self._transport = hgdir.get_workingtree_transport(None)
        self.basedir = hgdir.root_transport.local_abspath(".")
        self.views = self._make_views()

    def flush(self):
        pass

    @needs_write_lock
    def add(self, files, ids=None, kinds=None):
        # hg does not use ids, toss them out
        if isinstance(files, basestring):
            files = [files]
        if kinds is None:
            kinds = [osutils.file_kind(self.abspath(f)) for f in files]
        hg_files = []
        for file, kind in zip(files, kinds):
            if kind == "directory":
                continue
            hg_files.append(file.encode('utf-8'))

        # hg does not canonicalise paths : make them absolute
        self._hgrepo[None].add(hg_files)

    @needs_write_lock
    def commit(self, message=None, revprops=None, *args, **kwargs):
        # TODO: selected file lists -> match function
        if revprops is None:
            extra = {}
        else:
            extra = revprops
        hgid = self._hgrepo.commit(message, extra=extra, force=True)
        return self.bzrdir.open_repository().lookup_foreign_revision_id(hgid)

    def _reset_data(self):
        """Reset all cached data."""

    def update(self, change_reporter=None, possible_transports=None):
        hg_update(self._hgrepo, None)

    def unlock(self):
        """Overridden to avoid hashcache usage - hg manages that."""
        try:
            return self._control_files.unlock()
        finally:
            self.branch.unlock()
