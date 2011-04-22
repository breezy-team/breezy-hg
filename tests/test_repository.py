# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""Tests for HgRepository."""

from bzrlib.plugins.hg.dir import (
    HgControlDirFormat,
    )
from bzrlib.tests import (
    TestCaseWithTransport,
    )


class ForeignTestsRepositoryFactory(object):

    def make_repository(self, transport):
        controldir = HgControlDirFormat().initialize_on_transport(transport)
        return controldir.open_repository()


class HgRepositoryTests(TestCaseWithTransport):

    def test_all_revision_ids_empty(self):
        tree = self.make_branch_and_tree(".", format="hg")
        self.assertEquals(set(),
            tree.branch.repository.all_revision_ids())

    def test_all_revision_ids(self):
        tree = self.make_branch_and_tree(".", format="hg")
        revid = tree.commit("foo")
        self.assertEquals(set([revid]),
            tree.branch.repository.all_revision_ids())
