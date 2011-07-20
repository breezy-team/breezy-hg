# Copyright (C) 2011 Canonical Ltd.

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

"""Tests for revision specifier."""

import binascii

from bzrlib.errors import (
    InvalidRevisionSpec,
    )

from bzrlib.revisionspec import (
    RevisionSpec,
    )

from bzrlib.tests import (
    TestCase,
    TestCaseWithTransport,
    )

from bzrlib.plugins.hg.revspec import (
    valid_hg_csid,
    )

class ValidHgCsidTests(TestCase):

    def test_valid(self):
        self.assertTrue(valid_hg_csid("abcdef"))
        self.assertFalse(valid_hg_csid("abcdez"))


class RevisionSpecTests(TestCaseWithTransport):

    def test_valid(self):
        tree = self.make_branch_and_tree(".", format="hg")
        revid = tree.commit("acommit")
        text = binascii.hexlify(tree.branch._tip())
        revspec = RevisionSpec.from_string("hg:%s" % text)
        self.assertEquals(revid, revspec.as_revision_id(tree.branch))

    def test_search(self):
        tree = self.make_branch_and_tree(".", format="hg")
        revid = tree.commit("acommit")
        text = binascii.hexlify(tree.branch._tip())[:20]
        revspec = RevisionSpec.from_string("hg:%s" % text)
        self.assertEquals(revid, revspec.as_revision_id(tree.branch))

    def test_bzr_branch(self):
        tree = self.make_branch_and_tree(".")
        revid = tree.commit("acommit")
        text = "ab" * 20
        revspec = RevisionSpec.from_string("hg:%s" % text)
        self.assertRaises(InvalidRevisionSpec, revspec.as_revision_id,
            tree.branch)
