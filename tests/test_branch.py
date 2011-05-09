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

"""Tests for Mercurial branches."""

from bzrlib.tests import (
    TestCase,
    TestCaseWithTransport,
    )

from bzrlib.plugins.hg.branch import (
    HgBranchFormat,
    FileHgTags,
    )
from bzrlib.plugins.hg.dir import (
    HgControlDirFormat,
    )

class BranchFormatTests(TestCase):

    def test_description(self):
        self.assertEquals("Mercurial Branch",
            HgBranchFormat().get_format_description())


class ForeignTestsBranchFactory(object):

    def make_empty_branch(self, transport):
        return HgControlDirFormat().initialize_on_transport(transport).open_branch()

    make_branch = make_empty_branch


class TestFileHgTags(TestCaseWithTransport):

    def test_no_tags_file(self):
        branch = self.make_branch(".", format=HgControlDirFormat())
        tags = FileHgTags(branch, branch.last_revision(), branch)
        self.assertEquals({}, tags.get_tag_dict())

    def test_tags(self):
        tree = self.make_branch_and_tree(".", format=HgControlDirFormat())
        self.build_tree_contents([
            (".hgtags", "4ad63131870d4fbf2a88d7403705310b2d0b9b76 v0.1\n")])
        tree.add([".hgtags"])
        tree.commit("add tags")
        tags = FileHgTags(tree.branch, tree.branch.last_revision(), tree.branch)
        self.assertEquals({"v0.1": "hg-v1:4ad63131870d4fbf2a88d7403705310b2d0b9b76"},
            tags.get_tag_dict())

    def test_space_in_tags(self):
        tree = self.make_branch_and_tree(".", format=HgControlDirFormat())
        self.build_tree_contents([
            (".hgtags", "4ad63131870d4fbf2a88d7403705310b2d0b9b76 tag with a space\n")])
        tree.add([".hgtags"])
        tree.commit("add tags")
        tags = FileHgTags(tree.branch, tree.branch.last_revision(), tree.branch)
        self.assertEquals({"tag with a space": "hg-v1:4ad63131870d4fbf2a88d7403705310b2d0b9b76"},
            tags.get_tag_dict())
