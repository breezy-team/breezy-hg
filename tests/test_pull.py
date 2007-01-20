# Copyright (C) 2005, 2006 Canonical Ltd
# Copyright (C) 2006 Jelmer Vernooij <jelmer@samba.org>

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

from bzrlib.inventory import Inventory
from bzrlib.plugins.hg import HgBzrDirFormat
from bzrlib.tests import TestCaseWithTransport

import copy
import os
import stat

class TestPulling(TestCaseWithTransport):
    """Tests for pulling from hg to bzr."""

    def setUp(self):
        super(TestPulling, self).setUp()
        self.build_tree(['hg/', 'hg/a', 'hg/b', 'hg/dir/', 'hg/dir/c'])
        hgdir = HgBzrDirFormat().initialize('hg')
        self.tree = hgdir.open_workingtree()
        mode = os.lstat('hg/b').st_mode
        os.chmod('hg/b', mode | stat.S_IEXEC)
        # do not add 'dir' to ensure that we pickup dir/c anyway : if hg
        # changes it behaviour, we want this test to start failing.
        self.tree.add(['a', 'b', 'dir/c'])
        self.tree.commit('foo')
        revone_inventory = Inventory()
        tip = self.tree.last_revision()
        entry = revone_inventory.add_path('a', kind='file', file_id='hg:a')
        entry.revision = tip
        entry.text_size = len('contents of hg/a\n')
        entry.text_sha1 = "72bcea9d6cba6ee7d3241bfa0c5e54506ad81a94"
        entry = revone_inventory.add_path('b', kind='file', file_id='hg:b')
        entry.executable = True
        entry.revision = tip
        entry.text_size = len('contents of hg/b\n')
        entry.text_sha1 = "b4d0c22d126cd0afeeeffa62961fb47c0932835a"
        entry = revone_inventory.add_path('dir', kind='directory',
            file_id='hg:dir')
        entry.revision = tip
        entry = revone_inventory.add_path('dir/c', kind='file',
            file_id='hg:dir:c')
        entry.revision = tip
        entry.text_size = len('contents of hg/dir/c\n')
        entry.text_sha1 = "958be752affac0fee70471331b96fb3fc1809425"
        self.revone_inventory = revone_inventory
        self.revidone = tip
        #====== end revision one
        self.build_tree(['hg/dir/d'])
        self.tree.add(['dir/d'])
        self.tree.commit('bar')
        self.revtwo_inventory = copy.deepcopy(revone_inventory)
        tip = self.tree.last_revision()
        entry = self.revtwo_inventory.add_path('dir/d', kind='file',
            file_id='hg:dir:d')
        entry.revision = tip
        entry.text_size = len('contents of hg/dir/d\n')
        entry.text_sha1 = "f48fc342f707bfb4711790e1813c0df4d44e1a23"
        self.revidtwo = tip
        #====== end revision two
        # in revision three, we reset the exec flag on 'b'
        os.chmod('hg/b', mode)
        self.tree.commit('reset mode on b')
        self.revthree_inventory = copy.deepcopy(self.revtwo_inventory)
        tip = self.tree.last_revision()
        # should be a new file revision with exec reset
        entry = self.revthree_inventory['hg:b']
        entry.revision = tip
        entry.executable = False
        self.revidthree = tip

    def test_inventory_from_manifest(self):
        repo = self.tree.branch.repository
        left = self.revone_inventory
        right = repo.get_inventory(self.revidone)
        self.assertEqual(left._byid, right._byid)
        left = self.revtwo_inventory
        right = repo.get_inventory(self.revidtwo)
        self.assertEqual(left._byid, right._byid)
        left = self.revthree_inventory
        right = repo.get_inventory(self.revidthree)
        self.assertEqual(left._byid, right._byid)

    def test_initial_revision_from_changelog(self):
        converted_rev = self.tree.branch.repository.get_revision(self.revidone)
        self.assertEqual([], converted_rev.parent_ids)
        self.assertEqual({}, converted_rev.properties)
        self.assertEqual('foo', converted_rev.message)
        self.assertEqual(self.revidone, converted_rev.revision_id)
        # we dont have a serialised inventory to convert, and the inv sha1 is
        # of reducing meaning now.
        self.assertEqual("", converted_rev.inventory_sha1)
        self.assertNotEqual(None, converted_rev.timestamp)
        self.assertNotEqual(None, converted_rev.timezone)
        self.assertNotEqual(None, converted_rev.committer)

    def test_non_initial_revision_from_changelog(self):
        converted_rev = self.tree.branch.repository.get_revision(self.revidtwo)
        self.assertEqual([self.revidone], converted_rev.parent_ids)
        self.assertEqual({}, converted_rev.properties)
        self.assertEqual('bar', converted_rev.message)
        self.assertEqual(self.revidtwo, converted_rev.revision_id)
        # we dont have a serialised inventory to convert, and the inv sha1 is
        # of reducing meaning now.
        self.assertEqual("", converted_rev.inventory_sha1)
        self.assertNotEqual(None, converted_rev.timestamp)
        self.assertNotEqual(None, converted_rev.timezone)
        self.assertNotEqual(None, converted_rev.committer)

    def test_get_config_nickname(self):
        # the branch nickname should be hg because the test dir is called hg.
        self.assertEqual("hg", self.tree.branch.get_config().get_nickname())

    def test_has_revision(self):
        self.assertTrue(self.tree.branch.repository.has_revision(self.revidone))
        self.assertTrue(self.tree.branch.repository.has_revision(self.revidtwo))
        self.assertFalse(self.tree.branch.repository.has_revision('foo'))

    def test_pull_into_bzr(self):
        bzrtree = self.make_branch_and_tree('bzr')
        bzrtree.pull(self.tree.branch)
        self.assertFileEqual('contents of hg/a\n', 'bzr/a')
        self.assertFileEqual('contents of hg/b\n', 'bzr/b')
        self.assertFileEqual('contents of hg/dir/c\n', 'bzr/dir/c')
 
