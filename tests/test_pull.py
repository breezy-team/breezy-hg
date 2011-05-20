# Copyright (C) 2005, 2006 Canonical Ltd

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
from bzrlib.plugins.hg.dir import HgControlDirFormat
from bzrlib.tests import TestCaseWithTransport

import base64
import copy
import mercurial.node
import os
import stat


class TestPulling(TestCaseWithTransport):
    """Tests for pulling from hg to bzr."""

    def setUp(self):
        super(TestPulling, self).setUp()
        self.build_tree(['hg/', 'hg/a', 'hg/b', 'hg/dir/', 'hg/dir/c'])
        hgdir = HgControlDirFormat().initialize('hg')
        self.tree = hgdir.open_workingtree()
        mode = os.lstat('hg/b').st_mode
        os.chmod('hg/b', mode | stat.S_IEXEC)
        # do not add 'dir' to ensure that we pickup dir/c anyway : if hg
        # changes it behaviour, we want this test to start failing.
        self.tree.add(['a', 'b', 'dir/c'])
        self.tree.commit('foo')
        revone_inventory = Inventory()
        tip = self.tree.last_revision()
        revone_inventory.root.revision = tip
        entry = revone_inventory.add_path(u'a', kind='file', file_id='hg:a')
        entry.revision = tip
        entry.text_size = len('contents of hg/a\n')
        entry.text_sha1 = "72bcea9d6cba6ee7d3241bfa0c5e54506ad81a94"
        entry = revone_inventory.add_path(u'b', kind='file', file_id='hg:b')
        entry.executable = True
        entry.revision = tip
        entry.text_size = len('contents of hg/b\n')
        entry.text_sha1 = "b4d0c22d126cd0afeeeffa62961fb47c0932835a"
        entry = revone_inventory.add_path(u'dir', kind='directory',
            file_id='hg:dir')
        entry.revision = tip
        entry = revone_inventory.add_path(u'dir/c', kind='file',
            file_id='hg:dir_sc')
        entry.revision = tip
        entry.text_size = len('contents of hg/dir/c\n')
        entry.text_sha1 = "958be752affac0fee70471331b96fb3fc1809425"
        self.revone_inventory = revone_inventory
        self.revidone = tip
        #====== end revision one
        # in revisiontwo we add a new file to dir, which should not change
        # the revision_id on the inventory.
        self.build_tree(['hg/dir/d'])
        self.tree.add(['dir/d'])
        self.tree.commit('bar')
        self.revtwo_inventory = copy.deepcopy(revone_inventory)
        tip = self.tree.last_revision()
        entry = self.revtwo_inventory.add_path(u'dir/d', kind='file',
            file_id='hg:dir_sd')
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
        #====== end revision three
        # in revision four we change the file dir/c, which should not alter
        # the last-changed field for 'dir'.
        self.build_tree_contents([('hg/dir/c', 'new contents')])
        self.tree.commit('change dir/c')
        self.revfour_inventory = copy.deepcopy(self.revthree_inventory)
        tip = self.tree.last_revision()
        entry = self.revfour_inventory['hg:dir_sc']
        entry.revision = tip
        entry.text_size = len('new contents')
        entry.text_sha1 = "7ffa72b76d5d66da37f4b614b7a822c01f23c183"
        self.revidfour = tip
        #====== end revision four

    def test_inventory_from_manifest(self):
        repo = self.tree.branch.repository
        left = self.revone_inventory
        right = repo.revision_tree(self.revidone)
        left_byid = list(left.iter_entries_by_dir())
        right_byid = list(right.iter_entries_by_dir())
        for i in range(len(left_byid)):
            self.assertEqual(left_byid[i], right_byid[i])
        self.assertEqual(left_byid, right_byid)
        left = self.revtwo_inventory
        right = repo.revision_tree(self.revidtwo)
        self.assertEqual(left_byid, right_byid)
        left = self.revthree_inventory
        right = repo.revision_tree(self.revidthree)
        self.assertEqual(left_byid, right_byid)
        left = self.revfour_inventory
        right = repo.revision_tree(self.revidfour)
        self.assertEqual(left_byid, right_byid)

    def test_initial_revision_from_changelog(self):
        converted_rev = self.tree.branch.repository.get_revision(self.revidone)
        self.assertEqual((), converted_rev.parent_ids)
        self.assertEqual({"hg:extra:branch": base64.b64encode("default"),
            'manifest': '18b146e67ed06c840bc37906f14f232a6139e4a4',
            'converted-from': 'hg %s\n' % mercurial.node.hex(converted_rev.foreign_revid)},
            converted_rev.properties)
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
        self.assertEqual((self.revidone,), converted_rev.parent_ids)
        self.assertEqual({"hg:extra:branch": base64.b64encode("default"),
            'manifest': '23f1837a1232d834ec828ba402711f2a81f1403e',
            'converted-from': 'hg %s\n' % mercurial.node.hex(converted_rev.foreign_revid)},
            converted_rev.properties)
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
        self.assertFileEqual('new contents', 'bzr/dir/c')
