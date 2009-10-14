# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
# -*- encoding: utf-8 -*-

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

from bzrlib.tests import TestCase

from bzrlib.plugins.hg.idmap import (
    MemoryIdmap,
    TdbIdmap,
    )

class IdmapTestCase(object):

    def test_lookup_manifest_revid_noexistant(self):
        self.assertRaises(KeyError, self.idmap.lookup_revision_by_manifest_id, "a" * 20)

    def test_lookup_manifest_revid(self):
        self.idmap.insert_manifest("a" * 20, "jelmer@voo")
        self.assertEquals("jelmer@voo", self.idmap.lookup_revision_by_manifest_id("a" * 20))

    def test_revids(self):
        self.idmap.insert_manifest("a" * 20, "jelmer@voo")
        self.idmap.insert_manifest("b" * 20, "jelmer@bar")
        self.assertEquals(set(["jelmer@voo", "jelmer@bar"]), 
            self.idmap.revids())


class MemoryIdmapTests(TestCase,IdmapTestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.idmap = MemoryIdmap()


class TdbIdmapTests(TestCase,IdmapTestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.idmap = TdbIdmap()

