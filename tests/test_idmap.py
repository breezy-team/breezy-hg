# Copyright (C) 2009 Jelmer Vernooij <jelmer@jelmer.uk>
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

from breezy.tests import TestCase

from breezy.plugins.hg.idmap import (
    MemoryIdmap,
    SqliteIdmap,
    TdbIdmap,
    )

class IdmapTestCase(object):

    def test_lookup_manifest_revid_noexistant(self):
        self.assertRaises(KeyError, self.idmap.lookup_revision_by_manifest_id, "a" * 20)

    def test_lookup_manifest_revid(self):
        self.idmap.insert_revision("jelmer@voo", "a" * 20, "a"*20, "c" * 20)
        self.assertEquals("jelmer@voo", self.idmap.lookup_revision_by_manifest_id("a" * 20))

    def test_revids(self):
        self.idmap.insert_revision("jelmer@voo", "a" * 20, "a"*20, "c" * 20)
        self.idmap.insert_revision("jelmer@bar", "b" * 20, "b"*20, "d" * 20)
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


class SqliteIdmapTests(TestCase,IdmapTestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.idmap = SqliteIdmap()

