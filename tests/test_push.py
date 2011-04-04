# Copyright (C) 2011 Canonical Ltd

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

"""Tests for pushing revisions into Mercurial repositories."""


from bzrlib.tests import (
    TestCase,
    TestCaseWithTransport,
    )

from mercurial.node import nullid

from bzrlib.plugins.hg.changegroup import (
    chunkify,
    dinventories,
    drevisions,
    extract_base,
    )
from bzrlib.plugins.hg.mapping import default_mapping
from bzrlib.plugins.hg.overlay import get_overlay


class ChunkifyTests(TestCase):

    def test_empty(self):
        self.assertEquals("\0\0\0\x04", chunkify(""))

    def test_somebytes(self):
        self.assertEquals("\0\0\0\x08abcd", chunkify("abcd"))


class ExtractBaseTests(TestCase):

    def test_empty(self):
        (entries, base) = extract_base(iter([]))
        self.assertEquals("", base)
        self.assertEquals([], list(entries))

    def test_foo(self):
        (entries, base) = extract_base(iter(["c", "a", "b"]))
        self.assertEquals("c", base)
        self.assertEquals(["a", "b"], list(entries))


class DrevisionsTests(TestCaseWithTransport):

    def setUp(self):
        super(DrevisionsTests, self).setUp()
        self.tree = self.make_branch_and_tree('.')
        self.mapping = default_mapping
        self.overlay = get_overlay(self.tree.branch.repository, self.mapping)

    def drevs(self, revids, files, changelog_ids, manifest_ids, fileids={}, lossy=True):
        return drevisions(self.tree.branch.repository, self.mapping,
            revids, files, changelog_ids, manifest_ids,
            self.overlay, fileids, lossy)

    def test_empty(self):
        self.assertEquals([], list(self.drevs([], {}, {}, {})))

    def test_null(self):
        self.assertEquals([
            ("", (nullid, nullid), nullid),
            ], list(self.drevs(["null:"], {}, {}, {})))

    def test_first(self):
        revid = self.tree.commit("foo", timestamp=3434343434, timezone=3600)
        self.assertEquals([
            ("", (nullid, nullid), nullid),
             ('6d616e69666573746964\njrandom@example.com\n3434343434 -3600 bzr-revprop-branch-nick:work\n\nfoo',
               ('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
             '\xa7yH\x95\xc1\xbf\xa8$\xe9N\x08b\x1c\x82\xe5\x10\xd8\rj\xc6'),
            ], list(self.drevs(["null:", revid], {revid:{}}, {}, {revid:"manifestid"})))


class DinventoriesTests(TestCaseWithTransport):

    def setUp(self):
        super(DinventoriesTests, self).setUp()
        self.tree = self.make_branch_and_tree('.')
        self.mapping = default_mapping
        self.overlay = get_overlay(self.tree.branch.repository, self.mapping)

    def dinvs(self, revids, manifest_ids, files, fileids={}, lossy=True):
        self.tree.lock_read()
        try:
            return list(dinventories(self.tree.branch.repository, self.mapping,
                revids, manifest_ids, files, self.overlay,
                self.tree.branch.repository.texts, fileids, lossy))
        finally:
            self.tree.unlock()

    def test_none(self):
        self.assertEquals([], self.dinvs([], {}, {}, {}))

    def test_null(self):
        self.assertEquals([
            ("", (nullid, nullid), "null:"),
            ], self.dinvs(["null:"], {}, {}, {}))

    def test_empty(self):
        revid = self.tree.commit("foo", timestamp=3434343434, timezone=3600)
        self.assertEquals([
            ("", (nullid, nullid), "null:"),
             ('',
               ('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
             revid),
             ], self.dinvs(["null:", revid], {revid:"manifestid"}, {revid:{}}))
