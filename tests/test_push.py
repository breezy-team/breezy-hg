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


from bzrlib.tests import TestCase

from bzrlib.plugins.hg.changegroup import (
    chunkify,
    extract_base,
    )


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
