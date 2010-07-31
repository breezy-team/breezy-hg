# Copyright (C) 2010 Jelmer Vernooij <jelmer@samba.org>

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

from bzrlib.bzrdir import (
    format_registry,
    )

from bzrlib.tests import (
    TestCase,
    )

from bzrlib.plugins.hg import (
    HgBzrDirFormat,
    )


class HgBzrDirFormatTests(TestCase):

    def test_eq(self):
        format1 = HgBzrDirFormat()
        format2 = HgBzrDirFormat()
        self.assertEquals(format1, format2)
        self.assertEquals(format1, format1)
        bzr_format = format_registry.make_bzrdir("default")
        self.assertNotEquals(bzr_format, format1)

    def test_network_name(self):
        format = HgBzrDirFormat()
        self.assertEquals("hg", format.network_name())
