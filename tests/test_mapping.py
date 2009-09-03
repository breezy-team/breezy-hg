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

from mercurial.node import (
    hex,
    bin,
    )

from bzrlib.plugins.hg.mapping import (
    ExperimentalHgMapping,
    )
from bzrlib import (
    errors,
    )
from bzrlib.tests import (
    TestCase,
    )
       
class HgMappingTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.mapping = ExperimentalHgMapping()

    def test_revid_foreign_to_bzr(self):
        self.assertEquals("hg-experimental:" + hex("myrev"),
            self.mapping.revision_id_foreign_to_bzr("myrev"))

    def test_revid_bzr_to_foreign(self):
        self.assertEquals("myrev", 
                self.mapping.revision_id_bzr_to_foreign(
                    "hg-experimental:" + hex("myrev"))[0])

    def test_revid_bzr_to_foreign_invalid(self):
        self.assertRaises(errors.InvalidRevisionId, 
                self.mapping.revision_id_bzr_to_foreign, "foo:bar")

    def test_generate_file_id(self):
        self.assertEquals("hg:foo:bar",
            self.mapping.generate_file_id("foo/bar"))
