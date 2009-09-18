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

import mercurial

from bzrlib.plugins.hg.fetch import (
    format_changeset,
    parse_changeset,
    )
from bzrlib.tests import (
    TestCase,
    )
       
class ChangesetFormatterTests(TestCase):

    def test_simple(self):
        self.assertEquals("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@samba.org>
1253260798 -7200
myfile

Some
commit
message""",
            format_changeset(mercurial.node.nullid, ["myfile"],
                "Jelmer Vernooij <jelmer@samba.org>",
                "2009-09-18 09:59:58", "Some\ncommit\nmessage",
                {}))


class ParseChangesetTests(TestCase):

    def test_simple(self):
        self.assertEquals((mercurial.node.nullid, 
                "Jelmer Vernooij <jelmer@samba.org>",
                (1253260798.0, -7200), ["myfile"], "Some\ncommit\nmessage",
                {}), parse_changeset("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@samba.org>
1253260798 -7200
myfile

Some
commit
message"""))
