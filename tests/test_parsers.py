# Copyright (C) 2009 Jelmer Vernooij <jelmer@jelmer.uk>

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

from breezy.plugins.hg.parsers import (
    decode_str,
    deserialize_file_text,
    format_changeset,
    parse_changeset,
    serialize_file_text,
    )
from breezy.tests import (
    TestCase,
    )

class ChangesetFormatterTests(TestCase):

    def test_simple(self):
        self.assertEquals("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@jelmer.uk>
1253260798 -7200
myfile

Some
commit
message""",
            format_changeset(mercurial.node.nullid, ["myfile"],
                u"Jelmer Vernooij <jelmer@jelmer.uk>",
                (1253260798.0, -7200), u"Some\ncommit\nmessage",
                {}))

    def test_extra(self):
        self.assertEquals("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@jelmer.uk>
1253260798 -7200 extra:data\x00more:extra
myfile

Some
commit
message""",
            format_changeset(mercurial.node.nullid, ["myfile"],
                u"Jelmer Vernooij <jelmer@jelmer.uk>",
                (1253260798.0, -7200), u"Some\ncommit\nmessage",
                {"extra": "data", "more":"extra"}))

    def test_invalid_author(self):
        self.assertRaises(ValueError, format_changeset, 
                mercurial.node.nullid, ["myfile"],
                u"",
                (1253260798.0, -7200), u"Some\ncommit\nmessage",
                {})
        self.assertRaises(ValueError, format_changeset, 
                mercurial.node.nullid, ["myfile"],
                u"Jelmer\nVernooij",
                (1253260798.0, -7200), u"Some\ncommit\nmessage",
                {})

    def test_invalid_date(self):
        self.assertRaises(TypeError, format_changeset, 
                mercurial.node.nullid, ["myfile"],
                u"Jelmer Vernooij <jelmer@jelmer.uk>",
                1253260798, u"Some\ncommit\nmessage",
                {})


class ParseChangesetTests(TestCase):

    def test_simple(self):
        self.assertEquals((mercurial.node.nullid, 
                u"Jelmer Vernooij <jelmer@jelmer.uk>",
                (1253260798.0, -7200), ["myfile"], u"Some\ncommit\nmessage",
                {}), parse_changeset("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@jelmer.uk>
1253260798 -7200
myfile

Some
commit
message"""))

    def test_extra(self):
        self.assertEquals((mercurial.node.nullid, 
                u"Jelmer Vernooij <jelmer@jelmer.uk>",
                (1253260798.0, -7200), ["myfile"], u"Some\ncommit\nmessage",
                {"date": "extra"}), 
                parse_changeset("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@jelmer.uk>
1253260798 -7200 date:extra
myfile

Some
commit
message"""))

    def test_invalid_timezone(self):
        self.assertEquals((mercurial.node.nullid, 
                "Jelmer Vernooij <jelmer@jelmer.uk>",
                (1253260798.0, 0), ["myfile"], "Some\ncommit\nmessage",
                {}), 
                parse_changeset("""0000000000000000000000000000000000000000
Jelmer Vernooij <jelmer@jelmer.uk>
1253260798 bla
myfile

Some
commit
message"""))


class TextSerializers(TestCase):

    def test_serialize(self):
        self.assertEquals("\1\ncopy: bla\n\1\nfoo\n", serialize_file_text({"copy": "bla"}, "foo\n"))

    def test_deserialize(self):
        self.assertEquals(({"copy": "bla"}, "foo\n"), deserialize_file_text("\1\ncopy: bla\n\1\nfoo\n"))



class DecodeStrTests(TestCase):

    def test_decode_ascii(self):
        self.assertEquals(u"foo", decode_str("foo"))

    def test_decode_utf8(self):
        self.assertEquals("\xc3\xa4".decode("utf-8"), decode_str('\xc3\xa4'))

    def test_decode_latin1(self):
        self.assertEquals("\xc3\xa4".decode("utf-8"), decode_str('\xe4'))
