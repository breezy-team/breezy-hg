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

import base64

from mercurial.node import (
    hex,
    nullid,
    )

from breezy.plugins.hg.mapping import (
    ExperimentalHgMapping,
    HgMappingv1,
    convert_converted_from,
    generate_convert_revision,
    as_bzr_parents,
    as_hg_parents,
    escape_path,
    flags_kind,
    unescape_path,
    )
from breezy import (
    errors,
    )
from breezy.revision import (
    NULL_REVISION,
    Revision,
    )
from breezy.tests import (
    TestCase,
    )

class HgMappingTests(object):

    def test_revid_foreign_to_bzr(self):
        self.assertEquals(self.mapping.revid_prefix + ":" + hex("a" * 20),
            self.mapping.revision_id_foreign_to_bzr("a" * 20))

    def test_revid_bzr_to_foreign(self):
        self.assertEquals("myrev", 
                self.mapping.revision_id_bzr_to_foreign(
                    self.mapping.revid_prefix + ":" + hex("myrev"))[0])

    def test_revid_bzr_to_foreign_invalid(self):
        self.assertRaises(errors.InvalidRevisionId, 
                self.mapping.revision_id_bzr_to_foreign, "foo:bar")

    def test_generate_file_id(self):
        self.assertEquals("hg:foo_sbar",
            self.mapping.generate_file_id("foo/bar"))

    def test_parse_file_id(self):
        self.assertEquals("foo/bar",
            self.mapping.parse_file_id("hg:foo_sbar"))

    def test_parse_file_id_invalid(self):
        self.assertRaises(ValueError, self.mapping.parse_file_id, "bar")


# TODO: Adapters:

class ExperimentalMappingTests(TestCase,HgMappingTests):

    def setUp(self):
        TestCase.setUp(self)
        self.mapping = ExperimentalHgMapping()


class Mappingv1Tests(TestCase,HgMappingTests):

    def setUp(self):
        TestCase.setUp(self)
        self.mapping = HgMappingv1()


class EscapePathTests(TestCase):

    def test_unescape(self):
        self.assertEquals("foo", unescape_path("foo"))
        self.assertEquals("f oo", unescape_path("f_woo"))
        self.assertEquals("f/oo", unescape_path("f_soo"))
        self.assertEquals("f_oo", unescape_path("f__oo"))
        self.assertRaises(ValueError, unescape_path, "f_oo")
        self.assertEquals("foo _", unescape_path("foo_w__"))

    def test_escape(self):
        self.assertEquals("bar", escape_path("bar"))
        self.assertEquals("bar_w", escape_path("bar "))
        self.assertEquals("bar_sblie_s", escape_path("bar/blie/"))
        self.assertEquals("bar____", escape_path("bar__"))


class ExportRevisionTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.mapping = ExperimentalHgMapping()

    def test_export(self):
        rev = Revision("myrevid")
        rev.committer = u"Jelmer <foo>"
        rev.message = u"ürk"
        rev.timestamp = 432432
        rev.timezone = 0
        rev.properties = {
                "something": u"else",
                "hg:extra:foo": base64.b64encode("bar")}
        (manifest, user, (time, timezone), desc, extra) = \
            self.mapping.export_revision(rev)
        self.assertEquals("Jelmer <foo>", user)
        self.assertEquals(None, manifest)
        self.assertEquals(u"ürk", desc)
        self.assertEquals({"bzr-revprop-something": "else", "foo": "bar"}, 
                          extra)


class FlagsKindTests(TestCase):

    def test_link(self):
        self.assertEquals('symlink', flags_kind({"bar": "l"}, "bar"))

    def test_other(self):
        self.assertEquals('file', flags_kind({"bar": "t"}, "bar"))

    def test_file(self):
        self.assertEquals('file', flags_kind({}, "bar"))


class AsHgParentsTests(TestCase):

    def test_empty(self):
        self.assertEquals((nullid, nullid), as_hg_parents([], None))

    def test_octopus(self):
        m = { "reva": "a"*20, "revb": "b"*20, "revc": "c"*20 }
        self.assertEquals(("a"*20, "b"*20), 
            as_hg_parents(["reva", "revb", "revc"], m.__getitem__))

    def test_one(self):
        m = { "reva": "a"*20 }
        self.assertEquals(("a"*20, nullid), 
            as_hg_parents(["reva"], m.__getitem__))


class AsBzrParentsTests(TestCase):

    def test_empty(self):
        self.assertEquals((), as_bzr_parents((nullid, nullid), None))

    def test_first_null(self):
        m = {"a" * 20: "reva"}
        self.assertEquals((NULL_REVISION, "reva"), 
                as_bzr_parents((nullid, "a" * 20), m.__getitem__))

    def test_second_null(self):
        m = {"a" * 20: "reva"}
        self.assertEquals(("reva",), 
                as_bzr_parents(("a" * 20, nullid), m.__getitem__))


class ConvertConvertedFromTests(TestCase):

    def test_unspecified(self):
        self.assertEquals("unspecified foo\n",
            convert_converted_from("foo"))

    def test_svn(self):
        self.assertEquals("svn myuuid:42:path\n",
            convert_converted_from("svn:myuuid/path@42"))

    def test_unknown(self):
        self.assertRaises(KeyError,
            convert_converted_from, "unknown:myuuid:42:path")


class GenerateConvertRevisionTests(TestCase):

    def test_generate_convert_revision_svn(self):
        self.assertEquals("svn:myuuid/path@42",
            generate_convert_revision("svn myuuid:42:path"))

    def test_generate_convert_revision_unspecified(self):
        self.assertEquals("bla",
            generate_convert_revision("unspecified bla"))

    def test_generate_convert_revision_unknown(self):
        self.assertRaises(KeyError,
            generate_convert_revision, "bla bla")
