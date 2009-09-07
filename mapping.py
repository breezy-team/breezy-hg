# Copyright (C) 2005, 2006, 2009 Canonical Ltd
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""Mappings."""

from mercurial.node import (
    hex,
    bin,
    )

from bzrlib import (
    errors,
    foreign,
    )

class ExperimentalHgMapping(foreign.VcsMapping):
    """Class that maps between Bazaar and Mercurial semantics."""
    experimental = True
    revid_prefix = "hg-experimental"

    def __init__(self):
        super(ExperimentalHgMapping, self).__init__(foreign_hg)

    @classmethod
    def revision_id_foreign_to_bzr(cls, revision_id):
        return "%s:%s" % (cls.revid_prefix, hex(revision_id))

    @classmethod
    def revision_id_bzr_to_foreign(cls, revision_id):
        if not revision_id.startswith("%s:" % cls.revid_prefix):
            raise errors.InvalidRevisionId(revision_id, cls)
        return bin(revision_id[len(cls.revid_prefix)+1:]), cls()

    @classmethod
    def generate_file_id(self, path):
        """Create a synthetic file_id for an hg file."""
        return "hg:" + path.replace('/', ':')


class HgMappingRegistry(foreign.VcsMappingRegistry):
    """Registry of all Bazaar <-> Mercurial mappings."""

    def revision_id_bzr_to_foreign(self, bzr_revid):
        if not bzr_revid.startswith("hg-"):
            raise errors.InvalidRevisionId(bzr_revid, None)
        (mapping_version, hg_ref) = bzr_revid.split(":", 1)
        mapping = self.get(mapping_version)
        return mapping.revision_id_bzr_to_foreign(bzr_revid)

    parse_revision_id = revision_id_bzr_to_foreign


mapping_registry = HgMappingRegistry()
mapping_registry.register_lazy("hg-experimental", "bzrlib.plugins.hg.mapping",
    "ExperimentalHgMapping")

class ForeignHg(foreign.ForeignVcs):
    """Foreign Mercurial."""

    def __init__(self):
        super(ForeignHg, self).__init__(mapping_registry)

    @classmethod
    def show_foreign_revid(cls, foreign_revid):
        return { "hg commit": hex(foreign_revid) }


foreign_hg = ForeignHg()
default_mapping = ExperimentalHgMapping()
