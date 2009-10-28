# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
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

# Please note that imports are delayed as much as possible here since 
# if DWIM revspecs are supported this module is imported by __init__.py.

from bzrlib.errors import (
    InvalidRevisionId,
    InvalidRevisionSpec,
    )
from bzrlib.revisionspec import (
    RevisionInfo,
    RevisionSpec,
    )

def valid_hg_csid(csid):
    import binascii
    try:
        binascii.unhexlify(csid)
    except TypeError:
        return False
    else:
        return True


class RevisionSpec_hg(RevisionSpec):
    """Selects a revision using a Mercurial revision."""

    help_txt = """Selects a revision using a Mercurial revision sha1.
    """
    
    prefix = 'hg:'
    wants_revision_history = False

    def _lookup_csid(self, branch, csid):
        from bzrlib.plugins.hg.repository import (
            MercurialSmartRemoteNotSupported,
            )
        bzr_revid = branch.mapping.revision_id_foreign_to_bzr(csid)
        try:
            if branch.repository.has_revision(bzr_revid):
                history = self._history(branch, bzr_revid)
                return RevisionInfo.from_revision_id(branch, bzr_revid, history)
        except MercurialSmartRemoteNotSupported:
            return RevisionInfo(branch, None, bzr_revid)
        raise InvalidRevisionSpec(self.user_spec, branch)

    def _history(self, branch, revid):
        history = list(branch.repository.iter_reverse_revision_history(revid))
        history.reverse()
        return history

    def __nonzero__(self):
        from bzrlib.revision import (
            NULL_REVISION,
            )
        # The default implementation uses branch.repository.has_revision()
        if self.rev_id is None:
            return False
        if self.rev_id == NULL_REVISION:
            return False
        return True

    def _find_short_csid(self, branch, csid):
        import mercurial.node
        from bzrlib.plugins.hg.mapping import (
            mapping_registry,
            )
        parse_revid = getattr(branch.repository, "lookup_revision_id",
                              mapping_registry.parse_revision_id)
        branch.repository.lock_read()
        try:
            graph = branch.repository.get_graph()
            for revid, _ in graph.iter_ancestry([branch.last_revision()]):
                try:
                    foreign_revid, mapping = parse_revid(revid)
                except InvalidRevisionId:
                    continue
                if mercurial.node.hex(foreign_revid).startswith(csid):
                    history = self._history(branch, revid)
                    return RevisionInfo.from_revision_id(branch, revid, history)
            raise InvalidRevisionSpec(self.user_spec, branch)
        finally:
            branch.repository.unlock()

    def _match_on(self, branch, revs):
        loc = self.spec.find(':')
        csid = self.spec[loc+1:].encode("utf-8")
        if len(csid) > 40 or not valid_hg_csid(csid):
            raise InvalidRevisionSpec(self.user_spec, branch)
        from bzrlib.plugins.hg import (
            lazy_load_mercurial,
            )
        lazy_load_mercurial()
        if len(csid) == 40:
            return self._lookup_csid(branch, csid)
        else:
            return self._find_short_csid(branch, csid)

    def needs_branch(self):
        return True

    def get_branch(self):
        return None
