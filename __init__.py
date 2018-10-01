# Copyright (C) 2005, 2006 Canonical Ltd
# Copyright (C) 2008 Jelmer Vernooij <jelmer@jelmer.uk>
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


"""Mercurial support for Bazaar.

"""

from __future__ import absolute_import

import breezy
from breezy import version_info
from breezy.transport import register_transport_proto

hg_compatible_versions = [(4, 7)]

hg_compatible_version_strings = ["%d.%d" % x for x in hg_compatible_versions]

from breezy.i18n import load_plugin_translations
translation = load_plugin_translations("bzr-hg")
gettext = translation.gettext

from breezy import (
    errors,
    trace,
    )
from breezy.controldir import (
    network_format_registry as controldir_network_format_registry,
    ControlDirFormat,
    Prober,
    )

_mercurial_loaded = False

def lazy_load_mercurial():
    global _mercurial_loaded
    if _mercurial_loaded:
        return
    _mercurial_loaded = True
    import mercurial
    try:
        from mercurial import demandimport
        demandimport.enable = lambda: None
    except ImportError:
        pass
    from mercurial.__version__ import version as hg_version
    if hg_version != "unknown":
        hg_major_version = ".".join(hg_version.split(".")[:2])
        if hg_major_version not in hg_compatible_version_strings and not "+" in hg_version:
            raise errors.DependencyNotPresent("mercurial",
                'bzr-hg: Mercurial version %s not supported.' % hg_version)
    trace.mutter("bzr-hg: using Mercurial %s" % hg_version)


try:
    from breezy.registry import register_lazy
except ImportError:
    from breezy.foreign import (
        foreign_vcs_registry,
        )
    foreign_vcs_registry.register_lazy("hg",
        "breezy.plugins.hg.mapping", "foreign_hg", "Mercurial")
    from breezy.send import (
        format_registry as send_format_registry,
        )
    send_format_registry.register_lazy('hg', 'breezy.plugins.hg.send',
                                       'send_hg', 'Mecurial bundle format')
else:
    register_lazy("breezy.foreign", "foreign_vcs_registry", "hg",
        "breezy.plugins.hg.mapping", "foreign_hg", "Mercurial")
    register_lazy("breezy.send", "format_registry", 'hg',
            'breezy.plugins.hg.send', 'send_hg', 'Mecurial bundle format')

def has_hg_http_smart_server(transport, external_url):
    """Check if there is a Mercurial smart server at the remote location.

    :param transport: Transport to check
    :param externa_url: External URL for transport
    :return: Boolean indicating whether transport is backed onto hg
    """
    from breezy.transport.http import Request
    url = external_url.rstrip("/") + "?pairs=%s-%s&cmd=between" % ("0" * 40, "0" * 40)
    req = Request('GET', url, accepted_errors=[200, 403, 404, 405])
    req.follow_redirections = True
    resp = transport._perform(req)
    if resp.code == 404:
        return False
    headers = resp.headers
    ct = headers.getheader("Content-Type")
    if ct is None:
        return False
    return ct.startswith("application/mercurial")


def has_hg_dumb_repository(transport):
    try:
        return transport.has_any([".hg/requires", ".hg/00changelog.i"])
    except (errors.NoSuchFile, errors.PermissionDenied,
            errors.InvalidHttpResponse):
        return False


class HgProber(Prober):

    # Perhaps retrieve list from mercurial.hg.schemes ?
    _supported_schemes = ["http", "https", "file", "ssh"]

    def probe_transport(self, transport):
        try:
            external_url = transport.external_url()
        except errors.InProcessTransport:
            raise errors.NotBranchError(path=transport.base)
        scheme = external_url.split(":")[0]
        if scheme not in self._supported_schemes:
            raise errors.NotBranchError(path=transport.base)
        from breezy import urlutils
        external_url = urlutils.split_segment_parameters(external_url)[0]
        # Explicitly check for .hg directories here, so we avoid
        # loading foreign branches through Mercurial.
        if (external_url.startswith("http:") or
            external_url.startswith("https:")):
            if not has_hg_http_smart_server(transport, external_url):
                raise errors.NotBranchError(path=transport.base)
        else:
            if not has_hg_dumb_repository(transport):
                raise errors.NotBranchError(path=transport.base)

        lazy_load_mercurial()
        from mercurial import error as hg_errors

        import urllib2
        from breezy.plugins.hg.dir import HgControlDirFormat
        format = HgControlDirFormat()
        try:
            format.open(transport)
        except hg_errors.RepoError, e:
            raise errors.NotBranchError(path=transport.base)
        except hg_errors.Abort, e:
            trace.mutter('not a hg branch: %s', e)
            raise errors.NotBranchError(path=transport.base)
        except urllib2.HTTPError, e:
            trace.mutter('not a hg branch: %s', e)
            raise errors.NotBranchError(path=transport.base)
        return format

    @classmethod
    def known_formats(cls):
        from breezy.plugins.hg.dir import HgControlDirFormat
        return [HgControlDirFormat()]


ControlDirFormat.register_prober(HgProber)
ControlDirFormat._server_probers.insert(0, HgProber)

controldir_network_format_registry.register_lazy("hg",
    "breezy.plugins.hg.dir", "HgControlDirFormat")

breezy.controldir.format_registry.register_lazy("hg",
    "breezy.plugins.hg.dir", "HgControlDirFormat",
    "Mercurial repository. ", native=False, hidden=False)

from breezy.repository import (
    format_registry as repository_format_registry,
    network_format_registry as repository_network_format_registry,
    )
repository_network_format_registry.register_lazy('hg',
    'breezy.plugins.hg.repository', 'HgRepositoryFormat')


from breezy.branch import (
    format_registry as branch_format_registry,
    network_format_registry as branch_network_format_registry,
    )
branch_network_format_registry.register_lazy(
    "hg", "breezy.plugins.hg.branch", "HgBranchFormat")

branch_format_registry.register_extra_lazy(
    "breezy.plugins.hg.branch", "HgBranchFormat")

from breezy.workingtree import (
    format_registry as workingtree_format_registry,
    )
workingtree_format_registry.register_extra_lazy(
    "breezy.plugins.hg.workingtree", "HgWorkingTreeFormat")

repository_format_registry.register_extra_lazy('breezy.plugins.hg.repository',
        'HgRepositoryFormat')

from breezy.revisionspec import revspec_registry
revspec_registry.register_lazy("hg:", "breezy.plugins.hg.revspec",
    "RevisionSpec_hg")

from breezy.commands import (
    plugin_cmds,
    )
plugin_cmds.register_lazy('cmd_hg_import', [], 'breezy.plugins.hg.commands')

register_transport_proto('hg+ssh://',
        help="Access using the Mercurial smart server protocol over SSH.")

from breezy.revisionspec import RevisionSpec_dwim
RevisionSpec_dwim.append_possible_lazy_revspec(
    "breezy.plugins.hg.revspec", "RevisionSpec_hg")

def test_suite():
    from unittest import TestSuite, TestLoader
    from breezy.plugins.hg import tests

    suite = TestSuite()

    suite.addTest(tests.test_suite())

    return suite


# TODO: test that a last-modified in a merged branch is correctly assigned
# TODO: test that we set the per file parents right: that is the rev of the
# last heads, *not* the revs of the revisions parents. (note, this should be
# right already, because if the use of find_previous_heads, but surety is nice.)
# TODO: test that the assignment of .revision to directories works correctly
# in the following case:
# branch A adds dir/file in rev X
# branch B adds dir/file2 in rev Y
# branch B merges branch A: there are *two* feasible revisions that both claim
# to have created 'dir' - X and Y. We want to always choose lower-sorting one.
# Its potentially impossible to guarantee a consistent choice otherwise, and
# having the dir flip-flop in bzr would be unhelpful. This choice is what I
# think has been implemented, but not having got hg merges operating from
# within python yet, its quite hard to produce this scenario to test.
# TODO: file version extraction should elide 'copy' and 'copyrev file headers.
