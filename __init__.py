# Copyright (C) 2005, 2006 Canonical Ltd
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


"""Mercurial support for Bazaar.

"""

import bzrlib
import bzrlib.api

from info import (
    bzr_compatible_versions,
    hg_compatible_version_strings,
    bzr_plugin_version as version_info,
    )

bzrlib.api.require_any_api(bzrlib, bzr_compatible_versions)

from bzrlib import (
    errors,
    trace,
    )
from bzrlib.foreign import (
    foreign_vcs_registry,
    )
from bzrlib.send import (
    format_registry as send_format_registry,
    )

from bzrlib.controldir import (
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


foreign_vcs_registry.register_lazy("hg",
    "bzrlib.plugins.hg.mapping", "foreign_hg", "Mercurial")

def has_hg_http_smart_server(transport):
    try:
        url = transport.external_url() + "?pairs=%s-%s&cmd=between" % (
            "0" * 40, "0" * 40)
    except errors.InProcessTransport:
        return False
    if not url.startswith("http:") and not url.startswith("https:"):
        return False
    from bzrlib.transport.http._urllib import HttpTransport_urllib, Request
    if isinstance(transport, HttpTransport_urllib):
        req = Request('GET', url, accepted_errors=[200, 403, 404, 405])
        req.follow_redirections = True
        resp = transport._perform(req)
        if resp.code == 404:
            return False
        headers = resp.headers
    else:
        try:
            from bzrlib.transport.http._pycurl import PyCurlTransport
        except errors.DependencyNotPresent:
            return False
        else:
            import pycurl
            from cStringIO import StringIO
            if isinstance(transport, PyCurlTransport):
                conn = transport._get_curl()
                conn.setopt(pycurl.URL, url)
                transport._set_curl_options(conn)
                conn.setopt(pycurl.HTTPGET, 1)
                conn.setopt(pycurl.NOBODY, 1)
                header = StringIO()
                data = StringIO()
                conn.setopt(pycurl.HEADERFUNCTION, header.write)
                conn.setopt(pycurl.WRITEFUNCTION, data.write)
                transport._curl_perform(conn, header)
                code = conn.getinfo(pycurl.HTTP_CODE)
                if code == 404:
                    raise errors.NoSuchFile(transport._path)
                headers = transport._parse_headers(header)
            else:
                return False
    ct = headers.getheader("Content-Type")
    return ct.startswith("application/mercurial")


def has_hg_dumb_repository(transport):
    try:
        return transport.has(".hg/requires")
    except (errors.NoSuchFile, errors.PermissionDenied):
        return False


class HgProber(Prober):

    def probe_transport(self, transport):
        # little ugly, but works
        from bzrlib.transport.local import LocalTransport
        lazy_load_mercurial()
        from mercurial import error as hg_errors
        # Over http, look for the hg smart server

        if (not has_hg_dumb_repository(transport) and
            not has_hg_http_smart_server(transport)):
            # Explicitly check for .hg directories here, so we avoid
            # loading foreign branches through Mercurial.
            raise errors.NotBranchError(path=transport.base)
        import urllib2
        from bzrlib.plugins.hg.dir import HgControlDirFormat
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


ControlDirFormat.register_prober(HgProber)
ControlDirFormat._server_probers.insert(0, HgProber)
from bzrlib.plugins.hg.dir import HgControlDirFormat
ControlDirFormat.register_format(HgControlDirFormat())

controldir_network_format_registry.register_lazy("hg",
    "bzrlib.plugins.hg.dir", "HgControlDirFormat")

bzrlib.bzrdir.format_registry.register_lazy("hg",
    "bzrlib.plugins.hg.dir", "HgControlDirFormat",
    "Mercurial repository. ", native=False, hidden=False)

from bzrlib.repository import (
    format_registry as repository_format_registry,
    network_format_registry as repository_network_format_registry,
    )
repository_network_format_registry.register_lazy('hg',
    'bzrlib.plugins.hg.repository', 'HgRepositoryFormat')

try:
    register_extra_lazy_repository_format = getattr(repository_format_registry,
        "register_extra_lazy")
except AttributeError:
    pass
else:
    register_extra_lazy_repository_format('bzrlib.plugins.hg.repository',
        'HgRepositoryFormat')

send_format_registry.register_lazy('hg', 'bzrlib.plugins.hg.send',
                                   'send_hg', 'Mecurial bundle format')

from bzrlib.revisionspec import revspec_registry
revspec_registry.register_lazy("hg:", "bzrlib.plugins.hg.revspec",
    "RevisionSpec_hg")

from bzrlib.commands import (
    plugin_cmds,
    )
plugin_cmds.register_lazy('cmd_hg_import', [], 'bzrlib.plugins.hg.commands')

from bzrlib.revisionspec import dwim_revspecs
from bzrlib.plugins.hg.revspec import RevisionSpec_hg
dwim_revspecs.append(RevisionSpec_hg)


def test_suite():
    from unittest import TestSuite, TestLoader
    from bzrlib.plugins.hg import tests

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
