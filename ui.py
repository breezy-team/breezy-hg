# Copyright (C) 2009 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Mercurial UI interface implementation that translates to Bazaar calls."""

from breezy import (
    config,
    trace,
    )

import mercurial.ui

class ui(mercurial.ui.ui):

    def debug(self, *msg):
        for x in msg:
            trace.mutter("hg: %s" % x.rstrip())

    def warn(self, *msg):
        for x in msg:
            trace.warning("hg: %s" % x.rstrip())

    def note(self, *msg):
        for x in msg:
            trace.mutter("hg: %s" % x.rstrip())

    def status(self, *msg):
        for x in msg:
            trace.mutter("hg: %s" % x.rstrip())

    def username(self):
        return config.GlobalConfig().username()
