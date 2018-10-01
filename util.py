# Copyright (C) 2009-2010 Jelmer Vernooij <jelmer@jelmer.uk>
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


"""Utility datatypes.

"""

from collections import defaultdict

class lazydict(defaultdict):

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.default_factory, dict(self))

    def __missing__(self, key):
        self[key] = value = self.default_factory(key)
        return value
