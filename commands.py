# Copyright (C) 2010 Jelmer Vernooij <jelmer@jelmer.uk>
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


"""Commands specific to dealing with Mercurial branches."""


from breezy import (
    errors,
    )
from breezy.commands import (
    Command,
    )

import os


class cmd_hg_import(Command):
    """Convert a Mercurial repository to a Bazaar repository.

    """
    _see_also = ['formats']
    takes_args = ['from_location', 'to_location?']

    def run(self, from_location, to_location=None):
        from breezy.controldir import ControlDir
        if to_location is None:
            to_location = os.path.basename(from_location.rstrip("/\\"))
        from_dir = ControlDir.open(from_location)
        try:
            to_dir = ControlDir.open(to_location)
        except errors.NotBranchError:
            to_dir = ControlDir.create(to_location)
        try:
            to_repo = to_dir.open_repository()
        except errors.NoRepositoryPresent:
            to_repo = to_dir.create_repository()
        try:
            to_branch = to_dir.open_branch()
        except errors.NotBranchError:
            to_branch = to_dir.create_branch()
        to_branch.pull(from_dir.open_branch())
