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

"""Support in "bzr send" for Mercurial bundles."""

import time
from breezy import (
    branch as _mod_branch,
    merge_directive,
    osutils,
    )
from mercurial import (
    changegroup,
    )

from breezy.plugins.hg.mapping import (
    default_mapping,
    )
from breezy.plugins.hg.changegroup import (
    dchangegroup,
    )


BUNDLE_TYPE = 'HG10BZ'


class HgMergeDirective(merge_directive._BaseMergeDirective):

    def to_lines(self):
        return self.patch.splitlines(True)

    @classmethod
    def from_objects(cls, repository, revision_id, time, timezone,
                     target_branch, local_target_branch=None,
                     public_branch=None, message=None):
        submit_branch = _mod_branch.Branch.open(target_branch)
        submit_branch.lock_read()
        try:
            submit_revision_id = submit_branch.last_revision()
            repository.fetch(submit_branch.repository, submit_revision_id)
            graph = repository.get_graph()
            todo = graph.find_difference(submit_revision_id, revision_id)[1]
            cg, revidmap = dchangegroup(repository,
                getattr(submit_branch, "mapping", default_mapping), todo)
            fn = changegroup.writebundle(cg, None, BUNDLE_TYPE)
            f = open(fn, 'r')
            try:
                contents = f.read()
            finally:
                f.close()
        finally:
            submit_branch.unlock()
        return cls(revision_id, None, time, timezone, target_branch, contents,
                   None, public_branch, message)


def send_hg(branch, revision_id, submit_branch, public_branch,
              no_patch, no_bundle, message, base_revision_id):
    return HgMergeDirective.from_objects(
        branch.repository, revision_id, time.time(),
        osutils.local_time_offset(), submit_branch,
        public_branch=public_branch, message=message)
