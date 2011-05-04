# Copyright (C) 2010 Leonid Borisenko <leo.borisenko@gmail.com>

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

"""Tests for fetching from Mercurial into Bazaar."""

from bzrlib.branch import Branch

from bzrlib.plugins.hg.dir import HgControlDirFormat
from bzrlib.plugins.hg.ui import ui as hgui

from bzrlib.tests import TestCaseWithTransport

from mercurial import hg
import mercurial.localrepo

class TestFetching(TestCaseWithTransport):

    def test_recursive_removing_of_empty_directories(self):
        # Create files in directory of Mercurial repository.
        self.build_tree([
            "hg/",
            "hg/f1",
            "hg/d1/",
            "hg/d1/d2/",
            "hg/d1/d2/f2",
        ])

        # Create Mercurial repository itself and fill it's history.
        hgrepo = mercurial.localrepo.localrepository(hgui(), "hg", create=True)
        hgrepo[None].add(["f1", "d1/d2/f2"])
        hgrepo.commit("Initial commit")
        hgrepo[None].remove(["d1/d2/f2"], unlink=True)
        hgrepo.commit("Remove file f2, so parent directories d2, d1 are empty")

        # Import history from Mercurial repository into Bazaar repository.
        bzrtree = self.make_branch_and_tree("bzr")
        hgdir = HgControlDirFormat().open(self.get_transport("hg"))
        bzrtree.pull(hgdir.open_branch())

        # As file f2 was deleted, directories d1 and d2 should not exists.
        self.assertPathDoesNotExist("bzr/d1")

        # Self-assurance check that history was really imported.
        self.assertPathExists("bzr/f1")

    def test_getting_existing_text_metadata(self):
        # Create Mercurial repository and Bazaar branch to import into.
        hgrepo = mercurial.localrepo.localrepository(hgui(), "hg", create=True)
        hgbranch = Branch.open("hg")
        bzrtree = self.make_branch_and_tree("bzr")

        # Create file 'f1' in Mercurial repository, commit it
        # and pull commited changeset to Bazaar branch.
        self.build_tree_contents([("hg/f1", "Initial content")])
        hgrepo[None].add(["f1"])
        hgrepo.commit("Initial commit")
        bzrtree.pull(hgbranch)

        # Change content of file 'f1' in Mercurial repository and commit
        # change.
        self.build_tree_contents([("hg/f1", "Changed content")])
        hgrepo.commit("Change content of file f1")

        # Pull commited changeset to Bazaar branch.
        #
        # Should not raise KeyError which looked like:
        #
        #  <...full traceback skipped...>
        #  File "/tmp/hg/fetch.py", line 208, in manifest_to_inventory_delta
        #      (fileid, ie.revision))
        # KeyError: ('hg:f1', 'hg-v1:97562cfbcf3b26e7eacf17ca9b6f742f98bd0719')
        bzrtree.pull(hgbranch)

        # Self-assurance check that changesets was really pulled in.
        self.assertFileEqual("Changed content", "bzr/f1")

    def test_incremental_fetching_of_repository_with_non_conflict_merge(self):
        # Create Mercurial configuration and override possible definition
        # of external interactive merge tools.
        ui = hgui()
        ui.setconfig("ui", "merge", "internal:merge")

        # Create Mercurial repository and Bazaar branch to import into.
        hgrepo = mercurial.localrepo.localrepository(ui, "hg", create=True)
        hgbranch = Branch.open("hg")
        bzrtree = self.make_branch_and_tree("bzr")

        # Create history graph in Mercurial repository
        # (history flows from left to right):
        #
        # A--B--D
        # |
        # \--C
        self.build_tree_contents([("hg/f1", "f1")])
        hgrepo[None].add(["f1"])
        hgrepo.commit("A (initial commit; first commit to main branch)")
        self.build_tree_contents([("hg/f2", "f2")])
        hgrepo[None].add(["f2"])
        hgrepo.commit("B (second commit to main branch)")
        hg.update(hgrepo, 0)
        self.build_tree_contents([("hg/f3", "f3")])
        hgrepo[None].add(["f3"])
        hgrepo.commit("C (first commit to secondary branch)")
        hg.update(hgrepo, 1)
        self.build_tree_contents([("hg/f4", "f4")])
        hgrepo[None].add(["f4"])
        hgrepo.commit("D (third commit to main branch)")

        # Pull commited changesets to Bazaar branch.
        bzrtree.pull(hgbranch)

        # Continue history graph in Mercurial repository
        # (history flows from up to down):
        #
        # a--b--d--E
        # |       |
        # \--c--F-/
        hg.update(hgrepo, 2)
        self.build_tree_contents([("hg/f5", "f5")])
        hgrepo[None].add(["f5"])
        hgrepo.commit("F (second commit to secondary branch)")
        hg.update(hgrepo, 3)
        hg.merge(hgrepo, 4)
        hgrepo.commit("E (commit merge of main branch with secondary branch)")

        # Pull commited changesets to Bazaar branch.
        bzrtree.pull(hgbranch)

        # Self-assurance check that all changesets was really pulled in.
        for i in range(1, 6):
            file_content = "f%d" % i
            file_path = "bzr/%s" % file_content
            self.assertFileEqual(file_content, file_path)

    def test_incremental_fetching_of_repository_with_conflict_merge(self):
        # Create Mercurial configuration and override possible definition
        # of external interactive merge tools.
        ui = hgui()
        ui.setconfig("ui", "merge", "internal:local")

        # Create Mercurial repository and Bazaar branch to import into.
        hgrepo = mercurial.localrepo.localrepository(ui, "hg", create=True)
        hgbranch = Branch.open("hg")
        bzrtree = self.make_branch_and_tree("bzr")

        # Create history graph with conflict in Mercurial repository
        # (history flows from left to right, conflict made at commits B and C):
        #
        # A--B--D
        # |
        # \--C
        self.build_tree_contents([("hg/f1", "f1")])
        hgrepo[None].add(["f1"])
        hgrepo.commit("A (initial commit; first commit to main branch)")
        self.build_tree_contents([("hg/conflict_file", "Main branch")])
        hgrepo[None].add(["conflict_file"])
        hgrepo.commit("B (second commit to main branch)")
        hg.update(hgrepo, 0)
        self.build_tree_contents([("hg/conflict_file", "Secondary branch")])
        hgrepo[None].add(["conflict_file"])
        hgrepo.commit("C (first commit to secondary branch)")
        hg.update(hgrepo, 1)
        self.build_tree_contents([("hg/f2", "f2")])
        hgrepo[None].add(["f2"])
        hgrepo.commit("D (third commit to main branch)")

        # Pull commited changesets to Bazaar branch.
        bzrtree.pull(hgbranch)

        # Continue history graph in Mercurial repository
        # (history flows from up to down):
        #
        # a--b--d--E
        # |       |
        # \--c--F-/
        hg.update(hgrepo, 2)
        self.build_tree_contents([("hg/f3", "f3")])
        hgrepo[None].add(["f3"])
        hgrepo.commit("F (second commit to secondary branch)")
        hg.update(hgrepo, 3)
        hg.merge(hgrepo, 4)
        self.build_tree_contents([("hg/conflict_file",
                                   "Main branch\nSecondary branch")])
        hgrepo.commit("E (commit merge of main branch with secondary branch)")

        # Pull commited changesets to Bazaar branch.
        bzrtree.pull(hgbranch)

        # Self-assurance check that all changesets was really pulled in.
        for i in range(1, 4):
            file_content = "f%d" % i
            file_path = "bzr/%s" % file_content
            self.assertFileEqual(file_content, file_path)

        self.assertFileEqual("Main branch\nSecondary branch",
            "bzr/conflict_file")
