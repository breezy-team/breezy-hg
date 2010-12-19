from bzrlib.plugins.hg import HgControlDirFormat
from bzrlib.plugins.hg.ui import ui as hgui
from bzrlib.tests import TestCaseWithTransport
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
        bzrtree = self.make_branch_and_tree('bzr')
        hgdir = HgControlDirFormat().open(self.get_transport('hg'))
        bzrtree.pull(hgdir.open_branch())

        # As file f2 was deleted, directories d1 and d2 should not exists.
        self.failIfExists('bzr/d1')

        # Self-assurance check that history was really imported.
        self.failUnlessExists('bzr/f1')
