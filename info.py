bzr_plugin_name = 'hg'

bzr_compatible_versions = [(1, x, 0) for x in [13, 14, 15, 16, 17, 18]] + \
                          [(2, x, 0) for x in [0, 1, 2]]

bzr_minimum_version = bzr_compatible_versions[0]

bzr_maximum_version = bzr_compatible_versions[-1]

bzr_plugin_version = (0, 2, 0, 'dev', 0)

bzr_control_formats = {"Mercurial": {'.hg/': None}}

hg_compatible_versions = ["1.3", "1.4", "1.5", "1.6"]
