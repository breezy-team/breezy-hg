bzr_plugin_name = 'hg'

bzr_compatible_versions = [(2, x, 0) for x in [3, 4]]

bzr_minimum_version = bzr_compatible_versions[0]

bzr_maximum_version = bzr_compatible_versions[-1]

bzr_plugin_version = (0, 2, 0, 'dev', 0)

bzr_control_formats = {"Mercurial": {'.hg/': None}}

hg_compatible_versions = [(1, 6), (1, 7), (1, 8), (1, 9), (2, 0)]

hg_compatible_version_strings = ["%d.%d" % x for x in hg_compatible_versions]
