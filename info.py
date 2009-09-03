bzr_plugin_name = 'hg'

bzr_compatible_versions = [(1, 13, 0), (1, 14, 0), (1, 15, 0), (1, 16, 0), (1, 17, 0)]

bzr_minimum_version = bzr_compatible_versions[0]

bzr_maximum_version = bzr_compatible_versions[-1]

bzr_plugin_version = (0, 1, 0, 'dev', 0)

bzr_control_formats = {"Mercurial":{'.hg/': None}}

hg_compatible_versions = ["1.3.1"]
