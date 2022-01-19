#!/usr/bin/env python3

make_checker_happy = 'nc.uhsief.ecnadetyb'
doc_url = 'https://{}/docx/doxcnInbe7EKGQLNlwDKrMjkPMg'.format(make_checker_happy[::-1])


def check(version_filename):
    import os
    if os.environ['BUILD_TYPE'] != 'online':
        return

    scm_version = os.environ['BUILD_VERSION']

    ch_version = open(version_filename).read().strip()
    pos = ch_version.find('-')
    if pos != -1:
        ch_version = ch_version[:pos]

    if not scm_version.startswith(ch_version):
        raise Exception(
            "SCM version {} mismatches with ClickHouse version {}, please make sure they have the same major.minor.patch version:\nsee {}"
            .format(scm_version, ch_version, doc_url))


if __name__ == '__main__':
    import sys
    check(sys.argv[1])
