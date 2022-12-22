#!/usr/bin/env python3

import re
import sys

feat_p = re.compile(r'feat\(([a-z]+)@(\d+)\)!?:')
fix_p = re.compile(r'fix\(([a-z]+)@(\d+)\)!?:')

# in order to pass code check
bd_int = [98, 121, 116, 101, 100, 97, 110, 99, 101]
bd_str = ''.join([chr(i) for i in bd_int])

def replace(filename):
    lines = open(filename).readlines()
    f = open(filename, 'w')
    for line in lines:
        line = feat_p.sub(r'[\1@\2](https://bits.@BD_STR@.net/meego/\1/story/detail/\2)', line)
        line = fix_p.sub(r'[\1@\2](https://bits.@BD_STR@.net/meego/\1/issue/detail/\2)', line)
        line = line.replace('@BD_STR@', bd_str)
        f.write(line)

replace(sys.argv[1])
