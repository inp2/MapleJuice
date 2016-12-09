#!/usr/bin/env python
import sys

flag = True
for line in sys.stdin:
        if flag:
                print line[:-1].split(' ')[0],
                flag = False
        print line[:-1].split(' ')[1],

