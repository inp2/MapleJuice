#!/usr/bin/env python
import sys

for line in sys.stdin:
        if line:
                link = line.split()
                print link[0], link[1]
