#!/usr/bin/env python
import sys
import re

for line in sys.stdin:
    for word in line[:-1].split():
        print word, 1
