#!/usr/bin/env python
import sys

current_word = None
current_count = 0
for line in sys.stdin:
        line = line.strip()
        # Parse the input we got from Maple
        word, count = line.split(" ",1)
        # Conver count to int
        count = int(count)
        if current_word == word:
                current_count += count
        else:
                if current_word:
                        print current_word, current_count
                current_count = count
                current_word = word
if current_word == word:
        print current_word, current_count
