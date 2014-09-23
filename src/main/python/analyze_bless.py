#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'priska'

"""
Compute basic frequency counts on the BLESS evaluation set:
distributions of semantic relations /labels, left nouns, right nouns.
"""

from config import *

from collections import Counter
import os

BLESS_DIR  = os.path.join( BASE_DIR, 'bless' )
bless_file = os.path.join( BLESS_DIR, 'bless_nouns.tsv' )


with open( bless_file, 'r' ) as f:
	lines = f.read()
	content = lines.split('\n')[:-1]

w1rds  = []
w2rds  = []
labels = []

d_w1   = {}
d_w2   = {}

for line in content:
	
	items = line.split('\t')
	
	w1 = items[0]
	w2 = items[1]
	lbl = items[2]
	
	w1rds.append( w1 )
	w2rds.append( w2 )
	labels.append( lbl )

	if d_w1.get( w1 ):
		if d_w1[w1].get( lbl ):
			d_w1[w1][lbl] += 1
		else:
			d_w1[w1][lbl] = 1
	else:
		d_w1[w1] = {}
		d_w1[w1][lbl] = 1

	if d_w2.get( w2 ):
		if d_w2[w2].get( lbl ):
			d_w2[w2][lbl] += 1
		else:
			d_w2[w2][lbl] = 1
	else:
		d_w2[w2] = {}
		d_w2[w2][lbl] = 1


print( "Total number of items: %d" % len(labels ))
print( "Label counts: %s\n" % Counter( labels ).items())

print( "Total number of different left nouns: %d" % len( Counter( w1rds )))
print( "Occurrences of left nouns: %s\n" % Counter( w1rds ).items())

print( "Total number of different right nouns: %d" % len( Counter( w2rds )))
# print( "Occurrences of right nouns: %s" % Counter( w2rds ).items())

