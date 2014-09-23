#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'priska'

from config import *
import os


def prepare_bless_data( inputfile, outputfile ):
	"""
	Select noun-noun pairs from the BLESS evaluation data, strip their POS 
	tags, reformat the data to 'noun1\tnoun2\tlabel', write them to file.
	"""

	with open( inputfile, 'r' ) as obf:
		with open( outputfile, 'w' ) as bf:
			
			for line in obf:

				items = line[:-1].split( '\t' )
				noun1 = items[0]
				noun2 = items[3]
				label = items[2]

				if noun1[-2:] == '-n' and noun2[-2:] == '-n':
					bf.write( '%s\t%s\t%s\n' % ( noun1[:-2], noun2[:-2], label ))


if __name__ == "__main__":

	BLESS_DIR = os.path.join( BASE_DIR, 'bless' ) 

	incoming_bless_file = os.path.join( BLESS_DIR, 'bless-gems', 'BLESS.txt' )
	new_bless_file		= os.path.join( BLESS_DIR, 'bless_nouns.tsv' )

	prepare_bless_data( incoming_bless_file, new_bless_file )

