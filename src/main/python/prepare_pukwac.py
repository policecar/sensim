#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'priska'

"""
	Prepare the PukWac corpus for processing with Pig and Hadoop. Therefore,
	convert the input format to one sentence per line, tab-separated and with
	the special separator ':::::' for indicating the next token ( PukWac line ).

	[1] http://wacky.sslmit.unibo.it/doku.php?id=start
"""

from config import *

import os
import gzip

pukwac_dir 		= os.path.join( wacky_dir, "pukwac/" )
pukwac4pig_dir 	= os.path.join( wacky_dir, "pukwac_for_pig/" )

for ffile in os.listdir( pukwac_dir ):

	if ffile.endswith( '.gz' ):

		pfile = os.path.join( pukwac_dir, ffile )
		with gzip.open( pfile, 'r' ) as pf:

			p4pfile = os.path.join( pukwac4pig_dir, ffile )[:-3]
			with open( p4pfile, 'w' ) as ppf:

				sentence = ''
				ctr = 0

				for line in pf:

					# line = line.decode( 'latin_1' )
					if line.startswith( b'<text id="ukwac:' ) or line.startswith( b'<s>' ):
						ctr += 1
						if ctr % 100000 == 0:
							print( 'prepared %d sentences' % ctr )
						continue
					elif line == b'</s>\n':
						ppf.write( '%s\n' % sentence[:-7])
						sentence = ''
					else:
						sentence += b'%s\t:::::\t' % line[:-1]

