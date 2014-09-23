#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'priska'

"""
Prepare sensim data for clustering. 

Depending on the clustering algorithm you want to use, choose one of the matrix 
format options and MatrixMarket header if needed. Possible options so far are 
--matrix=cw for Chinese Whispers [1], 
--matrix=sim for a similarity matrix compatible with scikit-learn [2]
--matrix=sim --mm=True for a similarity matrix compatible with fastcluster's hclust.R [3]
--matrix=feat for a feature matrix compatible with scikit-learn [2]

Required file(s): ctx_lmi.gz or sensim_lmi_l200.gz

where ctx_lmi.gz can be produced with the sensim pipeline[4] and sensim_lmi_l200.gz
is the output of the JoBimText pipeline[5].

[1] http://wortschatz.informatik.uni-leipzig.de/~cbiemann/software/CW.html
[2] http://scikit-learn.org/stable/modules/clustering.html#clustering
[3] http://cran.r-project.org/web/packages/fastcluster/fastcluster.pdf
[4] https://github.com/policecar/sensim
[5] http://maggie.lt.informatik.tu-darmstadt.de/jobimtext/

Usage:	
		python clustering_utils.py --prune=10
"""

from config import *

try:
	import IPython
	from IPython import embed
except ImportError:
	pass

import re
import os

import gzip
from optparse import OptionParser
import logging

# set up logger
logging.basicConfig( level=logging.INFO,
					 format='%(asctime)s %(levelname)s %(message)s' )
log = logging.getLogger(__name__)

def make_pair_dict( sim_file, pair_dict_file, prune_factor ):
	"""
	Make a dictionary that maps a unique numeric id to each pair.

	Input  format: l_pair \t r_pair \t similarity
	Output format: id \t pair
	"""

	log.info( "Generating id2pair dict" )

	d_pairs 	= {}
	nid 		= 0
	num_lines 	= 0 
	num_entries = 0

	with gzip.open( sim_file, 'r' ) as f:

		current_left_pair = None
		l = 0

		for line in f:
			
			num_lines += 1
			if num_lines % 1000000 == 0:
				log.info( "%d lines later" % num_lines )

			# preparatory
			items = line.split('\t')
			l_pair = items[0]
			r_pair = items[1]
			sim = float( items[2][:-1] )

			# prune to use only X most similar pairs
			if l_pair == current_left_pair:
				l += 1
			else:
				l = 0
				current_left_pair = l_pair

			# filter pairs with special characters
			m_l = re.search( r"[^a-zA-Z:@]", l_pair )
			m_r = re.search( r"[^a-zA-Z:@]", r_pair )

			# prune entries with similarity < prune factor or either of the above
			# re l, keep only the 50 most similar pairs per pair
			if sim < prune_factor or m_l or m_r or l > 50 or ( l_pair == r_pair ):
				continue
			else:
				num_entries += 1

			# add left pair to dict
			if not d_pairs.get( l_pair ):
				nid += 1 	# start counting at 1, not 0
				d_pairs[ l_pair ] = nid

			# add right pair to dict
			if not d_pairs.get( r_pair ):
				nid += 1
				d_pairs[ r_pair ] = nid

	# sort pairs by numeric id and write them to dict file
	with open( pair_dict_file, 'w' ) as outf:
		for k in sorted( d_pairs, key=d_pairs.get ):
			outf.write( "%d\t%s\n" % ( d_pairs[k], k ))

	return d_pairs, num_entries


def write_sims_for_chinese_whispers( sim_file, cw_sim_file, d_pairs ):
	"""
	Produce a version of sim_file with numeric ids instead of pair names and
	including symmetric entries. E.g., if the entry 'pair1 pair2 sim' exists,
	make an additional entry 'pair2 pair1 sim'.

	Note: similarity values will be output as integers, cf. CW requirements.

	Input  format: l_pair \t r_pair \t similarity
	Output format: nid1 \t nid2 \t similarity
	"""

	log.info( "Writing similarities for Chinese Whispers" )

	sim_matrix = dict()

	with gzip.open( sim_file, 'r' ) as f:

		for line in f:

			# preparatory
			items = line.split('\t')
			l_pair = items[0]
			r_pair = items[1]
			# convert similarity to int to match ChineseWhispers' requirements
			sim = int( float( items[2][:-1] ))	# remove '\n'

			# conditional because we potentially pruned in when making d_pairs
			if d_pairs.get( l_pair ) and d_pairs.get( r_pair ):
				# look up ids
				nid1 = d_pairs[ l_pair ]
				nid2 = d_pairs[ r_pair ]

				if sim_matrix.get( nid1 ):
					sim_matrix[ nid1 ][ nid2 ] = sim
				else:
					sim_matrix[ nid1 ] = {}
					sim_matrix[ nid1 ][ nid2 ] = sim
				
				if sim_matrix.get( nid2 ):
					sim_matrix[ nid2 ][ nid1 ] = sim
				else:
					sim_matrix[ nid2 ] = {}
					sim_matrix[ nid2 ][ nid1 ] = sim
	
	# write all entries to file, in numeric order
	with open( cw_sim_file, 'w' ) as outf:

		# sort primary dict by key
		for nid1 in sorted( sim_matrix ):
			# sort secondary dict by key
			for nid2 in sorted( sim_matrix[ nid1 ] ):
				# # skip entries where both noun pairs are the same (?)
				# # consider skipping them much earlier, ie. when building d_pairs
				# if nid1 == nid2:
				# 	continue
				# else:
				# 	sim = value[ nid2 ]
				# 	outf.write( "%d\t%d\t%d\n" % ( nid1, nid2, sim ))
				sim = sim_matrix[ nid1 ][ nid2 ]
				outf.write( "%d\t%d\t%d\n" % ( nid1, nid2, sim ))


def write_numeric_pair_pattern_entries_to_file( ctx_lmi_file, pair_pattern_file, \
	d_patterns, d_pairs, prune_factor ):

	with gzip.open( ctx_lmi_file, 'r' ) as cf:

		with open( pair_pattern_file, 'w' ) as outf:

			for line in cf:

				items 		= line.split('\t')
				pair 		= items[0]
				pattern 	= items[1]
				lmi 		= float( items[2] )

				# need this condition because we pruned above
				if d_patterns.get( pattern ) and d_pairs.get( pair ) and lmi >= prune_factor:
					pairid 		= d_pairs[ pair ]
					patternid 	= d_patterns[ pattern ]

					outf.write( "%d\t%d\t%f\n" % ( pairid, patternid, lmi ))


if __name__ == "__main__":

	CLUST_DIR = os.path.join( BASE_DIR, 'clust' )

	# parse ( named ) commandline options
	op = OptionParser()
	op.add_option("--prune",	# an integer
				  action="store", dest="prune_factor", default=1,
				  help="Prune factor to use; see code.")
	( opts, args ) = op.parse_args()
	
	prune_factor = int( opts.prune_factor )

	sim_file 		= os.path.join( CLUST_DIR, 'sensim_lmi_l200.gz' )
	pair_dict_file 	= os.path.join( CLUST_DIR, "%s_%d" % ( 'id_pair', prune_factor ))
	cw_sim_file 	= os.path.join( CLUST_DIR, "%s_%d" % ( 'id_id_lmi', prune_factor ))

	d_pairs = make_pair_dict( sim_file, pair_dict_file, prune_factor )[0]
	write_sims_for_chinese_whispers( sim_file, cw_sim_file, d_pairs )

