from __future__ import division, print_function
__author__ = 'priska'

"""
Evaluate the clustering performed by Chinese Whispers[1] by computing the 
B-Cubed measure[1] for those entries that overlap with the BLESS data set[2] 
and hence have labels.

[1] http://wortschatz.informatik.uni-leipzig.de/~cbiemann/software/CW.html
[2] Bagga and Baldwin. "Entity-based cross-document coreferencing using the 
	vector space model." Proceedings of the 17th international conference on 
	Computational linguistics Vol1. ACL, 1998.
[3] https://sites.google.com/site/geometricalmodels/shared-evaluation

Usage:	
	
	python evaluate_cw_clustering.py 13
	python evaluate_cw_clustering.py 35
	
	where the second argument specifies the pruning used or whatever comes at 
	the end of the files clustering and id_pair. 
"""

from config import *

try:
	import IPython
	from IPython import embed
except ImportError:
	pass

import sys, os
import logging

import numpy as np
import pandas as pd

from collections import Counter

from sklearn import metrics
from sklearn.metrics import pairwise_distances
from scipy.sparse import lil_matrix

from coref_metrics import _prf, b_cubed, sets_to_mapping

# set up logger
logging.basicConfig( level=logging.INFO,
					 format='%(asctime)s %(levelname)s %(message)s' )
log = logging.getLogger(__name__)


def read_pair_dict( pair_dict_file ):
	"""
	Generates the mappings id_to_pair and pair_to_id as dictionaries.
	"""
	id_to_pair_map = dict()
	pair_to_id_map = dict()
	
	with open( pair_dict_file, 'r' ) as f:
	
		for line in f:
	
			items 	= line.split('\t')
			nid 	= int( items[0] )
			pair 	= items[1][:-1]		# remove '\n'
			id_to_pair_map[ nid ] = pair
			pair_to_id_map[ pair ] = nid

	return id_to_pair_map, pair_to_id_map


def compute_overlap_cw_bless( bless_file, cl_pair_dict ):
	"""
	Given a dictionary of clustered data points with the pair names as keys and
	the BLESS file ( as string ), computes the overlap between them and returns 
	it as { pair: label } and { label: set( pairs )} dictionaries.
	"""	
	overlap = dict()
	lbl_to_pairs_map = dict()

	with open( bless_file, 'r' ) as f:
	
		for line in f:
			
			items = line.split('\t')
			bless_pair = items[0]
			label = int( items[1][:-1] )	# remove '\n'

			# if BLESS pair is in clustered data, add it to overlap
			if cl_pair_dict.get( bless_pair ):
				
				overlap[ bless_pair ] = label
				
				if not lbl_to_pairs_map.get( label ):
					lbl_to_pairs_map[ label ] = set()
				lbl_to_pairs_map[ label ].add( bless_pair )

	return overlap, lbl_to_pairs_map


def retrieve_predicted_labels( clustering_file, overlap ):
	"""
	Filters clustered pairs for those that occur in BLESS as well, cf. overlap,
	and returns them as { clusterid: set( pairs )} dictionary.
	"""
	cid_to_pairs_map = dict()

	with open( clustering_file, 'r' ) as f:

		n = 0

		for line in f:
		
			items 		= line.split('\t')
			nid 		= int( items[0] )
			pair 		= items[1]
			clusterid 	= int( items[2] )
			
			# # simulate a single cluster
			# clusterid		= 1
			
			# # simulate one cluster per data points
			# n += 1
			# clusterid	= n 	
			
			# # simulate four random clusters
			# from random import randrange
			# clusterid 	= randrange(5) + 1 

			if pair in overlap:
				if not cid_to_pairs_map.get( clusterid ):
					cid_to_pairs_map[ clusterid ] = set()
				cid_to_pairs_map[ clusterid ].add( pair )

	return cid_to_pairs_map


def remove_small_clusters( cl2p_map, overlap, y_true, y_pred ):
	"""
	Remove small clusters before evaluation and adapt y_true, y_pred respectively.
	"""

	# filter away small clusters
	pairs = set()
	for clid, cl in cl2p_map.items():
		if len( cl ) > 0:
			# pairs = pairs.union( cl )
			pairs = pairs | cl

	# remove entries from y_true and y_pred that have been removed
	n = 0
	y_true_sm = y_true[:]
	y_pred_sm = y_pred[:]
	for o,y in overlap.items():
		idx = overlap.keys().index(o)
		if not o in pairs:
			y_true_sm.pop( idx - n )
			y_pred_sm.pop( idx - n )
			n += 1

	print( len( y_true ))
	print( len( y_true_sm ))

	return y_true_sm, y_pred_sm


if __name__ == "__main__":

	CLUST_DIR 		= os.path.join( BASE_DIR, 'clust' )

	bless_file 		= os.path.join( CLUST_DIR, 'bless_nouns_clust.tsv' )
	clustering_file = os.path.join( CLUST_DIR, 'clustering' )
	pair_dict_file 	= os.path.join( CLUST_DIR, 'id_pair' )
	sim_file		= os.path.join( CLUST_DIR, 'id_id_lmi' )

	clustering_file = "%s%s%s" % ( clustering_file, '_', sys.argv[1] )
	pair_dict_file  = "%s%s%s" % ( pair_dict_file, '_', sys.argv[1] )
	sim_file 		= "%s%s%s" % ( sim_file, '_', sys.argv[1] )

	# get pair_to_id map
	id2p_map, p2id_map = read_pair_dict( pair_dict_file )

	# compute overlap between BLESS and clustered data
	# additionally retrieve a { label: set( pairs )} dictionary
	overlap, l2p_map = compute_overlap_cw_bless( bless_file, p2id_map )
	
	# get gold standard aka true labels
	y_true = overlap.values()

	# get mapping from cluster id to pairs and the list of predicted cluster ids
	cl2p_map = retrieve_predicted_labels( clustering_file, overlap.keys())
	y_pred = sets_to_mapping( cl2p_map ).values()

	# # discard small clusters from evaluation because unwarranted improvement of ( homogeneity ) score
	# y_true, y_pred = remove_small_clusters( cl2p_map, overlap, y_true, y_pred )

	log.info( "Evaluate clustering" )
	log.info( "Homogeneity: %0.3f" % metrics.homogeneity_score( y_true, y_pred ))
	log.info( "Completeness: %0.3f" % metrics.completeness_score( y_true, y_pred ))
	log.info( "V-measure: %0.3f" % metrics.v_measure_score( y_true, y_pred ))
	log.info( "Adjusted Rand Index: %0.3f" % metrics.adjusted_rand_score( y_true, y_pred ))
	log.info( "Adjusted Mutual Information: %0.3f" % metrics.adjusted_mutual_info_score( y_true, y_pred ))
	print( 'Metric', 'P', 'R', 'F1', sep='\t' )
	print( 'B-Cubed', *( '{:0.2f}'.format( 100 * x ) for x in _prf( *b_cubed( l2p_map, cl2p_map ))), sep='\t' )
	# print( b_cubed( l2p_map, cl2p_map ))

	# print some statistics of the data
	# get number of clusters larger than x
	x = 5
	all_cl2p_map = dict()
	with open( clustering_file+'.read', 'r' ) as cf:
		for line in cf:
			items = line.split('\t')
			clid = items[0]
			pairs = items[2][:-1].split(', ')
			all_cl2p_map[clid] = pairs
	print()
	print( "Number of clusters: %d" % ( len( all_cl2p_map )))
	print( "Number of clusters with >= %d items: %d" % ( x, len( [ k for k,v in all_cl2p_map.items() if len(v) >= x ])))
	print( "Number of clusters with >= %d items: %d" % ( x*2, len( [ k for k,v in all_cl2p_map.items() if len(v) >= x*2 ])))
	print( "Number of clusters with >= %d items: %d" % ( x*3, len( [ k for k,v in all_cl2p_map.items() if len(v) >= x*3 ])))
	print( "Number of clusters with >= %d items: %d" % ( x*4, len( [ k for k,v in all_cl2p_map.items() if len(v) >= x*4 ])))
	
	# get number of eval clusters larger than x
	print()
	print( "Number of eval clusters: %d" % ( len( cl2p_map.keys() )))
	print( "Number of eval clusters with >= %d items: %d" % ( x, len([ k for k,v in cl2p_map.items() if len(v) >= x ])))
	print( "Number of eval clusters with >= %d items: %d" % ( x*2, len([ k for k,v in cl2p_map.items() if len(v) >= x*2 ])))
	print( "Number of eval clusters with >= %d items: %d" % ( x*3, len([ k for k,v in cl2p_map.items() if len(v) >= x*3 ])))
	print( "Number of eval clusters with >= %d items: %d" % ( x*4, len([ k for k,v in cl2p_map.items() if len(v) >= x*4 ])))

	# get pairs in overlap by <relation>
	coords = [ k for k,v in overlap.items() if v == 0 ]
	hypers = [ k for k,v in overlap.items() if v == 1 ]
	meros  = [ k for k,v in overlap.items() if v == 2 ]
	rands  = [ k for k,v in overlap.items() if v == 3 ]

	# get number of <relation> pairs in overlap
	print()
	print( "Number of co-hyponyms in eval clusters: %d" % len( coords ))
	print( "Number of hypernyms in eval clusters: %d" % len( hypers ))
	print( "Number of meronyms in eval clusters: %d" % len( meros ))
	print( "Number of random pairs in eval clusters: %d" % len( rands ))

	# get cluster IDs of <relation> pairs in eval clusters larger than x
	c, h, m = [], [], []
	for clid, pairs in cl2p_map.items():
		# if cluster larger than x
		if len( pairs ) > x:
			# for each pair in current cluster
			for p in pairs:
				# if pair is <relation>, add cluster ID to <relation> list
				# which gives me the number of <relation> items in clusters larger 
				# than x and the number of clusters that contain <relation> items
				if p in coords:
					c.append( clid )
				elif p in hypers:
					h.append( clid )
				elif p in meros:
					m.append( clid )

	# number of <relation> pairs in eval clusters
	print()
	print( "Number of co-hyponyms in eval clusters with >= %d items: %d" % ( x, len( c )))
	print( "Number of hypernyms in eval clusters with >= %d items: %d" % ( x, len( h )))
	print( "Number of meronyms in eval clusters with >= %d items: %d" % ( x, len( m )))

	print()
	print( "Number of eval clusters with >= %d items containing co-hyponyms: %d" % ( x, len( set( c ))))
	print( "Number of eval clusters with >= %d items containing hypernyms: %d" % ( x, len( set( h ))))
	print( "Number of eval clusters with >= %d items containing meronyms: %d" % ( x, len( set( m ))))
	print()
	print( Counter( c ))
	print( Counter( h ))
	print( Counter( m ))

	# embed()

