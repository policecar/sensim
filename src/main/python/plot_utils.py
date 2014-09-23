#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'priska'

def plot_coo_matrix( m ):
	"""
	"""

	import matplotlib.pyplot as plt
	from scipy.sparse import coo_matrix
	
	if not isinstance(m, coo_matrix):
		m = coo_matrix(m)
	fig = plt.figure()
	ax = fig.add_subplot(111, axisbg='black')
	ax.plot(m.col, m.row, 's', color='white', ms=1)
	ax.set_xlim(0, m.shape[1])
	ax.set_ylim(0, m.shape[0])
	ax.set_aspect('equal')
	for spine in ax.spines.values():
		spine.set_visible(False)
	ax.invert_yaxis()
	ax.set_aspect('equal')
	ax.set_xticks([])
	ax.set_yticks([])
	return ax


def plot_confusion_matrix( conf_matrix ):
	"""
	"""

	# transform raw counts into percentages aka normalize confusion matrix
	cfmat = conf_matrix.astype( float )   # convert values to floats
	pcfmat = cfmat / cfmat.sum( axis=1 )[:,None]

	import matplotlib.pyplot as plt

	cmap = plt.cm.get_cmap( 'summer' ) # hsv, gist_ncar, summer
	plt.matshow( pcfmat, cmap=cmap )
	plt.title( 'Confusion matrix' )
	plt.colorbar()
	plt.ylabel( 'True label' )
	plt.xlabel( 'Predicted label' )
	plt.xticks( np.arange( 0, pcfmat.shape[1] ), [ 'coord', 'hyper', 'mero', 'rand' ])
	plt.yticks( np.arange( 0, pcfmat.shape[1] ), [ 'coord', 'hyper', 'mero', 'rand' ])
	plt.show()


def plot_histogram( l ):
	"""
	"""
	import matplotlib.pyplot as plt

	plt.hist( l )
	plt.title( "Title" )
	plt.xlabel( "X axis label" )
	plt.ylabel( "Y axis label" )
	plt.show()


def plot_cluster_sizes( filename ):
	"""
	"""
	import matplotlib.pyplot as plt
	import numpy as np
	from collections import Counter

	num_clusters = 0
	clusters = {}
	elements = []

	with open( filename, 'r' ) as f:
		for line in f:
			items = line.split('\t')
			clusterid = int( items[0] )
			num_pairs = int( items[1] )
			pairs = items[2][:-1]
			num_clusters += 1
			clusters[clusterid] = pairs
			elements.append( pairs )

	cluster_sizes = []
	for el in clusters.values():
		items = el.split(', ')
		sz = len( items )
		cluster_sizes.append( sz )

	counted_data = Counter( cluster_sizes )
	print( "Counter of cluster sizes: " )
	print( counted_data )

	labels = []
	values = []
	for key in sorted( counted_data.iterkeys() ):
		labels.append( key )
		values.append( counted_data[key] )

	indexes = np.arange( len( labels ))
	width = 1

	plt.yscale( 'symlog', basey=10 )

	plt.bar( indexes, values, width )
	plt.xticks( indexes + width * 0.5, labels )

	plt.xlabel( "Size of cluster" )
	plt.ylabel( "Number of clusters" )
	plt.title( "Distribution of cluster sizes" )

	plt.show()

	# show only clusters larger X
	# awk '$2 >= 15 ' clustering_10_50p_distlog.read > clustering_10_50p_distlog_largeCl.read

	return counted_data

