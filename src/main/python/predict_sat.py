#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'priska'

"""
	Predict the correct answer to the SAT analogy questions /evaluation data[1] 
	based on features from one of various models of distributional similarity.

	Current models to choose from are:
	(1) context features from a distributional thesaurus[2]
	(2) subtrees containing both nouns extracted from dependency parses[3]

	[1] http://aclweb.org/aclwiki/index.php?title=SAT_Analogy_Questions_%28State_of_the_art%29
	[2] s. Biemann and Riedl 2013
	[3] work from my master's thesis

	Usage:
	python predict_sat.py
"""

from config import *
import re, os, csv, logging
from collections import Counter

from scipy.spatial.distance import cosine, jaccard
from sklearn.feature_extraction import DictVectorizer

try:
	import IPython
	from IPython import embed
except ImportError:
	pass

# set up logger
logging.basicConfig( level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s' )
log = logging.getLogger( __name__ )


def clean_SAT_data( satfile, pairfile, testfile ):
	"""
		Read SAT analogy questions as received from Peter Turney and select
		those questions that contain noun-noun pairs in accordance with the 
		distributional model developed in \cite{herger2014}. 
		
		In addition to the incoming questions, generate an inverse version of
		each; cf. if all pairs are inversed the relation should still hold.

		Write these pairs to disk twice: as a list of multiple-choice questions 
		( with answers ) and as a list of pairs.
	"""

	with open( satfile, 'r' ) as satf:

		# remove boiler-plate
		lines = list()
		for line in satf:
			if not re.match( r'(\r\n|#|ML:|KS type|190)', line ):
				lines.append( line.rstrip( '\r\n' ))

		sat = dict()
		pairs = set()

		num_duplicate_keys = 0
		idx = 0

		it = iter( lines )
		for line in it:

			# select only noun-noun questions, ie. those tagged n:n
			# FYI the possible answers can be pairs with POS tags other than n:n
			if line.endswith( 'n:n' ):

				# use index to uniquify nouns
				idx += 1

				# extract question pair ( and its inverse )
				q_tmp = line.split( ' ' )[0:2]	# drop POS tag
				question = '(%s,%s)' % ( q_tmp[0], q_tmp[1] )
				question_inv = '(%s,%s)' % ( q_tmp[1], q_tmp[0] )

				# extract answer pairs
				answers = list()
				t_answers = list()
				answers_inv = list()
				t_answers_inv = list()
				
				for i in range( 5 ):
				
					tmp = it.next().split( ' ' )[0:2]
					# as incoming
					answers.append( '%s\t%s' % ( tmp[0], tmp[1] ))
					t_answers.append( '(%s,%s)' % ( tmp[0], tmp[1] ))
					# inverse
					answers_inv.append( '%s\t%s' % ( tmp[1], tmp[0] ))				
					t_answers_inv.append( '(%s,%s)' % ( tmp[1], tmp[0] ))				

					# Note: the two formats are kept for backwards compatibility,
					# ie. to not break get_features_from_db(..) pairs.tsv should
					# contain tab-separated nouns
				
				# convert correct answer from letter ( a, b, c, d, e ) to integer ( 0 - 4 )
				solution = ord( it.next() ) - 97
				key = "%d__%d" % ( idx, solution )

				# construct dict of multiple-choice questions and their inverses
				sat[ key ] = { question: t_answers, question_inv: t_answers_inv }
				
				# make list of ( unique ) noun pairs 
				pairs.add( '%s\t%s' % ( q_tmp[0], q_tmp[1] ))
				pairs.add( '%s\t%s' % ( q_tmp[1], q_tmp[0] ))
				for ans in answers:
					pairs.add( ans )
				for ans in answers_inv:
					pairs.add( ans )

			else:
				# dismiss next 6 items as well
				for i in range( 6 ):
					it.next()

		# save selected multiple-choice questions to file
		with open( testfile, 'w' ) as tf:
			wr = csv.writer( tf, delimiter='\t' )
			for k,v in sat.items():
				wr.writerow( [ k, v ] )
		
		# save pairs to file
		with open( pairfile, 'w' ) as pf:
			wr = csv.writer( pf, delimiter='\t' )
			for p in pairs:
				l = p.split( '\t' )	# not nice but so for now
				wr.writerow( l )


def get_features_from_db( pairfile, featurefile ):
	"""
		Fetch features from a MySQL database.
	"""

	import MySQLdb

	db = MySQLdb.connect( host=host, port=port, user=user, passwd=passwd, db=db_name )
	cursor = db.cursor()

	# read pairs from file
	with open( pairfile, 'r' ) as pf:
		content = pf.read()
		lines = content.split( '\r\n' )[:-1]
		pairs = []
		for line in lines:
			pairs.append( line.split( '\t' ))

	# retrieve pairs and their features from mysql db
	with open( featurefile, 'w' ) as featf:

		cntr = 0

		for pair in pairs:
			
			cntr += 1
			if cntr % 10 == 0:
				print( '(%s out of at most 192 * 6 * 2 aka 2304)' % ( cntr ))

			# adapt nouns to their form in the db
			noun1 = pair[0] + '#NN'
			noun2 = pair[1] + '#NN'

			# search for co-occurrences of these noun pairs
			cursor.execute( "SELECT * from LMI_s_0_t_0_p_1000_l_200_simsort \
				where word1 = %s and word2 = %s limit 20;", ( noun1, noun2 ))
			res = cursor.fetchall()
			if len( res ):
				# if found, get their features
				cursor.execute( "SELECT * from LMI_s_0_t_0 where word = %s and \
					feature like %s", ( noun1, noun2 + '%' ))
				feats = cursor.fetchall()
				print( res )

				# write the retrieved results to file
				printpair = "(%s,%s)" % ( pair[0], pair[1] )
				featf.write( "%s\t%s\t%s\n" % ( printpair, str( res[0][0:3] ), 
					str( feats )))
			else:
				printpair = "(%s,%s)" % ( pair[0], pair[1] )
				featf.write( "%s\n" % ( printpair, ))
			
			# repeat for inverted noun pairs
			cursor.execute( "SELECT * from LMI_s_0_t_0_p_1000_l_200_simsort \
				where word1 = %s and word2 = %s limit 20;", ( noun2, noun1 ))
			res = cursor.fetchall()
			if len( res ):
				# if found, get their features
				cursor.execute( "SELECT * from LMI_s_0_t_0 where word = %s and \
					feature like %s", ( noun2, noun1 + '%' ))
				feats = cursor.fetchall()
				print( res )

				# write the retrieved results to file
				printpair = "(%s,%s)" % ( pair[1], pair[0] )
				featf.write( "%s\t%s\t%s\n" % ( printpair, str( res[0][0:3] ), 
					str( feats )))
			else:
				printpair = "(%s,%s)" % ( pair[1], pair[0] )
				featf.write( "%s\n" % ( printpair ))


def get_features_from_vectors( pairfile, vectorfile, featurefile ):

	# from nltk.stem.wordnet import WordNetLemmatizer
	# lm = WordNetLemmatizer()
	from textblob import Word

	# read pairs from file
	# Note: the SAT data is small enough to be read into memory
	with open( pairfile, 'r' ) as pf:
		
		content = pf.read()
		lines = content.split( '\r\n' )[:-1]
		sat_pairs = list()
		sat_pair_dict = dict()
		
		for line in lines:
		
			# rewrite SAT pairs from 'tree \t beet' to '(tree,beet)' and 
			# add the incoming version and its lemmatized version to the list
			pair = line.split( '\t' )
			string_pair = '(%s,%s)' % ( pair[0], pair[1] )
			lemma_pair  = '(%s,%s)' % ( Word( pair[0] ).lemmatize(), Word( pair[1] ).lemmatize() )
			sat_pairs.append( string_pair )
			sat_pairs.append( lemma_pair )
			# additionally write SAT pairs to dict to include original version
			sat_pair_dict[ lemma_pair ] = string_pair
			sat_pair_dict[ string_pair ] = string_pair

	log.info( 'number of incoming pairs: %d' % ( len( sat_pairs ) / 2 ))

	num_pairs_found = 0
	cntr = 0

	# for every noun pair vector, if it's in the SAT data, use its features
	with open( vectorfile, 'r' ) as vf:

		# open another file to write SAT pairs with features to
		with open( featurefile, 'w' ) as featf:

			for line in vf:
				
				if len( line ):
					# incoming format: 8582724 \t duck::@::tiger \t {(627,53.0),(501,72.3)}
					# outgoing format: (duck,tiger) \t (duck,tiger) \t {(627,53.0),(501,72.3)}
					# the first column is the pair as-is, the second its lemma version
					datapoint = line.split( '\t' )
					noun_pair = datapoint[1]
					pair_feats = datapoint[2][:-1] 	# remove trailing \n
					# reformat noun pair
					tmp = noun_pair.split( '::@::' )
					if len( tmp ) > 1:
						noun_pair = '(%s,%s)' % ( tmp[0], tmp[1] )

				# provide some feedback on how many vectors have been parsed
				cntr += 1
				if cntr % 1000000 == 0:
					log.info( "Number of vector entries parsed: %d" % cntr )

				if noun_pair in sat_pairs:
					orig_pair = sat_pair_dict[ noun_pair ]
					featf.write( "%s\t%s\t%s\n" % ( orig_pair, noun_pair, pair_feats, ))
					num_pairs_found += 1

					# consider using inverted pairs in case of very low number of hits

	log.info( "number of noun pairs found: %d" % num_pairs_found )


def preprocess_DB_features( featurefile ):

	# read features into dictionary ( from mysql db )
	with open( featurefile, 'r' ) as ff:

		content = ff.read()
		lines = content.split( '\n' )[:-1] # because the last line is empty

		feats = {}
		for line in lines:
			columns = line.split( '\t' )
			if len( columns ) == 3:
				feats[ columns[0] ] = columns[2]
			else:
				feats[ columns[0] ] = "()"

		for pair,features in feats.items():
			features = eval( features )	# Beware of alligators!
			new_features = {}
			for item in features:
				item = item[1:]	# throw away first entry of tuple
				match = re.match( r'.*#NN(.*)', item[0] )
				if match:
					# new_features.append(( match.group(1), item[1] ))
					new_features[ match.group(1) ] = item[1]
			feats[ pair ] = new_features

	return feats


def preprocess_VEC_features( featurefile ):

	# read features into dictionary
	with open( featurefile, 'r' ) as ff:

		# Note: since the number of features here does not exceed the size of SAT,
		# this can be read into memory for easier handling

		feats = {}
		for line in ff:
			if len( line ):
				columns = line.split( '\t' )
				if len( columns ) == 3:
					feats[ columns[0] ] = columns[2][:-1] 	# remove trailing \n
				else:
					feats[ columns[0] ] = "()"

		# reformat the features to be a list of feature tuples ( feature, number )
		for pair, features in feats.items():

			features = eval( features )	# Beware of crocodiles!
			new_features = {}
			for item in list( features ):
				new_features[ item[0] ] = item[1]
			feats[ pair ] = new_features

	return feats


def get_similarity( question, qfeats, afeats, sim_measure ):

	similarity = 0.0
	
	if sim_measure == 'overlap':

		shared_features = set( qfeats ).intersection( set( afeats ))
		similarity = float( len( shared_features ))

	elif sim_measure == 'dotproduct':

		# select feature names, compute intersection, then compute dot product
		shared_features = set( qfeats.keys()).intersection( set( afeats.keys() ))
		print( 'shared features for %s\t\t%s' % ( question, shared_features ))
		similarity = 0.0
		for feat in shared_features:
			similarity += qfeats[ feat ] * afeats[ feat ]

	elif sim_measure == 'cosine':
		# 
		dvec = DictVectorizer( sparse=False )
		m_qa = dvec.fit_transform( [ qfeats, afeats ] )
		similarity = 1.0 - cosine( m_qa[0,:], m_qa[1,:] )

	return similarity


def compute_similarity_between_datapoints( testfile, featurefile, predictionfile, model, sim_measure ):

	# read multiple-choice questions into dictionary
	with open( testfile, 'r' ) as tf:

		content = tf.read()
		lines = content.split( '\r\n' )[:-1] # because the last line is empty

		mcq = dict()
		for line in lines:
			line = line.split( '\t' )
			mcq[ line[0] ] = line[1]

	log.info( "number of incoming questions: %d" % len( mcq ))

	# read features from file -- exact format varies with source, ie. DB or vectors
	if model == 'DB':
		feats = preprocess_DB_features( featurefile )
	elif model == 'VEC':
		feats = preprocess_VEC_features( featurefile )

	# at this point feats should be a dictionary with a noun pair of the form
	# '(years,almanac)' as key and a list with features of the form 
	# ('#prep_in', 22.732648620424) as value

	# iterate through every question, compute the similarity between question 
	# and answer options, and write the chosen answer to file according to the 
	# following format: question_pair \t answer_pair \t y_pred \t y_true

	# heuristic to consider: if only a single answer was found, pick that one,
	# even if there's no similarity with the question's features
	
	# find the answer most similar to the question and write it to predictions.tsv
	with open( predictionfile, 'w' ) as rf:

		num_complete_questions = 0
		num_incomplete_questions = 0
		answers_found = []

		# for every question
		for key, value in mcq.items():
		# for question_uniq, answers_uniq in mcq.items():

			elems = eval( value )							# Beware of alligators!
			correct_answer = int( key.split( '__' )[1] )	# it's ugly -- i'm sorry

			# if neither the question nor its inverse have been seen in the data,
			# skip the whole multiple-choice question
			if not ( feats.get( elems.keys()[0]) or feats.get( elems.keys()[1] )):
				rf.write( '%s\t%s\t%d\t%d\n' % ( elems.keys()[0], '__', -1, correct_answer ))
				answers_found.append(-1)
				continue

			# check which of the versions has more seen answers
			num_answers_found = 0
			num_inv_answers_found = 0

			for ans in elems[ elems.keys()[0] ]:
				if feats.get( ans ):
					num_answers_found += 1

			for ans in elems[ elems.keys()[1] ]:
				if feats.get( ans ):
					num_inv_answers_found += 1

			max_answers_found = max( num_answers_found, num_inv_answers_found )
			answers_found.append( max_answers_found )
			# print( max_answers_found )

			# set a threshold of number of answers that need to have been seen
			# to make a prediction at all
			if max_answers_found < 1:
				rf.write( '%s\t%s\t%d\t%d\n' % ( elems.keys()[0], '__', -1, correct_answer ))
				continue
			
			# go down the original branch
			elif num_answers_found >= num_inv_answers_found:
				question = elems.keys()[0]
				answers = elems[ elems.keys()[0] ]

			# else go down the inverted branch
			else:
				question = elems.keys()[1]
				answers = elems[ elems.keys()[1] ]

			qfeats = feats.get( question )

			max_sim = 0.0
			best_guess = None

			for idx, answer in enumerate( answers ):

				# compute similarity between the question's features and each 
				# of the possible answers' feature sets
				try: 
					afeats = feats.get( answer )
					similarity = get_similarity( question, qfeats, afeats, sim_measure )
				except:
					continue

				if similarity > max_sim:
					max_sim = similarity
					best_guess = answers[ idx ]

			if max_answers_found == 5:
				num_complete_questions += 1
			elif best_guess:
				num_incomplete_questions += 1

			# cases in which one of the if or elif conditions above was met 
			# but no feature overlap was found, end up in the except part,
			# as well as all cases for which none of the conditions above hold
			try:
				predicted_label = answers.index( best_guess )

			except:

				# strategy: remain silent if unsure
				best_guess = '__'
				predicted_label = -1

				# # strategy: predict random choice as fallback
				# from random import randint
				# predicted_label = randint( 0, 4 )
				# best_guess = answers_uniq[ predicted_label ]

				pass

			rf.write( '%s\t%s\t%d\t%d\n' % \
				( elems.keys()[0], best_guess, predicted_label, correct_answer ))

	log.info( "Counter of number of answers found: %s" % Counter( answers_found ))
	log.info( "complete questions found in data: %d" % num_complete_questions )
	log.info( "questions answered with incomplete data: %d" % num_incomplete_questions )


def compute_loss( predictionsfile ):

	with open( predictionsfile, 'r' ) as pf:

		# format: question pair \t answer pair \t prediction \t correct answer
		content = pf.read()
		lines = content.split( '\n' )[:-1]
		
		N = len( lines )
		correctness = 0.0 		# cf. the ACL wiki on SAT analogy questions
		zero_one_loss = 0.0

		# for every questions
		for line in lines:

			items  = line.split( '\t' )
			y_pred = int( items[2] )
			y_true = int( items[3] )

			# # simulate random predictions
			# from random import randrange
			# y_pred = randrange(5)

			# if predicted label is '-1', skip the datum
			if y_pred == -1:
				N -= 1
				continue

			if y_pred == y_true:
				correctness += 1.0				
				# print( '%s' % ( str( items )))
			else:
				zero_one_loss += 1.0
				print( '%s' % ( str( items )))

		if N == 0:
			print( "Similarity could not be computed for lack of feature overlap." )

		else:
			# normalize the loss by the number of items
			print( 'correctness: %f' % ( correctness / float( N )))
			print( 'zero-one loss: %f' % ( zero_one_loss / float( N )))
			print( 'samples predicted: %d' % ( N ))


if __name__ == "__main__":

	SAT_DIR = os.path.join( BASE_DIR, 'sat' ) 

	sat_file 	 	= os.path.join( SAT_DIR, 'SAT-package-V3.txt' )
	pair_file    	= os.path.join( SAT_DIR, 'pairs.tsv' )
	test_file    	= os.path.join( SAT_DIR, 'test.tsv' )
	vector_file  	= os.path.join( SAT_DIR, 'vectors.tsv' )
	# feature_file = os.path.join( SAT_DIR, 'features_from_db.tsv' )
	feature_file 	= os.path.join( SAT_DIR, 'features_from_vectors.tsv' )
	prediction_file = os.path.join( SAT_DIR, 'predictions.tsv' )

	clean_SAT_data( sat_file, pair_file, test_file )
	
	# get_features_from_db( pair_file, feature_file )
	get_features_from_vectors( pair_file, vector_file, feature_file )

	model     	 = 'VEC' 		# current options: DB, VEC
	sim_measure  = 'overlap' 	# current options: overlap, dotproduct, cosine
	
	compute_similarity_between_datapoints( test_file, feature_file, prediction_file, model, sim_measure ) 
	compute_loss( prediction_file )
	
