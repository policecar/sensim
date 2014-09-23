/**
	From features make pair and pattern dictionaries 
	incl. numeric IDs and counts. Do some frequency pruning.
  */

DEFINE get_pruned_pair_and_pattern_dicts( FEATURE_DIR, num_sent_per_pair, 
	num_sent_per_pattern, num_pat_per_pair, num_pair_per_pattern )
RETURNS pairs, patterns {

	-- read data from file
	features0 = LOAD '$FEATURE_DIR' USING PigStorage() 
		as ( noun1:chararray, noun2:chararray, pattern:chararray, sentence:chararray ) ;
	
	-- combine nouns to noun pairs
	features = FOREACH features0 GENERATE 
		TOTUPLE( noun1, noun2 ) as pair, pattern, sentence ;

	-- distill unique pairs including their occurrence counts ( wrt sentences and patterns )
	pairs1 = FOREACH ( GROUP features BY pair ) {
		uniquepatterns = DISTINCT features.pattern ;
		GENERATE 
			group as pair, 
			COUNT( features ) as numsentences, 
			COUNT( uniquepatterns ) as numpatterns ;
	};
	-- frequency prune pairs
	pairs2 = FILTER pairs1 BY ( numsentences > $num_sent_per_pair ) AND
		( numpatterns > $num_pat_per_pair ) ;

	-- sort pairs by frequency
	pairs3 = ORDER pairs2 BY numsentences DESC, numpatterns DESC, pair ;
	
	-- attach a unique numeric ID to each pair
	$pairs = RANK pairs3 ;

	-- distill unique patterns including their occurrence counts ( wrt sentences and pairs )
	patterns1 = FOREACH ( GROUP features BY pattern ) {
		uniquepairs = DISTINCT features.pair ;
		GENERATE 
			group as pattern, 
			COUNT( features ) as numsentences, 
			COUNT( uniquepairs ) as numpairs ;
	};
		-- frequency prune patterns
	patterns2 = FILTER patterns1 BY ( numsentences > $num_sent_per_pattern ) AND 
		( numpairs > $num_pair_per_pattern ) ;

	-- sort patterns by frequency
	patterns3 = ORDER patterns2 BY numsentences DESC, numpairs DESC, pattern ;

	-- attach unique numeric ID to each pattern
	$patterns = RANK patterns3 ;

};
