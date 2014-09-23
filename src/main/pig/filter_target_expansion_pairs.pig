/**
	Read target-expansion pairs from JOBIM model[1] and 
	filter them by some definition of <word>, POS tag and top X.
	
	[1] eg. LMI_s_0_t_0_p_1000_l_200_simsort from https://bitly.com/1eaKaN9
  */

DEFINE filter_target_expansion_pairs( DT_DIR, numTops )
RETURNS top_similar { 

	-- read from file
	pairs = LOAD '$DT_DIR' USING PigStorage('\t') 
		as ( target:chararray, expansion:chararray, similarity:double ) ;

	-- keep only entries where both target and expansion word  
	-- (1) begin with a letter 
	-- (2) have length > 1 
	-- (3) are nouns ( singular or plural )
	-- (4) but not proper nouns ( NNP, NNPS )
	-- [ currently doesn't apply ] (5) and where the expansion does not equal the target
	-- note: '^\\s' is adopted from Google's syntactic n-grams ( s. their ReadMe )
	filtered1 = FILTER pairs BY 
		( target MATCHES '[a-zA-Z][\\S]+#NNS?' ) AND 
		( expansion MATCHES '[a-zA-Z][\\S]+#NNS?' ) ;

	-- remove POS tags ( #..)  from targets and expansions
	filtered2 = FOREACH filtered1 GENERATE REGEX_EXTRACT( target, '(.*)#.*', 1 ) 
		as target, REGEX_EXTRACT( expansion, '(.*)#.*', 1 ) as expansion, 
		similarity ;

	-- don't use identical pairs
	filtered3 = FILTER filtered2 BY ( target != expansion ) ;
	
	-- remove non-words where non-word is defined as a word containing digits 
	-- or other 'funny' symbols -- i'm sry.
	filtered = FILTER filtered3 BY 
		( REGEX_EXTRACT_ALL( target, '.*[0-9\\.\\+@].*' ) is NULL ) AND 
		( REGEX_EXTRACT_ALL( expansion, '.*[0-9\\.\\+@].*' ) is NULL ) ;

	-- use only the topX most similar expansions per target
	-- Note: similarities are not globally normalized, ie. they indicate 
	-- a within-target ranking
	grouped = GROUP filtered BY target ;
	$top_similar = FOREACH grouped {
		sorted = ORDER filtered BY target, similarity DESC ;
		top = LIMIT sorted $numTops ;
		GENERATE FLATTEN( top ) as ( target:chararray, expansion:chararray, similarity:double ) ;
	};
};