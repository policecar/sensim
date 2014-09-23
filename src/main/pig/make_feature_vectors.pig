/**
	From textual features make numeric feature vectors.
  */

DEFINE make_feature_vectors( FEATURE_DIR, PATTERN_DIR, PAIR_DIR, minPatFreq, minNumPat )
RETURNS vectors, pairs, patterns {

	-- register user-defined functions ( UDF )
	DEFINE IndexToIndexMapper sensim.IndexToIndexMapper( '$PATTERN_DIR/../patterns_idx_map/part-r-00000' );

	-- read features from file ( skip sentence )
	lines0 = LOAD '$FEATURE_DIR' USING PigStorage('\t')
		as ( noun1:chararray, noun2:chararray, pattern:chararray ) ;
		
	-- combine nouns to tuples
	lines = FOREACH lines0 GENERATE TOTUPLE( noun1, noun2 ) as pair, pattern ;

	-- load patterns
	patterns0 = LOAD '$PATTERN_DIR' USING PigStorage('\t') 
		as ( id:int, pattern:chararray, numsentences:int, numpairs:int );

	-- load pairs
	pairs0 = LOAD '$PAIR_DIR' USING PigStorage('\t')
		as ( id:int, pair:tuple( noun1:chararray, noun2:chararray ),
			 numsentences:int, numpatterns:int );

	-- select unique pair-pattern combinations and count them
	pairpatterns = FOREACH ( GROUP lines BY ( pair, pattern )) {
		GENERATE FLATTEN( group ) as ( pair, pattern ), COUNT( lines ) as freq;
	};

	-- filter through patterns ( which is a frequency-pruned subset of all patterns)
	-- and add numeric pattern ids
	joined1 = JOIN pairpatterns BY pattern, patterns0 BY pattern;
	-- reorder columns and remove duplicate column 'pattern'
	joined2 = FOREACH joined1 GENERATE $0 as pair, $3 as patternid, $1 as pattern, $2 as patternfreq;

	-- filter through pairs ( which is a frequency-pruned subset of all pairs )
	-- and add numeric pair ids
	joined3 = JOIN joined2 BY pair, pairs0 BY pair;
	joined4 = FOREACH joined3 GENERATE 
		$4 as pairid, $0 as pair, $1 as patternid, $2 as pattern, $3 as patternfreq;

	-- collect observed patterns per pair, substitute pattern with its numeric ID
	fvectors = GROUP joined4 BY ( pairid, pair );
	vectors1 = FOREACH fvectors {
		projected = FOREACH joined4 GENERATE patternid, patternfreq ;
		-- projected = FOREACH joined4 GENERATE pattern, patternfreq ; -- use patterns instead of ids
		-- frequency pruning: keep only patterns that occur more than minPatFreq with this pair
		filtered = FILTER projected BY ( patternfreq > $minPatFreq );
		GENERATE FLATTEN( group ) as ( pairid, pair ), 
			filtered as values:bag{ t:tuple( idx:int, cnt:long )} ;
			-- filtered as values:bag{ t:tuple( idx:chararray, cnt:long )} ; -- use patterns instead of ids
	};

	-- annoying hack because i can't get Pig to effect the filters below otherwise
	STORE vectors1 INTO '$PATTERN_DIR/../manualvectors.gz' USING PigStorage('\t');

	vectors1 = LOAD '$PATTERN_DIR/../manualvectors.gz' USING PigStorage('\t')
	as ( pairid:int, pair:tuple( noun1:chararray, noun2:chararray), 
		 values:bag{ t:tuple( idx:int, cnt:long )}) ;
		 -- values:bag{ t:tuple( idx:chararray, cnt:long )}) ; -- use patterns instead of ids
	-- end of annoying hack

	-- WTF works if loaded from disk, doesn't otherwise
	-- more frequency pruning: throw away empty bags and pairs with less than 2 patterns
	vectors2 = FILTER vectors1 BY (( values is not NULL ) AND ( not IsEmpty( values ))) ;
	vectors3 = FILTER vectors2 BY ( SIZE( values ) > $minNumPat ) ;
	
	--  compute and rank final, pruned selection of pairs
	pairs1 = FOREACH ( GROUP vectors3 BY pair ) GENERATE group as pair ;
	-- retrieve frequency counts from pairs0
	pairs2 = JOIN pairs0 BY pair, pairs1 by pair ;
	pairs3 = FOREACH pairs2 GENERATE pairs0::pair, pairs0::numsentences, pairs0::numpatterns ;
	-- assign new numeric ids
	pairs4 = RANK pairs3 ;
	pairs5 = FOREACH pairs4 GENERATE $0 as id, $1 as pair, $2 as numsentences, $3 as numpatterns ;

	-- substitute the pair's numeric ids in these vectors with gap-free numbers 
	-- from the freshly squeezed, pruned pairs
	vectors4 = JOIN vectors3 BY pair, pairs5 BY pair;
	vectors5 = FOREACH vectors4 GENERATE pairs5::id as id, pairs5::pair as pair, 
		vectors3::values as values ;
	
	$pairs = FOREACH pairs5 GENERATE $0, $1, $2, $3 ;

	patterns1 = FOREACH vectors3 GENERATE FLATTEN( values ) ;
	patterns2 = FOREACH ( GROUP patterns1 BY idx ) GENERATE group as idx, SUM( patterns1.cnt ) as cnt ;
	-- retrieve frequency counts from patterns0
	patterns3 = JOIN patterns0 BY id, patterns2 BY idx ;
	patterns4 = FOREACH patterns3 GENERATE 
		patterns0::id as id, patterns0::pattern as pattern, 
		patterns0::numsentences as numsentences, patterns0::numpairs as numpairs ; 
	patterns5 = RANK patterns4 ;
	$patterns = FOREACH patterns5 GENERATE $0, pattern, numsentences, numpairs ;

	-- make a map from old IDs to new IDs
	idxmap1 = FOREACH patterns5 GENERATE $1 as previd, $0 as newid ;
	idxmap = ORDER idxmap1 BY $0 PARALLEL 1; -- probably unnecessary because sorted already
	STORE idxmap INTO '$PATTERN_DIR/../patterns_idx_map' USING PigStorage('\t');

	grouped = GROUP vectors5 BY ( id, pair ) ;
	$vectors = FOREACH grouped {
		projected2 = FOREACH vectors5 GENERATE FLATTEN( values ) ;
		substituted = FOREACH projected2 GENERATE IndexToIndexMapper( idx ), cnt ;
		GENERATE FLATTEN( group ), substituted 
			as values:bag{ t:tuple( idx:int, cnt:long )} ;
	};
	-- $vectors = FOREACH vectors5 GENERATE $0.. ;

};
