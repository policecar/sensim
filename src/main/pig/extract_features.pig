/**
	Extract shortest dependency path between target and expansion 
	nouns as feature. Returns <noun1> <noun2> <feature> <sentence>
  */

DEFINE extract_features( INPUT_DIR, selectionType, subtreeSize, numSkipsInSubtree )
RETURNS features {

	-- define dynamic invokers
	DEFINE FeatureExtractor sensim.FeatureExtractor( '$selectionType', 
		'$subtreeSize', '$numSkipsInSubtree' ) ;

	-- read data from file
	parses = LOAD '$INPUT_DIR' USING PigStorage('\t') 
		as ( pair:tuple( noun1:chararray, noun2:chararray ), parse:chararray ) ;

	-- extract shortest path along dependency tree b/w the two nouns
	$features = FOREACH parses 
		GENERATE FLATTEN( FeatureExtractor( parse, pair.noun1, pair.noun2 ))
		as ( noun1:chararray, noun2:chararray, pattern:chararray, 
			 sentence:chararray );
};