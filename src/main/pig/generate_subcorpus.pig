/**
	From input corpus pick sentences that contain a target-expansion pair.

	mileage ( last measured ): 2hrs, 17mins, 26sec
  */

DEFINE generate_subcorpus( CORPUS_DIR, PAIR_DIR, language, numReducers )
RETURNS subcorpus {

	-- define dynamic invokers for UDFs
	DEFINE NounPairLabeler sensim.NounPairLabeler( '$language' ) ;

	-- read input files
	parses0 = LOAD '$CORPUS_DIR' USING PigStorage('\n') as parse:chararray ;
	parses1 = RANK parses0 ;
	parses = FOREACH parses1 GENERATE (long) $0 as id:long, $1 as parse:chararray ;
	
	filterpairs0 = LOAD '$PAIR_DIR' USING PigStorage('\t') 
		as ( target:chararray, expansion:chararray, similarity:chararray ) ;
		-- as ( target:chararray, expansion:chararray, similarity:double ) ;

	filterpairs = FOREACH filterpairs0 GENERATE TOTUPLE( target, expansion ) as pair ; 

	-- first approach t"was: generate all noun pairs for each parse, join them 
	-- with filter pairs ( eg. target-expansion pairs or BLESS pairs ); problem:
	-- this temporarily generates much more data than needed; hence try the 
	-- following: assign id to parses, make NounPairLabeler return noun pairs 
	-- and parse ids instead of parses themselves; join noun pairs with target 
	-- expansion pairs, add parses to result
	
	-- find all noun pairs in each sentence and return them with the resp. parse id
	-- Note: the NounPairLabeler returns lemmatized ( but not lowercased ) nouns
	pairparsepairings = FOREACH parses GENERATE FLATTEN( NounPairLabeler( id, parse )) 
		as ( noun1:chararray, noun2:chararray, parseid:long ) ;
	
	-- lowercase nouns ( cf. lowercased, lemmatized target-expansion pairs )
	-- Note: the lemmatizer used with these particular target-expansions was
	-- somewhat broken; consider re-lemmatizing here
	aligned = FOREACH pairparsepairings GENERATE 
		TOTUPLE( LOWER( noun1 ), LOWER( noun2 )) as pair, (long) parseid ;
	
	-- filter extracted noun pairs with filter pairs ( eg. target-expansion 
	-- pairs or BLESS pairs )
	joined1 = JOIN aligned BY pair, filterpairs BY pair PARALLEL $numReducers ;
	interim = FOREACH joined1 GENERATE filterpairs::pair as pair, aligned::parseid as id:long ;

	-- attach parses to joined by parse ID
	joined2 = JOIN interim BY id, parses BY id PARALLEL $numReducers ;
	$subcorpus = FOREACH joined2 GENERATE interim::pair, parses::parse ;
};
