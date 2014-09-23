/**

  */

DEFINE gimme_pairs_and_sentences( INPUT_DIR )
RETURNS selection {

	-- define dynamic invokers
	DEFINE PairSentenceSelector sensim.PairSentenceSelector();

	-- read data from file
	subcorpus = LOAD '$INPUT_DIR' 
		as ( pair:tuple( noun1:chararray, noun2:chararray ), parse:chararray ) ;

	-- select sentence in plain text from parse for visual inspection
	$selection = FOREACH subcorpus GENERATE FLATTEN( PairSentenceSelector( pair, parse ));

};