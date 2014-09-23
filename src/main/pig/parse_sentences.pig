/**
	Send each incoming sentence through a basic NLP pipeline 
	for POS tagging, lemmatizing and dependency parsing.

  */

DEFINE parse_sentences( INPUT_DIR, language )
RETURNS parses {

	-- define dynamic invokers
	DEFINE CoreNLPAnnotator sensim.CoreNLPAnnotator( '$language' );

	-- read data from file
	sentences = LOAD '$INPUT_DIR' USING PigStorage() as sentence:chararray ;

	-- annotate and parse sentences
	$parses = FOREACH sentences GENERATE CoreNLPAnnotator( sentence );

};