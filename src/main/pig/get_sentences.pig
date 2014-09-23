/**
	From data as received, discard parses and get plain text sentences only.
  */

DEFINE get_sentences( INPUT_DIR ) 
RETURNS sentences {

	lines = LOAD '$INPUT_DIR' USING PigStorage('\t');
	$sentences = FOREACH lines GENERATE $0;

};