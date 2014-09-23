/**
	Compute frequencies over patterns, pairs, sentences.
  */

DEFINE count_frequencies( INPUT_DIR, OUTPUT_DIR )
RETURNS void {

	-- read features from file
	lines = LOAD '$INPUT_DIR' USING PigStorage('\t') 
		AS ( noun1:chararray, noun2:chararray, pattern:chararray, sentence:chararray );

	-- combine every two nouns into a tuple
	lines = FOREACH lines GENERATE 
		TOTUPLE( noun1, noun2 ) AS pair, noun1, noun2, pattern, sentence;

	-- count number of times each sentence occurs ( s. Zipf's law )
	-- and how many unique noun pairs it occurs with
	sentencefrequencies = FOREACH ( GROUP lines BY sentence ) {
		uniquepairs = DISTINCT lines.pair;
		GENERATE COUNT( lines ) AS freq, COUNT( uniquepairs ) AS numpairs, group AS sentence;
	};
	sentencefrequencies = ORDER sentencefrequencies BY freq DESC;

	-- count the number of unique sentences and patterns per pair  << how skewed is the data
	pairfrequencies = FOREACH ( GROUP lines BY pair ) {
		uniquepatterns = DISTINCT lines.pattern;
		GENERATE group AS pair, COUNT( lines ) as numsentences, 
			COUNT( uniquepatterns ) AS numpatterns, uniquepatterns;
	};
	pairfrequencies = ORDER pairfrequencies BY numpatterns DESC PARALLEL 1;
	
	-- count number of pairs per patterns  << how long is the tail
	patternfrequencies = FOREACH ( GROUP lines by pattern ) {
		uniquepairs = DISTINCT lines.pair;
		GENERATE group AS pattern, COUNT( lines ) AS numsentences, 
			COUNT( uniquepairs ) AS numpairs, uniquepairs AS pairs;
	};
	patternfrequencies = ORDER patternfrequencies BY numpairs DESC PARALLEL 1;
	
	-- co-group pairs and patterns, and count their occurrences
	pairpatternfrequencies = FOREACH ( GROUP lines BY ( pair, pattern )) {
		GENERATE FLATTEN( group ), COUNT( lines ) as freq;
	};
	pairpatternfrequencies = ORDER pairpatternfrequencies BY pair, freq DESC PARALLEL 1;

	-- store noun pair, feature and source sentences
	STORE sentencefrequencies INTO '$OUTPUT_DIR/sentence-frequencies.gz' USING PigStorage('\t');
	STORE pairfrequencies INTO '$OUTPUT_DIR/pair-frequencies.gz' USING PigStorage('\t');
	STORE patternfrequencies INTO '$OUTPUT_DIR/pattern-frequencies.gz' USING PigStorage('\t');
	STORE pairpatternfrequencies INTO '$OUTPUT_DIR/pair-pattern-frequencies.gz' USING PigStorage('\t');
	
};