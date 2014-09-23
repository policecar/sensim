/**
	Rewrite features to match JoBim format required by simsets.
	Rewrite features as output by extract_features.pig to match stevo's JoBim
	format; ie. word1 \t @::patter::word2 \t count

	Input:
		noun1 \t noun2 \t pattern

	Output:
	
		noun1 \t @::verb::noun2
		noun1 \t noun2::verb^-1::@
		noun2 \t noun1::verb::@
		noun2 \t @::verb^-1::noun1
		verb \t noun1::@::noun2
		verb^1 \t noun2::@::noun1
	
	and flipped:
	
		@::verb::noun2  noun1
		noun1::@::noun2  verb
		noun1::verb::@  noun2
		@::verb^-1::noun1  noun2
		noun2::@::noun1  verb^-1
		noun2::verb^-1::@  noun1
  */

DEFINE rewrite_features_as_jobim( INPUT_DIR ) 
RETURNS jobim_feats, jobim_feats_flipped {

	features0 = LOAD '$INPUT_DIR' USING PigStorage('\t') 
		as ( noun1:chararray, noun2:chararray, pattern:chararray ) ;
	
	-- reformat to match stevo's jobim format
	jobim_n1 = FOREACH features0 GENERATE noun1 as jo, 
		CONCAT( CONCAT( '@::', pattern ), CONCAT( '::', noun2 )) as bim, 
		1 as cnt ;
	jobim_n1_inv = FOREACH features0 GENERATE noun1 as jo, 
		CONCAT( CONCAT( noun2, '::' ), CONCAT( pattern, '^-1::@' )) as bim, 
		1 as cnt ;
	jobim_n2 = FOREACH features0 GENERATE noun2 as jo, 
		CONCAT( CONCAT( noun1, '::' ), CONCAT( pattern, '::@' )) as bim, 
		1 as cnt ;
	jobim_n2_inv = FOREACH features0 GENERATE noun2 as jo, 
		CONCAT( CONCAT( '@::', pattern ), CONCAT( '^-1::', noun1 )) as bim, 
		1 as cnt ;
	jobim_pat = FOREACH features0 GENERATE pattern as jo, 
		CONCAT( CONCAT( noun1, '::@::' ), noun2 ) as bim, 1 as cnt ;
	jobim_pat_inv = FOREACH features0 GENERATE CONCAT( pattern, '^-1' ) as jo, 
		CONCAT( CONCAT( noun2, '::@::' ), noun1 ) as bim, 1 as cnt ;
	
	jobim = UNION jobim_n1, jobim_n1_inv, jobim_n2, jobim_n2_inv, jobim_pat, 
		jobim_pat_inv ;

	-- in case any lines occur doubly, merge them and add their counts
	-- here, since counts are 1, COUNT(..) works fine, else use SUM(..)
	grouped = GROUP jobim BY ( jo, bim ) ;
	summed = FOREACH grouped GENERATE FLATTEN( group ), COUNT( jobim ) ;
	$jobim_feats = DISTINCT summed PARALLEL 1 ;
	$jobim_feats_flipped = FOREACH $jobim_feats GENERATE $1, $0, $2;
} ;
