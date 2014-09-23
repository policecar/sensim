/**
	Reduce context features to best X according to the Lexicographer's 
	mutual information.
  */

DEFINE prune_ctxs( INPUT_DIR, OUTPUT_DIR, numContexts ) 
RETURNS void {

	-- load data
	ctxs = LOAD '$INPUT_DIR' USING PigStorage( '\t' ) 
		as ( jo:chararray, bim:chararray, cnt:double ) ;

	-- prune all contexts to keep only the X best
	ctx_pruned = FOREACH ( GROUP ctxs BY jo ) {
		sorted = ORDER ctxs BY jo, cnt DESC ;
		top = LIMIT sorted $numContexts ;
		-- top = LIMIT ctxs $numContexts ;
		GENERATE FLATTEN( top ) ;
	} ;

	-- save data straight to disks
	-- rmf $OUTPUT_DIR ;
	STORE ctx_pruned INTO '$OUTPUT_DIR' USING PigStorage('\t') ;
} ;