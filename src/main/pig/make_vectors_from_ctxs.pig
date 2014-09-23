/**
  */

DEFINE make_vectors_from_ctxs( INPUT_DIR ) 
RETURNS ctx_vectors {

	-- load data from disk
	ctxs = LOAD '$INPUT_DIR' USING PigStorage( '\t' ) 
		as ( jo:chararray, bim:chararray, cnt:double ) ;

	-- for computing the distance matrix in a next step
	-- assign unique numeric ids to each bim
	bims0 = FOREACH ctxs GENERATE bim ;
	bims1 = DISTINCT bims0 ;
	bims2 = RANK bims1 ;
	bims3 = FOREACH bims2 GENERATE $0 as rankid:long, $1 as bim:chararray ; -- really?
	ctxs_enhanced1 = JOIN ctxs BY bim, bims3 BY $1 ;
	ctxs_enhanced = FOREACH ctxs_enhanced1 GENERATE ctxs::jo as jo:chararray, 
		ctxs::bim as bim:chararray, ctxs::cnt as lmi:double, 
		bims3::rankid as bid:long ; -- how come bims2::rank_bims1 doesn't work?

	-- 
	ctx_vectors0 = FOREACH ( GROUP ctxs_enhanced BY jo ) {
		projected = FOREACH ctxs_enhanced GENERATE bid, lmi ;
		GENERATE FLATTEN( group ) as jo:chararray, 
			projected as values:bag{ t:tuple( bid:chararray, lmi:double )} ;
	} ;
	$ctx_vectors = RANK ctx_vectors0 ;
} ;