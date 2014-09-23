	/**
	Mostly copied & pasted from Alan's
	/home/alan/pipeline/distance/macro_distance_pair_join_cosine.pig
  */

DEFINE compute_distance_matrix( VECTOR_DIR )
RETURNS dmatrix {

	-- define dynamic invokers for UDFs
	DEFINE CosineDistancePigFunction dima.CosineDistancePigFunction( '-skipValue 1 -offset 0' );

	-- read feature vectors from file
	vectors0 = LOAD '$VECTOR_DIR' USING PigStorage() 
		as ( pairid:int, pair:chararray, values:bag{ t:tuple( idx:int, cnt:double )} ) ;
		-- as ( pairid:int, pair:tuple( noun1:chararray, noun2:chararray ), 
		-- 	values:bag{ t:tuple( idx:int, cnt:double )} ) ;

	-- compute the cardinality of pairs and patterns
	-- format: pairDim.cardinality is a number
	pairDim1 = FOREACH ( GROUP vectors0 BY pairid ) GENERATE (int) COUNT( vectors0 ) ;
	pairDim = FOREACH ( GROUP pairDim1 ALL ) GENERATE (int) COUNT( pairDim1 ) as cardinality ;

	-- format: patDim as relation with two fields: cardinality and totalcount
	patDim1 = FOREACH vectors0 GENERATE FLATTEN( values );
	patDim2 = FOREACH ( GROUP patDim1 BY idx ) GENERATE group as idx, SUM( patDim1.cnt );
	patDim = FOREACH ( GROUP patDim2 ALL ) GENERATE 
		(int) COUNT( patDim2 ) as cardinality, patDim2.$1 as totalcount ;
	patDimSingle = FOREACH ( GROUP patDim2 ALL ) GENERATE (int) COUNT( patDim2 ) as cardinality ;


	-- reformat vectors to something like
	-- ( key:int, val:( cardinality:int, entries: { entry: ( index:int, value:double )}))
	vectors = FOREACH vectors0 GENERATE pairid as key, TOTUPLE( patDimSingle.cardinality, values ) as val; 

	replicated_vectors = FOREACH vectors GENERATE FLATTEN( val.values.idx ), key;

	--Group replicated vectors by the feature id
	grouped = GROUP replicated_vectors BY idx;
	
	--Per feature cross all vectors with this feature.
	--Statistics show that only a few vectors share a feature, 
	--thus we can do the cross in the memory.
	--Pig cannot nest in a for each cross.
	--Workaround: We use two unnests (flatten) for the cross 
	paired = FOREACH grouped GENERATE 
		FLATTEN( replicated_vectors.key ) as key1, 
		FLATTEN( replicated_vectors.key ) as key2; 
	
	--We want only the lower triangular matrix with the diagonal	
	filtered_by_key = FILTER paired BY key1 >= key2;
	
	distinct_pairs = DISTINCT filtered_by_key;
		 	
	joined1 = JOIN distinct_pairs by key1, vectors by key;
	joined1_rename = FOREACH joined1 GENERATE 
		distinct_pairs::key2 as key2, 
		distinct_pairs::key1 as key1, 
		vectors::val as val1;
	
	joined2 = JOIN joined1_rename by key2, vectors by key;
	joined2_rename = FOREACH joined2 GENERATE 
		joined1_rename::key1 as key1, 
		joined1_rename::key2 as key2, 
		val1, 
		vectors::val as val2;

	distance_raw = FOREACH joined2_rename GENERATE 
		key1 as row, 
		key2 as column, 
		CosineDistancePigFunction( val1, val2 ) as distance;
	distance_less_raw = FILTER distance_raw BY ( distance is not NULL );

	-- CONVERT TO MATRIX MARKET FORMAT
	-- now count the number of elements that we got back
	distance_group = GROUP distance_less_raw ALL;
	distance_count = FOREACH distance_group GENERATE COUNT_STAR( distance_less_raw ) as elements;

	-- store the counts, so we can later use this information for the matrix storage
	elements_counts = FOREACH patDim GENERATE 
		(long) patDim.cardinality, 
		(long) pairDim.cardinality as global, 
		(long) distance_count.elements;

	sorted = ORDER distance_less_raw by row ASC, column ASC, distance ASC PARALLEL 1;
	$dmatrix = CROSS sorted, elements_counts;
	-- $dmatrix = FOREACH dmatrix GENERATE $0, $1, $2;

} ;