/**
	Computes the mutual information of JoBim features extracted from a corpus.
	Here, mutual information is all Lexicographer's MI, pointwise MI and 
	normalized pointwise mutual information.

	Input  format: jo \t bim \t count
	Output format: jo \t bim \t mutualinf
  */

DEFINE compute_mutual_information( INPUT_DIR ) 
RETURNS ctx_lmi {

	-- load data from disk
	features = LOAD '$INPUT_DIR' USING PigStorage( '\t' ) 
		as ( jo:chararray, bim:chararray, cnt:double ) ;

	-- definition of PMI acc. to Evert 2005 ( PhD thesis ): 
	-- PMI is the ( logarithmic ) ratio of the actual joint probability 
	-- of two events to their expected joint probability if they were 
	-- independent events: P(A n B) / P(A) * P(B)

	-- count frequencies:
	-- P(A) = count A / count all nouns, P(B) analogously
	-- P(A n B) = count pair AB / count all pairs

	joint_counts = FOREACH ( GROUP features BY ( jo, bim )) {
		GENERATE group as jobim, SUM( features.cnt ) as cnt ;
	} ;
	
	jo_counts = FOREACH ( GROUP features BY jo ) {
		GENERATE group as it, SUM( features.cnt ) as cnt ;
	} ;
	bim_counts = FOREACH ( GROUP features BY bim ) {
		GENERATE group as it, SUM( features.cnt ) as cnt ;
	} ;
	single_counts = UNION jo_counts, bim_counts ;

	-- compute total count over all single and total counts
	-- Note: do this here, before spreading noun records again
	total_joint = FOREACH ( GROUP joint_counts ALL ) {
		GENERATE SUM( joint_counts.cnt ) as total ;
	} ;
	total_single = FOREACH ( GROUP single_counts ALL ) {
		GENERATE SUM( single_counts.cnt ) as total ;
	} ;

	-- combine all counts in one relation /data structure
	all_counts0 = JOIN joint_counts BY jobim.jo, single_counts BY $0 ;
	all_counts1 = FOREACH all_counts0 GENERATE 
		joint_counts::jobim as jobim, joint_counts::cnt as jobim_cnt,
		single_counts::it as jo, single_counts::cnt as jo_cnt ;
	all_counts2 = JOIN all_counts1 BY jobim.bim, single_counts BY $0 ;
	all_counts = FOREACH all_counts2 GENERATE 
		all_counts1::jobim as jobim, all_counts1::jobim_cnt as jobim_cnt,
		all_counts1::jo as jo, all_counts1::jo_cnt as jo_cnt, 
		single_counts::it as bim, single_counts::cnt as bim_cnt ;

	-- normalize counts
	normalized = FOREACH all_counts GENERATE 
		jobim, jobim_cnt,
		( jobim_cnt / total_joint.total ) as sc_jobim_cnt, 
		jo, ( jo_cnt / total_single.total ) as sc_jo_cnt,
		bim, ( bim_cnt / total_single.total ) as sc_bim_cnt ;

	-- compute pointwise mutual information ( PMI ), normalized PMI, 
	-- and lexicographer's /local mutual information ( LMI ) which 
	-- is the PMI score times the pair frequency
	-- Note: the Lexicographers mutual information is defined as in Bordag 2008
	-- Note: Pig Latin does not offer a built-in function for LOG2 (as yet), 
	-- hence apply: logA X = logB X / logB A
	pmi_npmi_lmi = FOREACH normalized GENERATE 
		FLATTEN( jobim ) as ( jo, bim ), 
		( LOG10( sc_jobim_cnt / ( sc_jo_cnt * sc_bim_cnt )) / LOG10( 2.0 )) as pmi,
		(( LOG10( sc_jobim_cnt / ( sc_jo_cnt * sc_bim_cnt )) / LOG10( 2.0 )) / - ( LOG10( sc_jobim_cnt ) / LOG10( 2.0 ))) as npmi,
		(( LOG10( sc_jobim_cnt / ( sc_jo_cnt * sc_bim_cnt )) / LOG10( 2.0 )) * jobim_cnt ) as lmi ;

	--  pmi1 = FOREACH pmi_npmi_lmi GENERATE jo, bim,  pmi ;
	-- npmi1 = FOREACH pmi_npmi_lmi GENERATE jo, bim, npmi ;
	 lmi1 = FOREACH pmi_npmi_lmi GENERATE jo, bim, lmi ;

	-- sort them alphabetically and by mutual information
	--  pmi = ORDER  pmi1 BY jo,  pmi DESC PARALLEL 1 ;
	-- npmi = ORDER npmi1 BY jo, npmi DESC PARALLEL 1 ;
	$ctx_lmi = ORDER lmi1 BY jo, lmi DESC PARALLEL 1 ;
} ;
