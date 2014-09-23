/**
	An NLP pipeline which takes a corpus[1] as input ( one sentence per line ), 
	selects from that corpus all those that contain pairs of distributionally
	similar common nouns[2], does some linguistic annotations[3], extracts 
	almost all subtrees as features[4], and delivers the data to a machine 
	learner ( either classifier[5] or clustering ). For details see the pipeline 
	specs below.
	
	Custom parameters can be specified in the <parameters> file. Also, 
	notice the <properties> file for pig and mapreduce specs.

	References:

	[1] currently either News120M or PukWac
	[2] http://www.langtech.tu-darmstadt.de/fileadmin/user_upload/Group_UKP/publikationen
		  /2013/BiemannRiedlText2D_JLM2013_60-369-1-PB.pdf
	[3] http://nlp.stanford.edu/software/index.shtml
	[4] http://nlp.stanford.edu/software/stanford-dependencies.shtml
	[5] https://github.com/policecar/simsets

	Usage: 
	pig -P <propertyfile> -m <parameterfile> pipeline.pig &> <logfile>
  */

set job.name '30<3' ;
set default_parallel 17 ;

-- register JAR when running on hadoop cluster
register '../../../target/sensim-1.0-SNAPSHOT-job.jar' ;

-- PIPELINE:

-- (00) EXTRACT SENTENCES FROM CORPUS ( OPTIONAL, IF NECESSARY )
-- (01) DEDUPLICATE SENTENCES ( REMOVE ZIPF ON THIS LEVEL )
-- (02) EITHER RUN NLP-PIPELINE, IE. PARSE AND ANNOTATE ( IF RAW DATA )
-- 		OR PARSE PRE-PARSED SENTENCES INTO MATCHING FORMAT ( IF PRE-PARSED )
-- (03) FILTER TARGET-EXPANSION PAIRS
-- (04) SELECT RELEVANT SUBCORPUS
-- (05) EXTRACT FEATURES ( ALL SUBTREES WITH RESTRICTIONS )
-- (06) DO SOME GLOBAL FREQUENCY PRUNING ( OPTIONAL )
--      ( MAKES DICTIONARIES OF PAIRS AND PATTERNS AS WELL )
-- (07) COUNT FREQUENCIES FOR HUMAN INSPECTION

-- FORK: 
-- OPTION 1/: DO CLASSIFICATION W/ SIMSETS, s. https://github.com/policecar/simsets
-- (08a) TRANSFORM FEATURES TO JOBIM FORMAT ( IE. MAKE ALL KINDS OF TRANSFORMATIONS )
-- (09a) COMPUTE MUTUAL INFORMATION OF CONTEXTS
-- (10a) MAKE FEATURE VECTORS FOR STEP (11a)
-- (11a) COMPUTE DISTANCE MATRIX AKA TRANSFORM FEATURE SPACE TO SIMILARITY SPACE
-- (XYa) DO TOPIC MODELING ( NOT IMPLEMENTED )

-- (10a) DO CLASSIFICATION WITH SIMSETS ( DIFFERENT CODE REPOSITORY )

-- OPTION 2/: DO CLUSTERING WITH R
-- (08b) MAKE FEATURE VECTORS
-- (09b) COMPUTE DISTANCE MATRIX
-- (10b) CLUSTER DATA ( ON A SHELL )

-- END OF PIPELINE

-- STEPS 00-02 NEED TO BE DONE ONLY ONCE PER CORPUS ( RESPECTIVE FOLDERS, FILES
-- ARE STORED ONE DIRECTORY UP ); THE OTHER STEPS ARE EXPERIMENT-SPECIFIC.

-- (00) GET PLAIN TEXT SENTENCES FROM PREPARSED NEWS120M CORPUS
import './get_sentences.pig';
sentences = get_sentences( '$BASE_DIR/../news120m.gz' );
rmf $BASE_DIR/../sentences.gz;
STORE sentences INTO '$BASE_DIR/../sentences.gz' USING PigStorage();

-- (01) DEDUPLICATE SENTENCES AKA REMOVE ZIPF DISTRIBUTION
sentences = LOAD '$BASE_DIR/../sentences.gz' USING PigStorage() ;
unique_sentences = DISTINCT sentences ;
rmf $BASE_DIR/../sentences_uniq.bz ;
STORE unique_sentences INTO '$BASE_DIR/../sentences_uniq.bz' USING PigStorage() ;

-- set split size to a small number to spread the workload 
-- produced by the parser onto more mappers
set mapred.min.split.size 300000 ;
set mapred.max.split.size 3000000 ;
-- set pig.maxCombinedSplitSize 100000 ;
set pig.noSplitCombination true;
set mapreduce.input.fileinputformat.split.maxsize 3000000 ;
set mapred.job.reuse.jvm.num.tasks -1 ;

-- (02) PARSE AND ANNOTATE SENTENCES
import './parse_sentences.pig' ;
parses = parse_sentences( '$BASE_DIR/../sentences_uniq.bz', 'en' ) ;
rmf $BASE_DIR/../annotated.bz ;
STORE parses INTO '$BASE_DIR/../annotated.bz' USING PigStorage('\t') ;


-- -- (01 alt) DEDUPLICATE SENTENCES AKA REMOVE ZIPF DISTRIBUTION
-- sentences = LOAD '$BASE_DIR/../pukwac4pig.gz' USING PigStorage() ;
-- unique_sentences = DISTINCT sentences ;
-- rmf $BASE_DIR/../pukwac4pig_uniq.bz ;
-- STORE unique_sentences INTO '$BASE_DIR/../pukwac4pig_uniq.bz' USING PigStorage() ;

-- -- (02 alt) READ PREPARSED PUKWAC DATA AND CONVERT THEM TO DKPRO JCAS FORMAT
-- import './transform_pukwac_to_cas.pig' ;
-- parses = transform_pukwac_to_cas( '$BASE_DIR/../pukwac4pig_uniq.bz', 'en' ) ;
-- rmf $BASE_DIR/../annotated.bz ;
-- STORE parses INTO '$BASE_DIR/../annotated.bz' ;


-- (03) PICK X MOST SIMILAR TARGET-EXPANSION PAIRS FROM JOBIM'S DISTRIBUTIONAL THESAURUS
import './filter_target_expansion_pairs.pig' ;
te_pairs_pruned = filter_target_expansion_pairs( '$BASE_DIR/../dt', $topX ) ; 
rmf $BASE_DIR/../target_expansions_pruned.gz ;
STORE te_pairs_pruned INTO '$BASE_DIR/../target_expansions_pruned.gz' USING PigStorage('\t') ;

-- (04) SELECT THE SUBCORPUS, EITHER VIA BLESS OR TARGET-EXPANSION PAIRS
import './generate_subcorpus.pig' ;
subcorpus = generate_subcorpus( '$BASE_DIR/../annotated.bz', 
	-- '$BASE_DIR/../bless_nouns.tsv', 'en', 304 ) ;
	-- '$BASE_DIR/../bless_nouns_enhanced.tsv', 'en', 304 ) ;
	-- '$BASE_DIR/../target_expansions.gz', 'en', 304 ) ;
	'$BASE_DIR/../target_expansions_pruned.gz', 'en', 304 ) ;
rmf $BASE_DIR/subcorpus.bz ;
STORE subcorpus INTO '$BASE_DIR/subcorpus.bz' USING PigStorage('\t') ;

-- (04b) EXTRACT AND STORE PAIRS AND THEIR RESPECTIVE SENTENCE FOR INSPECTION
import './gimme_pairs_and_sentences.pig' ;
pair_sentences = gimme_pairs_and_sentences( '$BASE_DIR/subcorpus.bz') ;
rmf $BASE_DIR/pair_sentences.gz ;
STORE pair_sentences INTO '$BASE_DIR/pair_sentences.gz' ;

-- reset number of mappers to more generally reasonable numbers
set pig.maxCombinedSplitSize 100000000 ; -- du features.gz > 1556910 (Kb)
set pig.splitCombination true ;
set mapred.job.reuse.jvm.num.tasks 4 ; -- semi-random choice

-- (05) FEATURE EXTRACTION
import './extract_features.pig' ;
features = extract_features( '$BASE_DIR/subcorpus.bz', 'COMMONNOUN', '6', '0' ) ;
-- Note that all nouns and patterns are lower-cased in extract_features.pig
rmf $BASE_DIR/features.gz ;
STORE features INTO '$BASE_DIR/features.gz' USING PigStorage('\t') ;

-- (06) DO GLOBAL FREQUENCY PRUNING ( AND MAKE DICTIONARIES W/ NUMERIC IDS )
import './get_pruned_pair_and_pattern_dicts.pig';
pairs, patterns = get_pruned_pair_and_pattern_dicts( '$BASE_DIR/features.gz', 0, 0, 2, 3 );
rmf $BASE_DIR/pairs_0023.gz;
STORE pairs INTO '$BASE_DIR/pairs_0023.gz' USING PigStorage();
rmf $BASE_DIR/patterns_0023.gz;
STORE patterns INTO '$BASE_DIR/patterns_0023.gz' USING PigStorage();

-- (07) COUNT ALL KINDS OF FREQUENCIES FOR LATER ANALYSIS 
-- ( results are stored to disk in-macro because it's many different files;
--   also: shell commands are not supported in macros, hence the rmf here   )
import './count_frequencies.pig';
rmf $BASE_DIR/frequencies.gz;
count_frequencies( '$BASE_DIR/features.gz', '$BASE_DIR/frequencies.gz' );


-- (08a) REWRITE FEATURES TO MATCH JOBIM FORMAT
import './rewrite_features_as_jobim.pig' ;
jobim, jobim_flipped = rewrite_features_as_jobim( '$BASE_DIR/features.gz' ) ;
rmf $BASE_DIR/jobim_feats.gz ;
STORE jobim INTO '$BASE_DIR/jobim_feats.gz' USING PigStorage('\t') ;
rmf $BASE_DIR/jobim_feats_flipped.gz ;
STORE jobim_flipped INTO '$BASE_DIR/jobim_feats_flipped.gz' USING PigStorage('\t') ;

-- (09a) COMPUTE THE MUTUAL INFORMATION ( LMI ) FOR ALL CONTEXT FEATURES
import './compute_mutual_information.pig' ;
-- for regular ..
ctx_lmi = compute_mutual_information( '$BASE_DIR/jobim_feats.gz' ) ;
rmf $BASE_DIR/ctx_lmi.gz ;
STORE ctx_lmi INTO '$BASE_DIR/ctx_lmi.gz' USING PigStorage('\t') ;
-- .. for flipped
ctx_lmi_flipped = compute_mutual_information( '$BASE_DIR/jobim_feats_flipped.gz' ) ;
rmf $BASE_DIR/ctx_lmi_flipped.gz ;
STORE ctx_lmi_flipped INTO '$BASE_DIR/ctx_lmi_flipped.gz' USING PigStorage('\t') ;


-- (09a cont.) PRUNE CONTEXT FEATURES TO GET ONLY THE X BEST PATTERNS
-- magic numbers ( until i find a good normalized measure, then use threshold )
%declare numCtx '1000' ;
-- %declare numCtx4Sim '2500' ;
%declare numCtx4Sim '1000' ;
%declare numSim '100' ;
import './prune_ctxs.pig' ;
-- prune for /ctx features
prune_ctxs( '$BASE_DIR/ctx_lmi.gz', '$BASE_DIR/ctx_$numCtx/ctx_lmi_pruned.gz', $numCtx ) ;
prune_ctxs( '$BASE_DIR/ctx_lmi_flipped.gz', '$BASE_DIR/ctx_$numCtx/ctx_lmi_flipped_pruned.gz', $numCtx ) ;
-- prune for /sim features
prune_ctxs( '$BASE_DIR/ctx_lmi.gz', '$BASE_DIR/ctx_$numCtx4Sim/ctx_lmi_pruned.gz', $numCtx4Sim ) ;
prune_ctxs( '$BASE_DIR/ctx_lmi_flipped.gz', '$BASE_DIR/ctx_$numCtx4Sim/ctx_lmi_flipped_pruned.gz', $numCtx4Sim ) ;

-- (10a) COMPUTE FEATURE VECTORS ( FOR SIMILARITY MATRIX )
import './make_vectors_from_ctxs.pig' ;
-- for regular ..
nvectors = make_vectors_from_ctxs( '$BASE_DIR/ctx_$numCtx4Sim/ctx_lmi_pruned.gz' ) ;
rmf $BASE_DIR/ctx_$numCtx4Sim/vectors_lmi.gz ;
STORE nvectors INTO '$BASE_DIR/ctx_$numCtx4Sim/vectors_lmi.gz' USING PigStorage('\t') ;
-- .. for flipped 
pvectors = make_vectors_from_ctxs( '$BASE_DIR/ctx_$numCtx4Sim/ctx_lmi_flipped_pruned.gz' ) ;
rmf $BASE_DIR/ctx_$numCtx4Sim/vectors_lmi_flipped.gz ;
STORE pvectors INTO '$BASE_DIR/ctx_$numCtx4Sim/vectors_lmi_flipped.gz' USING PigStorage('\t') ;

