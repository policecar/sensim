
-- define dynamic invokers
DEFINE CoreNLPAnnotator sensim.CoreNLPAnnotator( 'en' );
DEFINE PukwacReader sensim.PukwacReader( 'en' );
DEFINE NounPairLabeler sensim.NounPairLabeler( 'en' ) ;
DEFINE FeatureExtractor sensim.FeatureExtractor( 'COMMONNOUN', '6', '0' ) ;

-- -- OPTION 1: PARSE SENTENCES
-- -- read data from file
-- sentences = LOAD '$INPUT_DIR' USING PigStorage('\n') as sentence:chararray ;
-- -- annotate and parse sentences
-- parses = FOREACH sentences GENERATE CoreNLPAnnotator( sentence ) as parse:chararray ;
-- rmf $OUTPUT_DIR/mini_parses ;
-- STORE parses INTO '$OUTPUT_DIR/mini_parses' ;

-- OPTION 2: READ SENTENCES FROM PRE_PARSED CORPUS
import './src/main/pig/transform_pukwac_to_cas.pig' ;
parses0 = transform_pukwac_to_cas( '$INPUT_DIR', 'en' ) ;
rmf $OUTPUT_DIR/mini_parses ;
STORE parses0 INTO '$OUTPUT_DIR/mini_parses' ;

parses1 = RANK parses0 ;
parses = FOREACH parses1 GENERATE $0 as id:int, $1 as parse:chararray ;


-- GENERATE SUBCORPUS
import './src/main/pig/generate_subcorpus.pig' ;
subcorpus = generate_subcorpus( '$OUTPUT_DIR/mini_parses', '$OUTPUT_DIR/mini_bless', 'en', 1 ) ;
rmf $OUTPUT_DIR/mini_subcorpus ;
STORE subcorpus INTO '$OUTPUT_DIR/mini_subcorpus' ;


-- more_sentences = FOREACH parses GENERATE FLATTEN( NounPairLabeler( id, parse )) 
-- 	AS ( noun1:chararray, noun2:chararray, parse:chararray ) ;

-- -- lowercase nouns ( cf. lowercased, lemmatized target-expansion pairs )
-- subcorpus = FOREACH more_sentences GENERATE TOTUPLE( LOWER( noun1 ), LOWER( noun2 )) as pair, parse ;
-- rmf $OUTPUT_DIR/mini_subcorpus ;
-- STORE subcorpus INTO '$OUTPUT_DIR/mini_subcorpus' ;


-- EXTRACT FEATURES
-- subcorpus = LOAD '$OUTPUT_DIR/mini_subcorpus' as ( pair:tuple( noun1:chararray, noun2:chararray ), parse:chararray ) ;
features0 = FOREACH subcorpus GENERATE FLATTEN( FeatureExtractor( parse, pair.$0, pair.$1 ))
	as ( noun1:chararray, noun2:chararray, pattern:chararray, sentence:chararray ) ;

-- Note: nouns are lowercased in generate_subcorpus.pig and patterns are 
-- lowercased in FeatureExtractor.java
-- $features = FOREACH features GENERATE noun1, noun2, pattern, sentence ;

features = DISTINCT features0 ;
rmf $OUTPUT_DIR/mini_features ;
STORE features INTO '$OUTPUT_DIR/mini_features' ;
