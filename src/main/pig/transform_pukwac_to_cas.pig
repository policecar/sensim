/**
	Transform the preprocessed PukWAC corpus ( cf. prepare_pukwac.py ) to
	DKPro's JCas format, analogously to the output of parse_sentences.pig.

  */

DEFINE transform_pukwac_to_cas( INPUT_DIR, language )
RETURNS parses {

	-- define dynamic invokers
	DEFINE PukwacReader sensim.PukwacReader( '$language' );

	-- read data from file
	conll_sentences = LOAD '$INPUT_DIR' USING PigStorage('\n') 
		as conll_sentence:chararray ;

	-- transform the pre-annotated sentence to UIMAXML
	$parses = FOREACH conll_sentences GENERATE PukwacReader( conll_sentence );
};