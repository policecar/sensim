package sensim;

import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordParser;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordPosTagger;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordSegmenter;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.UIMAException;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;

import dima.UIMAXMLConverterHelper;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

/**
 * Date: 4/10/13
 * Time: 1:51 PM
 *
 * @author Priska Herger
 *         <p/>
 *         Description: An annotator class that takes raw text as input
 *         and processes it through a core natural language pipeline.
 */
@OutputSchema("annotations:chararray")
public class CoreNLPAnnotator extends EvalFunc<String> {

	private final JCas jCas;
	private final AnalysisEngine engine;
	private final UIMAXMLConverterHelper uimaXMLConverterHelper;
	private final String language;

	public CoreNLPAnnotator(String language) {

		super();
		this.language = language;

		try {

			AnalysisEngineDescription aggregate = createEngineDescription(
				createEngineDescription(StanfordSegmenter.class),
				createEngineDescription(StanfordPosTagger.class,
						StanfordPosTagger.PARAM_LANGUAGE, language,
						StanfordPosTagger.PARAM_VARIANT, "wsj-0-18-left3words-distsim"),
				createEngineDescription(StanfordLemmatizer.class),
				createEngineDescription(StanfordParser.class,
						StanfordParser.PARAM_LANGUAGE, language,
						StanfordParser.PARAM_WRITE_PENN_TREE, true,
						StanfordParser.PARAM_WRITE_POS, false, // already done in PosTagger above
						StanfordParser.PARAM_PRINT_TAGSET, true,
						StanfordParser.PARAM_VARIANT, "pcfg")
			);

			engine = AnalysisEngineFactory.createEngine(aggregate);
			jCas = engine.newJCas();
			uimaXMLConverterHelper = new UIMAXMLConverterHelper(false);

		} catch (ResourceInitializationException e) {
			throw new IllegalArgumentException(e);
		} catch (UIMAException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public String exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		try {

			String sentence = (String) input.get(0);

			jCas.reset();
			jCas.setDocumentText(sentence);
			jCas.setDocumentLanguage(language);
			engine.process(jCas);

			return uimaXMLConverterHelper.serialize(jCas);

		} catch (AnalysisEngineProcessException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

		return super.getArgToFuncMapping();
	}
}
