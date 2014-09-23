package sensim;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NN;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import dima.UIMAXMLConverterHelper;
import org.apache.commons.io.IOUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 11/19/13
 * Time: 1:57 AM
 *
 * @author Priska Herger
 */
@OutputSchema("sentences:bag {sentence:tuple (noun1:chararray, noun2:chararray, parse:chararray)}")
public class NounPairLabeler extends EvalFunc<DataBag> {

	private final JCas jCas;
	private final String language;

	private UIMAXMLConverterHelper uimaXMLConverterHelper;
	private BagFactory bagFactory = BagFactory.getInstance();
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	public NounPairLabeler(String language) throws UIMAException {

		super();
		this.language = language;

		uimaXMLConverterHelper = new UIMAXMLConverterHelper(false);
		jCas = JCasFactory.createJCas();

	}

	@Override
	public DataBag exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null || input.get(1) == null) {
			return null;
		}

		DataBag dataBag = bagFactory.newDefaultBag();
		
		try {

			long parseId = (Long) input.get(0);

			CharSequence charseq = (CharSequence) input.get(1);
			InputStream stream = IOUtils.toInputStream(charseq, Charsets.UTF_8.name());
			// note that jCas is changed in deserialize(...) and contains different data upon return!
			// design decision in favor of speed at the expense of readability
			uimaXMLConverterHelper.deserialize(stream, jCas);

			Iterator<Sentence> sentences = JCasUtil.iterator(jCas, Sentence.class);

			while (sentences.hasNext()) {

				Sentence sentence = sentences.next();

				//ArrayList<Token> tokens = Lists.newArrayList(JCasUtil.select(jCas, Token.class));
				List<Token> tokens = JCasUtil.selectCovered(Token.class, sentence);
				ArrayList<Token> nouns = Lists.newArrayList();
				for (Token t : tokens) {
					if (t.getPos() instanceof NN) {
						nouns.add(t);
					}
					// need this for the PukWac POS tags ( unfortunately )
					else if (t.getPos().getPosValue().equals("NN") || t.getPos().getPosValue().equals("NNS")) {
						nouns.add(t);
					}
				}

				if (nouns == null || nouns.size() == 1) {
					return null;
				}

				// get all pairs of nouns omitting incestuous and duplicate pairs
				List<List<Token>> entityPairs = Lists.newArrayList();
				//TODO: consider including inversed pairs here; else include them later in pipeline!
				for (int i = 0; i < nouns.size(); i++) {
					for (int j = i + 1; j < nouns.size(); j++) {

						entityPairs.add(Lists.newArrayList(nouns.get(i), nouns.get(j)));
					}
				}

				for (List<Token> pair : entityPairs) {

					Token n1 = pair.get(0);
					Token n2 = pair.get(1);

					Tuple tuple = tupleFactory.newTuple(3);

					// output: first noun \t second noun \t parseId
					tuple.set(0, (n1).getLemma().getValue());
					tuple.set(1, (n2).getLemma().getValue());
					tuple.set(2, parseId);
					dataBag.add(tuple);
				}
			}
			return dataBag;

		} catch (AnalysisEngineProcessException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (UIMAException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

		return super.getArgToFuncMapping();
	}
}
