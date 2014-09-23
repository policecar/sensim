package sensim;

import com.google.common.base.Joiner;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NN;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NN_Type;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.resources.MappingProvider;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import dima.UIMAXMLConverterHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.Tuple;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.factory.JCasBuilder;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 4/11/14
 * Time: 5:51 PM
 *
 * @author Priska Herger
 *
 * Customization of the Conll2006Reader.java from DKPro.
 *
 */
@OutputSchema("annotations:chararray")
public class PukwacReader extends EvalFunc<String> {

	private final JCas jCas;
	private UIMAXMLConverterHelper uimaXMLConverterHelper;

	private final String language;

	private static final int TOKEN 	= 0;
	private static final int LEMMA 	= 1;
	private static final int POS 	= 2;
	private static final int ID 	= 3;
	private static final int HEAD 	= 4;
	private static final int DEP 	= 5;

	public PukwacReader(String language) throws UIMAException {

		super();
		this.language = language;

		jCas = JCasFactory.createJCas();
		uimaXMLConverterHelper = new UIMAXMLConverterHelper(false);
	}

	@Override
	public String exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		jCas.reset();
		JCasBuilder doc = new JCasBuilder(jCas);

		try {

			String conllSentence = (String) input.get(0);

			List<String[]> words = readSentence(conllSentence);
			if(words == null || words.size() == 0) {
				return null;
			}

			int sentenceBegin = doc.getPosition();
			int sentenceEnd = sentenceBegin;

			// add the text to doc ( unsure if this is the best way to do it )
			String[] sentenceArray = new String[words.size()];
			for(int i =0; i < words.size(); i++) {
				sentenceArray[i] = words.get(i)[TOKEN];
			}
			String satz = Joiner.on(" ").join(sentenceArray);
			doc.getJCas().setDocumentText(satz);
			doc.getJCas().setDocumentLanguage(this.language);

			// process tokens, lemmas, POS tags
			Map<Integer, Token> tokens = new HashMap<Integer, Token>();
			for (String[] word : words) {

				// process token
				Token token = doc.add(word[TOKEN], Token.class);
				tokens.put(Integer.valueOf(word[ID]), token);
				doc.add(" ");

				// process lemma
				Lemma lemma = new Lemma(doc.getJCas(), token.getBegin(), token.getEnd());
				lemma.setValue(word[LEMMA]);
				lemma.addToIndexes();
				token.setLemma(lemma);

				// process part-of-speech tag
				POS pos = new POS(doc.getJCas(), token.getBegin(), token.getEnd());
				pos.setPosValue(word[POS]);
				pos.addToIndexes();
				token.setPos(pos);

				// process high-level DKPro types
				// s.a. https://code.google.com/p/dkpro-core-asl/wiki/ResourceProviderAPI
				if(word[POS].equals("NN") || word[POS].equals("NNS")) {
					NN nn = new NN(doc.getJCas(), token.getBegin(), token.getEnd());
					nn.setPosValue(word[POS]);
					nn.addToIndexes();
				}

				token.addToIndexes();
				lemma.addToIndexes();
				pos.addToIndexes();

				sentenceEnd = token.getEnd();
			}

			// process dependencies
			for(String[] word : words) {

				int depId = Integer.valueOf(word[ID]);
				int govId = Integer.valueOf(word[HEAD]);

//				// model the root as a loop onto itself
//				if (govId == 0) {
//					//govId = depId;
//				}

				// don't model ROOT as a loop, would cause loops in later graphs
 				if(word[DEP].equals("ROOT")) {
					continue;
				}

				Dependency dep = new Dependency(doc.getJCas());
				dep.setGovernor(tokens.get(govId));
				dep.setDependent(tokens.get(depId));
				dep.setDependencyType(word[DEP]);
				dep.setBegin(dep.getDependent().getBegin());
				dep.setEnd(dep.getDependent().getEnd());
				//dep.setBegin(Math.min(dep.getDependent().getBegin(), dep.getGovernor().getBegin()));
				//dep.setEnd(Math.max(dep.getDependent().getEnd(), dep.getGovernor().getEnd()));
				dep.addToIndexes();
			}

			// process sentence
			Sentence sentence = new Sentence(doc.getJCas(), sentenceBegin, sentenceEnd);
			sentence.addToIndexes();

			//// make it one sentence per line
			//doc.add("\n");

			//doc.close(); // throws an exception: "org.apache.uima.cas.CASRuntimeException: Data for Sofa feature setLocalSofaData() has already been set."

			return uimaXMLConverterHelper.serialize(doc.getJCas());
			//return uimaXMLConverterHelper.serialize(jCas) // ??

		} catch (SAXException e) {
			e.printStackTrace();
		}
		return null;
	}

	private List<String[]> readSentence(String conllSentence) throws IOException {

		List<String[]> words = new ArrayList<String[]>();
		String[] conllLines = conllSentence.split( "\t:::::\t" );

		for(String conllLine : conllLines) {

			String[] fields = conllLine.split("\t");
			if (fields.length != 6) {
				//throw new IOException("Invalid file format: Every word must have 6 tab-separted fields.");
				return null;
			}
			words.add(fields);
		}
		return words;
	}
}