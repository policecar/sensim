package sensim;

import com.google.common.base.Charsets;
import dima.UIMAXMLConverterHelper;
import org.apache.commons.io.IOUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Date: 4/12/14
 * Time: 11:35 PM
 *
 * @author Priska Herger
 */
@OutputSchema("sentence:tuple (pair:chararray, sentence:chararray)")
public class PairSentenceSelector extends EvalFunc<Tuple> {


	private final JCas jCas;

	private UIMAXMLConverterHelper uimaXMLConverterHelper;
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	public PairSentenceSelector() throws UIMAException {

		super();

		jCas = JCasFactory.createJCas();
		uimaXMLConverterHelper = new UIMAXMLConverterHelper(false);
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		try {

			Tuple pair = (Tuple) input.get(0);
			CharSequence charseq = (CharSequence) input.get(1);
			InputStream stream = IOUtils.toInputStream(charseq, Charsets.UTF_8.name());
			// note that jCas is changed in deserialize(...) and contains different data upon return!
			// design decision in favor of speed at the expense of readability
			uimaXMLConverterHelper.deserialize(stream, jCas);

			String sentence = jCas.getDocumentText();

			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, pair);
			tuple.set(1, sentence);

			return tuple;

		} catch (UIMAException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
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
