package dima;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XMLSerializer;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * XMI Cas serializer / de-serializer Helper.
 * <p/>
 * Date: 18.02.13
 * Time: 14:47
 *
 * @author Johannes Kirschnick
 */
public class UIMAXMLConverterHelper extends UIMAConverterHelper<String> {

	private static final Log LOG = LogFactory.getLog(UIMAXMLConverterHelper.class);


	private final XCASSerializer ser;
	private final boolean formattedOutput;

	/**
	 * Creates a new instance of the XML serializer.
	 *
	 * @param formattedOutput if true the output will be XML pretty printed, otherwise it will be just on one line
	 * @throws UIMAException in case of errors
	 */
	public UIMAXMLConverterHelper(boolean formattedOutput) throws UIMAException {
		this.formattedOutput = formattedOutput;
		JCas jCas = JCasFactory.createJCas();
		ser = new XCASSerializer(jCas.getTypeSystem());
	}

	@Override
	public String serialize(JCas jCas) throws IOException, SAXException {

		StringWriter writer = new StringWriter();
		XMLSerializer xmlSer = new XMLSerializer(writer, formattedOutput);
		ser.serialize(jCas.getCas(), xmlSer.getContentHandler());
		return writer.toString();
	}

	@Override
	public JCas deserialize(InputStream inputStream, JCas newElement) throws IOException, UIMAException, InterruptedException, SAXException {

		try {
			newElement.reset();
			// deserialize CAS
			XCASDeserializer.deserialize(inputStream, newElement.getCas());
			//XmiCasDeserializer.deserialize(inputStream, newElement.getCas());
			return newElement;
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
	}
}
