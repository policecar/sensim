package dima;

import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Date: 30.09.13
 * Time: 17:30
 *
 * @author Johannes Kirschnick
 * adapted from http://svn.apache.org/repos/asf/uima/uimaj/tags/uimaj-2.3.0/uimaj-2.3.0-07/uimaj-core/src/main/java/org/apache/uima/util/TCasToInlineXml.java
 */
public abstract class UIMAConverterHelper<T> {

	public abstract T serialize(JCas jCas) throws IOException, SAXException;

	public abstract JCas deserialize(InputStream inputStream, JCas newElement) throws IOException, UIMAException, InterruptedException, SAXException, ClassNotFoundException;

	public static String sanitizeString(String input) {
		// sanitize the content
		// get document text
		if(input != null) {
			char[] docCharArray = input.toCharArray();
			replaceInvalidXmlChars(docCharArray);
			return String.valueOf(docCharArray);
		}
		return input;
	}


	private static void replaceInvalidXmlChars(char[] aChars) {
		for (int i = 0; i < aChars.length; i++) {
			if ((aChars[i] < 0x20 && aChars[i] != 0x09 && aChars[i] != 0x0A && aChars[i] != 0x0D)
					|| (aChars[i] > 0xD7FF && aChars[i] < 0xE000) || aChars[i] == 0xFFFE
					|| aChars[i] == 0xFFFF) {
				// System.out.println("Found invalid XML character: " + (int)aChars[i] + " at position " +
				// i); //temp
				aChars[i] = ' ';
			}
		}
	}

	public JCas deserialize(String input, JCas newElement) throws IOException, UIMAException, InterruptedException, SAXException, ClassNotFoundException {
		return deserialize(IOUtils.toInputStream(input, Charsets.UTF_8.name()), newElement);
	}
}
