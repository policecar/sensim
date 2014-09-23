package sensim;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.N;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NN;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NP;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.*;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.UndirectedSubgraph;
import org.xml.sax.SAXException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.BronKerboschCliqueFinder;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.graph.SimpleGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dima.UIMAXMLConverterHelper;

/**
 * Date: 4/10/13
 * Time: 3:35 PM
 *
 * @author Priska Herger, Johannes Kirschnick
 *         <p/>
 *         Description: A Pig script that takes a JCas as input and extracts
 *         its annotations in terms of the shortest path between
 *         all recognized entities.
 */

@OutputSchema("features:bag {datum:tuple (noun1:chararray, noun2:chararray, feature:chararray, sentence:chararray)}")
public class FeatureExtractor extends EvalFunc<DataBag> {

	private final JCas jCas;

	private UIMAXMLConverterHelper uimaXMLConverterHelper;
	private BagFactory bagFactory = BagFactory.getInstance();
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	private int numMaxHops = 5;
	private int numSkipsInSubtree = 0;

	enum SelectionType {
		NOUN, COMMONNOUN, PROPERNOUN, NAMEDENTITY
	}

	private final Class<? extends Annotation> selectionType;

	enum Counters {
		FEATURES
	}

	public FeatureExtractor() throws UIMAException {

		this("NOUN");
	}

	public FeatureExtractor(String selectionType) throws UIMAException {

		uimaXMLConverterHelper = new UIMAXMLConverterHelper(false);
		jCas = JCasFactory.createJCas();

		switch (SelectionType.valueOf(selectionType)) {

			case NOUN:
				this.selectionType = N.class;
				break;
			case COMMONNOUN:
				this.selectionType = NN.class;
				break;
			case PROPERNOUN:
				this.selectionType = NP.class;
				break;
			case NAMEDENTITY:
				this.selectionType = NamedEntity.class;
				break;
			default:
				this.selectionType = N.class;
		}
	}

	public FeatureExtractor(String selectionType, String numMaxHops, String numSkipsInSubtree)
			throws UIMAException {

		this(selectionType);
		this.numMaxHops = Integer.parseInt(numMaxHops);
		this.numSkipsInSubtree = Integer.parseInt(numSkipsInSubtree);
	}

	@Override
	public DataBag exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}

		DataBag patternBag = bagFactory.newDefaultBag();

		try {

			CharSequence charseq = (CharSequence) input.get(0);
			InputStream stream = IOUtils.toInputStream(charseq, Charsets.UTF_8.name());
			// note that jCas is changed in deserialize(...) and contains different data upon return!
			// design decision in favor of speed at the expense of readability
			uimaXMLConverterHelper.deserialize(stream, jCas);

			// for every sentence in jCas, do
			Iterator<Sentence> sentences = JCasUtil.iterator(jCas, Sentence.class);

			while (sentences.hasNext()) {

				Sentence sentence = sentences.next();

				// insert dependencies into a graph and extract shortest path
				UndirectedGraph<Token, DependencyEdge> graph = makeDependencyGraph(sentence);
				if (graph == null) {
					return null;
				}

				if (input.size() == 3) {

					ArrayList<String> nounPair = Lists.newArrayList();
					nounPair.add((String) input.get(1));
					nounPair.add((String) input.get(2));

					patternBag.addAll(getAllSubtrees(sentence, nounPair, graph));

				}
				else {
					return null;
				}
			}

			// emit some stats
			PigStatusReporter pigStatusReporter = PigStatusReporter.getInstance();
			if (pigStatusReporter != null) {
				pigStatusReporter.getCounter(Counters.FEATURES).increment(patternBag.size());
			}

		} catch (UIMAException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		return patternBag;
	}

	/**
	 * Inserts all dependencies of an annotated sentence into a graph
	 * with governor and dependent as vertices and dependency type as
	 * edge.
	 *
	 * @return UndirectedGraph of dependencies
	 */
	public UndirectedGraph<Token, DependencyEdge> makeDependencyGraph(Sentence sentence) {

		UndirectedGraph<Token, DependencyEdge> graph = new SimpleGraph<Token, DependencyEdge>(DependencyEdge.class);

		// skip sentences with less than two words of the selected type
		List<? extends Annotation> nouns = JCasUtil.selectCovered(selectionType, sentence);
		if(nouns.size() < 2) {
			return null;
		}

		List<Dependency> dependencies = JCasUtil.selectCovered(Dependency.class, sentence);

		for (Dependency dependency : dependencies) {

			Token governor = dependency.getGovernor();
			Token dependent = dependency.getDependent();
			String dependencyType = dependency.getDependencyType();

			if(governor != null & dependent != null) { //TODO: why would it be null !?
				graph.addVertex(governor);
				graph.addVertex(dependent);
				graph.addEdge(governor, dependent, new DependencyEdge(governor, dependent, dependencyType));
			}
		}
		return graph;
	}

	/**
	 * Extract all subtrees involving the specified word pair up to a certain length as features.
	 *
	 * @param sentence
	 * @param wordPair
	 * @param graph
	 * @return dataBag
	 */
	public DataBag getAllSubtrees(Sentence sentence, ArrayList<String> wordPair, final UndirectedGraph<Token, DependencyEdge> graph) {

		DataBag dataBag = bagFactory.newDefaultBag();

		// make each word in the pair a Token
		List<Token> tokens = JCasUtil.selectCovered(Token.class, sentence);
		List<Token> pair = Lists.newArrayListWithExpectedSize(wordPair.size());
		for (Token token : tokens) {
			// if (token.getCoveredText().equals(nounPair.get(0)) || token.getCoveredText().equals(nounPair.get(1))) {
			if (token.getLemma().getValue().equals(wordPair.get(0)) || token.getLemma().getValue().equals(wordPair.get(1))) {
				pair.add(token);
			}
		}
		if (pair.size() != wordPair.size()) {
			return dataBag;
		}

		Token w1 = pair.get(0);
		Token w2 = pair.get(1);

		if (w1 == null || w2 == null) {
			return dataBag;
		}

		// get powerset of tokens
		Set<Token> tokenSet = new HashSet<Token>(tokens);

		// Beware: magic number! dictacted by the powerSet function which only works for N <= 30
		if (tokenSet.size() > 30) {
			return dataBag;
		}

		Set<Set<Token>> tokenPowerSet = Sets.powerSet(tokenSet);

		// make a subgraph for each set in the powerset of tokens
		for (Set<Token> tSet : tokenPowerSet) {
			try {

				// if tSet contains more than specified max number of tokens, continue
				if (tSet.size() < 3 || tSet.size() > this.numMaxHops) {
					continue;
				}

				// if tSet doesn't contain w1 _and_ w2 continue
				if (!tSet.contains(w1) || !tSet.contains(w2)) {
					continue;
				}

				// make subgraph
				UndirectedSubgraph<Token, DependencyEdge> subgraph =
						new UndirectedSubgraph<Token, DependencyEdge>(graph, tSet, graph.edgeSet());

				// if the subgraph is not connected, dismiss this set
				ConnectivityInspector<Token, DependencyEdge> inspector =
						new ConnectivityInspector<Token, DependencyEdge>(subgraph);
				if (!inspector.isGraphConnected()) {
					continue;
				}

				Set<DependencyEdge> edgeSet = subgraph.edgeSet();
				Set<Token> vertexSet = Sets.newHashSet();
				// subgraph.vertexSet() is unmodifiable but sometimes an 'and' Token will be added manually,
				// hence the following copy action
				for(Token tkn : subgraph.vertexSet()) {
					vertexSet.add(tkn);
				}

				// if edgeSet contains the dependency 'conj', manually add the 'and' Token to vertexSet
				for(DependencyEdge edge : edgeSet) {
					if(edge.dependency.equals("conj")) {
						List<Token> tmpTokens = JCasUtil.selectBetween(jCas, Token.class, edge.from, edge.to);
						for(Token tkn : tmpTokens) {
							if(tkn.getCoveredText().equals("and")) {
								vertexSet.add(tkn);
							}
						}
					}
				}

				if(vertexSet.size() < 3) {
					continue;
				}

					// if subgraph contains noun, dismiss it
					boolean skipThisSet = false;
					int smallerBegin = (w1.getBegin() < w2.getBegin()) ? w1.getBegin() : w2.getBegin();

					for(Token vertex : vertexSet) {

						// if this set contains nouns other than w1 and w2, skip it
						if(vertex.getPos().getPosValue().startsWith("N") && vertex != w1 && vertex != w2) {
							skipThisSet = true;
						}
						// if any token is a fullstop, skip the set ( because the same pattern will appear without )
						//TODO: would suffice to check on the last token
						if(vertex.getCoveredText().equals(".")) {
							skipThisSet = true;
						}
					}
					if(skipThisSet) {
						continue;
					}

				// generate patterns
				// join all vertices in correct (linear) positional order
				SortedMap<Integer, Token> vertexMap = new TreeMap<Integer, Token>();
				for(Token vertex : vertexSet) {
					vertexMap.put(vertex.getBegin(), vertex);
				}
				List<Token> vertexList = Lists.newArrayListWithExpectedSize(vertexSet.size());
				int i = 0;
				for(Integer pos : vertexMap.keySet()) { // Note: is sorted ascendingly by key set
					vertexList.add(i, vertexMap.get(pos));
					i += 1;
				}

				// join lower-cased covered text on space
				String pattern = Joiner.on(" ").join(Iterables.transform(vertexList, new Function<Token, String>() {
					@Override
					public String apply(Token token) {
						return token.getCoveredText().replaceAll("\n", " ").toLowerCase();
					}
				}));

				// use regular expressions to include word boundaries in match; important in particular with very
				// short w1 and w2, e.g. 's'
				Pattern regex1 = Pattern.compile("\\b" + w1.getCoveredText().toLowerCase() + "\\b");
				Pattern regex2 = Pattern.compile("\\b" + w2.getCoveredText().toLowerCase() + "\\b");

				Matcher matcher1 = regex1.matcher(pattern);
				int idx1 = matcher1.find() ? matcher1.start() : -1;

				Matcher matcher2 = regex2.matcher(pattern);
				int idx2 = matcher2.find() ? matcher2.start() : -1;

				// figure out their order here as well, s. shortest path method
				boolean assumedOrder;
				if(idx1 < idx2) {
					assumedOrder = true;
				} else {
					assumedOrder = false;
				}

				// replace w1 with X and w2 with Y
				Matcher matcher = regex1.matcher(pattern);
				pattern = matcher.replaceAll("X");
				matcher = regex2.matcher(pattern);
				pattern = matcher.replaceAll("Y");

				//TODO: generate patterns with skips

				Tuple tuple = tupleFactory.newTuple(4);
				// return nouns in order observed in sentence rather than incoming order
				tuple.set(0, (assumedOrder ? pair.get(0).getLemma().getValue() : pair.get(1).getLemma().getValue()));
				tuple.set(1, (assumedOrder ? pair.get(1).getLemma().getValue() : pair.get(0).getLemma().getValue()));
				tuple.set(2, pattern.trim());
				//String sentence = JCasUtil.selectCovering(jCas, Sentence.class, w1.getBegin(),
				//		w2.getEnd()).get(0).getCoveredText();
				//String sentence = jCas.getDocumentText();
				tuple.set(3, sentence.getCoveredText());
				dataBag.add(tuple);

			} catch (ExecException e) {
				e.printStackTrace();
			}
		}
		return dataBag;
	}

	/**
	 * Extract the shortest path along the dependency parse
	 * between the two provided nouns.
	 *
	 * @param nounPair
	 * @param graph
	 * @return DataBag dataBag     	a data bag of ( entity_pair, pattern ) tuples
	 *         					 	with the shortest path as pattern
	 */
	public DataBag getShortestPath(ArrayList<String> nounPair, final UndirectedGraph<Token, DependencyEdge> graph)
			throws ExecException {

		DataBag dataBag = bagFactory.newDefaultBag();

		ArrayList<Token> tokens = Lists.newArrayList(JCasUtil.select(jCas, Token.class));
		List<Token> pair = Lists.newArrayListWithExpectedSize(nounPair.size());
		for (Token token : tokens) {
			//TODO: FIXME: matching by use of strings is unreliable, e.g., wrt multiple occurrence
			if (token.getLemma().getValue().equals(nounPair.get(0)) || token.getLemma().getValue().equals(nounPair.get(1))) {
				pair.add(token);
			}
		}
		if (pair.size() != nounPair.size()) {
			return dataBag;
		}

		FloydWarshallShortestPaths shortestPaths =
				new FloydWarshallShortestPaths<Token, DependencyEdge>(graph);
		if (shortestPaths == null) {
			return dataBag;
		}

		// extract the shortest path between the two nouns
		int shortestPathsCount = shortestPaths.getShortestPathsCount();

		Token n1 = pair.get(0);
		Token n2 = pair.get(1);

		try {

			// if e1 or e2 is NULL we might not have information about the dependencies in the sentence
			// which contains the NER
			// this is just an error as we are searching for links in the whole document, not just sentence wise
			if(n1 == null || n2 == null) {
				return dataBag;
			}

			GraphPath<Token, DependencyEdge> shortestPath = shortestPaths.getShortestPath(n1, n2);
			if ((shortestPathsCount == 0) || (shortestPath == null)) {
				return dataBag;
			}

			// retrieve vertices on the shortest path
			List<Token> vertexPath = Graphs.getPathVertexList(shortestPath);
			// the first and last entry are the start and end vertex -> skip them
			List<Token> vertices = vertexPath.subList(1, vertexPath.size() - 1);
			String vertexLabels = Joiner.on(" ").join(Iterables.transform(vertices, new Function<Token, String>() {
				@Override
				public String apply(Token token) {
					return token.getCoveredText().replaceAll("\n", " ").toLowerCase();
				}
			}));

			// retrieve edges /dependencies on the shortest path
			List<DependencyEdge> edges = shortestPath.getEdgeList();
			String edgeLabels = Joiner.on(",").join(Iterables.transform(edges, new Function<DependencyEdge, String>() {
				@Override
				public String apply(DependencyEdge edge) {
					return edge.dependency;
				}
			}));

			// attempt to include preceding prepositions and ensuing ccs
			Iterator<Sentence> sentences = JCasUtil.iterator(jCas, Sentence.class);
			String prepender = "";
			String postpender = "";
			while (sentences.hasNext()) {
				Sentence sentence = sentences.next();
				List<Dependency> dependencies = JCasUtil.selectCovered(Dependency.class, sentence);

				for (Dependency dependency : dependencies) {

					Token governor = dependency.getGovernor();
					Token dependent = dependency.getDependent();
					String dependencyType = dependency.getDependencyType();

					// include preceding pobj ( hopefully prepositions / IN mostly )
					if (dependencyType.equals("pobj") && dependent.equals(n1)) {
						// if dependencyType is in shortest path already, ignore it here
						if (!shortestPath.getEdgeList().contains(new DependencyEdge(governor, dependent, dependencyType))) {
							prepender = governor.getCoveredText() + " ";
						}
					}
					// include actual cc string in case of conj dependency
					if (dependencyType.equals("cc") && governor.equals(n1)) {
						postpender = dependent.getCoveredText();
					}
				}
			}

			// skip patterns where both vertexLabels and postpender are empty
			if (vertexLabels.isEmpty() && postpender.isEmpty()) {
				return dataBag;
			}
			else if (!vertexLabels.isEmpty() && !postpender.isEmpty()) {
				postpender += " ";
			}

			// concatenate pattern parts
			final String pattern;
			final boolean normalOrder;
			// now we need to find the order of the entities
			if(pair.get(0).getBegin() < pair.get(1).getBegin()) {
				// X -> Y
				pattern = prepender + "X " + postpender + vertexLabels + " Y"; //[" + edgeLabels + "]";
				normalOrder = true;
			} else {
				// Y -> X
				//TODO: add pre- and postpenders here, too ?
				pattern = "Y " + vertexLabels + " X"; //[" + edgeLabels + "]";
				normalOrder = false;
			}

			Tuple tuple = tupleFactory.newTuple(4);

			// output: first noun \t second noun \t pattern \t original sentence
			tuple.set(0, (normalOrder ? pair.get(0) : pair.get(1)).getLemma().getValue());
			tuple.set(1, (normalOrder ? pair.get(1) : pair.get(0)).getLemma().getValue());
			tuple.set(2, pattern);
			String coveredText = JCasUtil.selectCovering(jCas, Sentence.class, n1.getBegin(),
					n1.getEnd()).get(0).getCoveredText();
			tuple.set(3, coveredText);
			dataBag.add(tuple);

		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}

		return dataBag;
	}

	/**
	 *
	 * Extract the k shortest paths between two given words along the dependency parse of a sentence.
	 * @param nounPair
	 * @param graph
	 * @return DataBag				a data bag of tuples of length 4 containing the word pair, the extracted pattern,
	 * 								and the underlying sentence.
	 * @throws ExecException
	 */
	public DataBag getKShortestPathes(ArrayList<String> nounPair, final UndirectedGraph<Token, DependencyEdge> graph)
		throws ExecException {

		DataBag dataBag = bagFactory.newDefaultBag();

		// get tokens for each of the two words
		ArrayList<Token> tokens = Lists.newArrayList(JCasUtil.select(jCas, Token.class));
		List<Token> pair = Lists.newArrayListWithExpectedSize(nounPair.size());
		for (Token token : tokens) {
			if (token.getLemma().getValue().equals(nounPair.get(0)) ||
					token.getLemma().getValue().equals(nounPair.get(1))) {
				pair.add(token);
			}
		}
		if (pair.size() != nounPair.size()) {
			return dataBag;
		}

		// for each word get its k shortest pathes to other words
		KShortestPaths kShortestPaths = new KShortestPaths(graph, pair.get(0), this.numSkipsInSubtree, this.numMaxHops);

		if (kShortestPaths == null) {
			return dataBag;
		}

		Token w1 = pair.get(0);
		Token w2 = pair.get(1);

		if(w1 == null || w2 == null) {
			return dataBag;
		}

		//TODO: finish writing this feature variant

		return dataBag;
	}

	/**
	 *
	 * Find all cliques in the given graph < well, untractable and such.
	 *
	 * @param graph             a graph
	 * @return DataBag dataBag  a data bag of tuples
	 */
	public DataBag findAllMaximalCliques(ArrayList<String> wordPair, final UndirectedGraph<Token, DependencyEdge> graph) throws ExecException {

		DataBag dataBag = bagFactory.newDefaultBag();

		// get tokens for each of the two words and check if they're contained in the sentence
		ArrayList<Token> tokens = Lists.newArrayList(JCasUtil.select(jCas, Token.class));
		List<Token> pair = Lists.newArrayListWithExpectedSize(wordPair.size());
		for (Token token : tokens) {
			if (token.getLemma().getValue().equals(wordPair.get(0)) ||
					token.getLemma().getValue().equals(wordPair.get(1))) {
				pair.add(token);
			}
		}
		if (pair.size() != wordPair.size()) {
			return dataBag;
		}

		BronKerboschCliqueFinder cliqueFinder = new BronKerboschCliqueFinder(graph);
		Collection cliques= cliqueFinder.getAllMaximalCliques();

		if (cliques == null || cliques.size() == 0) {
			return dataBag;
		}

		Token w1 = pair.get(0);
		Token w2 = pair.get(1);

		Iterator iterator = cliques.iterator();
		while (iterator.hasNext()) {

			HashSet<Token> hset = (HashSet<Token>) iterator.next();

			Iterator it2 = hset.iterator();
			Token source = (Token)it2.next();
			Token target = (Token)it2.next();

			ArrayList<Token> coveredTokens = (ArrayList<Token>) JCasUtil.selectCovered(jCas, Token.class,
					source.getBegin(), target.getEnd());

			ArrayList<String> pattern = Lists.newArrayList();
			ArrayList<String> pattern_variant = Lists.newArrayList();

			boolean foundW1 = false;
			boolean foundW2 = false;

			for(Token token : coveredTokens) {

				// check if both words are contained
				if(!foundW1 && token.equals(w1)) {
					foundW1 = true;
					pattern.add("X");
					pattern_variant.add("X");
				}
				else if(!foundW2 && token.equals(w2)) {
					foundW2 = true;
					pattern.add("Y");
					pattern_variant.add("Y");
				}
				else if(token.getPos().getPosValue().equals("JJ")) {
					pattern.add(token.getCoveredText().toLowerCase());
					pattern_variant.add("<adj>");
				}
				else if(token.getPos().getPosValue().startsWith("N")) {
					pattern.add(token.getCoveredText().toLowerCase());
					pattern_variant.add("<noun>");
				}
				else {
					pattern.add(token.getCoveredText().toLowerCase());
					pattern_variant.add(token.getCoveredText().toLowerCase());
				}
			}

			if(pattern.size() == 0 || !(foundW1 && foundW2)) {
				return dataBag;
			}

			Tuple tuple = tupleFactory.newTuple(4);
			tuple.set(0, w1.getLemma().getValue());
			tuple.set(1, w2.getLemma().getValue());
			tuple.set(2, StringUtils.join(pattern, " "));
			String sentence = JCasUtil.selectCovering(jCas, Sentence.class, source.getBegin(),
					target.getEnd()).get(0).getCoveredText();
			tuple.set(3, sentence);
			dataBag.add(tuple);

			// add a more abstracted version of the pattern, too, but only for short patterns, else long patterns get
			// unnecessary frequencies from abstraction
			if(coveredTokens.size() < 7) {
				Tuple tuple2 = tupleFactory.newTuple(4);
				tuple2.set(0, w1.getLemma().getValue());
				tuple2.set(1, w2.getLemma().getValue());
				tuple2.set(2, StringUtils.join(pattern_variant, " "));
				tuple2.set(3, sentence);
				dataBag.add(tuple2);
			}

			// add a another abstracted version of the pattern
			if(coveredTokens.size() < 10) {
				Tuple tuple3 = tupleFactory.newTuple(4);
				tuple3.set(0, w1.getLemma().getValue());
				tuple3.set(1, w2.getLemma().getValue());
				tuple3.set(2, StringUtils.join(pattern_variant, " ").replace("<noun>","").replace("<adj>",""));
				tuple3.set(3, sentence);
				dataBag.add(tuple3);
			}
		}
		return dataBag;
	}

	/**
	 * Extract the head of a named entity - for now pretend the head
	 * is the token with the highest degree in the dependency graph.
	 *
	 * @param entity 			the annotation (named entity, compound noun) whose head to get
	 * @param graph       		the dependency graph
	 * @return String head      the head of the annotation
	 */
	public Token getEntityHead(Annotation entity, UndirectedGraph<Token, DependencyEdge> graph) {

		// get all tokens
		List<Token> tokens = JCasUtil.selectCovered(Token.class, entity);
		Token head = null;
		int degree = -1;
		for (Token token : tokens) {
			if (graph.containsVertex(token)) {
				int tmp = graph.degreeOf(token);
				if (tmp >= degree) {
					degree = tmp;
					head = token;
				}
			}
		}
		return head;
	}

/*	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

		List<FuncSpec> funcList = new ArrayList<FuncSpec>();

		Schema tupleSchema = new Schema();
		tupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

		funcList.add(new FuncSpec(this.getClass().getName(), tupleSchema));

		return funcList;
	}
*/

	private static class DependencyEdge {
		private Token from;
		private Token to;
		private String dependency;

		private DependencyEdge(Token from, Token to, String dependency) {
			this.from = from;
			this.to = to;
			this.dependency = dependency;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			DependencyEdge that = (DependencyEdge) o;

			if (dependency != null ? !dependency.equals(that.dependency) : that.dependency != null) return false;
			if (from != null ? !from.equals(that.from) : that.from != null) return false;
			if (to != null ? !to.equals(that.to) : that.to != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = from != null ? from.hashCode() : 0;
			result = 31 * result + (to != null ? to.hashCode() : 0);
			result = 31 * result + (dependency != null ? dependency.hashCode() : 0);
			return result;
		}
	}
}
