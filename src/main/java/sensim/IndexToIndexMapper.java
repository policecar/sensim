package sensim;


import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

@OutputSchema("index:long")
public class IndexToIndexMapper extends EvalFunc<Integer> {

	private final File afile;
	private Map<Integer, Integer> idxMap;

	public IndexToIndexMapper( String pathToIdxMap ) {

		afile = new File( pathToIdxMap ) ;
	}

	@Override
	public Integer exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null ;
		}
		
		// check if idxMap exists, else instantiate it
		// Note: can't instantiate it in Constructor because afile doesn't exist 
		// yet, s. make_feature_vectors.pig
		if ( idxMap == null ) {
			// Note: a Scanner tokenizes input stream by whitespace (per default)
			Scanner sc = new Scanner( afile ) ;
			idxMap = new HashMap<Integer, Integer>() ;
			while ( sc.hasNext() ) {
				// pick pairs of tokens
				idxMap.put(sc.nextInt(), sc.nextInt()) ;
			}
			sc.close() ;

		}

		int id = (Integer)input.get(0) ;
		int idx ;
		if ( idxMap.containsKey( id )) {
			
			// Note: HashMap.get( key ) returns value
			idx = idxMap.get( id ) ;

		} else {
			return null ;
		}
		return idx;
	}
	
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

		return super.getArgToFuncMapping();
	}
}
