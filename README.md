## sensim

Learning semantic relations with distributional similarity.

### Contents 

* A natural language processing pipeline 
  based on [DKPro Core](http://www.ukp.tu-darmstadt.de/software/dkpro-core/)
  which utilizes [Pig](http://pig.apache.org/) for local or 
  [Hadoop](http://hadoop.apache.org/)-based execution.  

*  Annotation: segmentation, part-of-speech tagging, lemmatization, and dependency parsing based on [Stanford NLP](http://nlp.stanford.edu/software/index.shtml). Any of these components can be conveniently replaced with alternative implementations and models, e.g., the Stanford Parser with the [Berkeley parser](http://nlp.cs.berkeley.edu/Software.shtml) or a PCFG model with an RNN parser. 
  [This list](https://zoidberg.ukp.informatik.tu-darmstadt.de/artifactory/public-model-releases-local/de/tudarmstadt/ukp/dkpro/core/) provides an overview of available models.

* Feature extraction: all subtrees along a dependency parse that involve two tokens of a specific type are extracted as features. The type of token is specified generically – implemented options are common nouns, proper nouns, and named entities, but other types of tokens can be added easily. Features are weighed using the Lexicographer's mutual information[1].

* Classification with logistic regression as implemented in [scikit-learn](http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html); see [simsets](https://github.com/policecar/simsets) for details.

* Clustering with [Chinese Whispers](http://wortschatz.informatik.uni-leipzig.de/~cbiemann/software/CW.html). Extrinsic cluster evaluation with the B-Cubed measure[2], see the [python module](https://github.com/policecar/sensim/tree/master/src/main/python) and in particular [clustering_utils](https://github.com/policecar/sensim/blob/master/src/main/python/clustering_utils.py), [evaluate_cw_clustering](https://github.com/policecar/sensim/blob/master/src/main/python/evaluate_cw_clustering.py).

* Preliminary data analysis: [Root](http://root.cern.ch/) code to compute a variety of histograms and distance measures on the 
  feature vectors for preliminary evaluation of potentially useful clustering approaches 
  – [link](https://github.com/policecar/sensim/tree/master/src/main/root) to module. (Thanks to [jkerdels](https://github.com/jkerdels) for the introduction and most of the code!)


#### Run it on a hadoop cluster in mapreduce mode

```shell
cd sensim
mvn package -Dmaven.test.skip=true -Phadoop-job
cd src/main/pig/
pig -P <propertyfile> -m <parameterfile> pipeline.pig &> <logfile>
```

#### Run locally using Pig directly

```shell
# as above but substitute last line with
pig -x local -P properties -m parameters pipeline.pig
```

#### Run locally using a JUnit test

Import this Maven project into the IDE of your choice and run 
the method testCoreNLPAnnotator() in CoreNLPAnnotatorTest.java.

#### References

[1] http://wortschatz.uni-leipzig.de/~sbordag/papers/BordagMC08.pdf  
[2] http://pdf.aminer.org/000/019/970/entity_based_cross_document_coreferencing_using_the_vector_space_model.pdf

