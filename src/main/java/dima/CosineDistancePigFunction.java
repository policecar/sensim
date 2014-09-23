package dima;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.commons.cli.ParseException;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

import java.io.IOException;
import java.util.List;

/**
 * Date: 20.03.13
 * Time: 21:24
 *
 * @author Johannes Kirschnick
 */
@OutputSchema("distance:double")
public class CosineDistancePigFunction extends EvalFunc<Double> {

    private final MahoutVectorConverter vectorConverter;
    CosineDistanceMeasure cosineDistanceMeasure = new CosineDistanceMeasure();

    boolean skip = false;

    @Parameter(names = {"-skipValue"}, description = "Skip values which are equal to X", required = false)
    Double skipValue;

    @Parameter(names = {"-offset"}, description = "Subtract this from resulting distance function (before skip evaluation)", required = false)
    double offset = 0;


    public CosineDistancePigFunction() {
        this(""); // no options
    }

    public CosineDistancePigFunction(String options) {
        JCommander jCommander = new JCommander(this);

        try {
            // parse options
            jCommander.parse(options.split(" "));
            skip = (skipValue != null);
            vectorConverter = new MahoutVectorConverter();
        } catch (ParameterException e) {
            StringBuilder out = new StringBuilder();
            jCommander.setProgramName(this.getClass().getSimpleName());
            jCommander.usage(out);
            // We wrap this exception in a Runtime exception so that
            // existing loaders that extend PigStorage don't break
            throw new RuntimeException(e.getMessage() + "\n" + "In: " + options + "\n" + out.toString());
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }

    }

    @Override
    public Double exec(Tuple input) throws IOException {
        // we want something like this
        // (cardinality: int, entries: {entry: (index: int, value: double)})
        // for each vector
        Preconditions.checkArgument(input.size() == 2, "We need 2 arguments, not " + input.size());

        Tuple tuple1 = (Tuple) input.get(0);
        Integer sizeV1 = (Integer) tuple1.get(0);
        Tuple tuple2 = (Tuple) input.get(1);
        Integer sizeV2 = (Integer) tuple2.get(0);

        Preconditions.checkArgument(Ints.compare(sizeV1, sizeV2) == 0,
                "Vector sizes are different " + sizeV1 + " != " + sizeV1);

        Vector vector = vectorConverter.toVector(tuple1);
        Vector vector2 = vectorConverter.toVector(tuple2);

        double distance = cosineDistanceMeasure.distance(vector, vector2) + offset;
        // shortcut evaluation should prevent NPE
        if(skip && distance == skipValue) {
            // ignore
            return null;
        }
        return distance;
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

        return super.getArgToFuncMapping();
    }
}
