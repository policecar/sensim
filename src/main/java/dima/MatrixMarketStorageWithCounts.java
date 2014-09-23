package dima;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.*;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.codehaus.jackson.map.util.LRUMap;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/**
 * Date: 27.03.13
 * Time: 15:34
 *
 * @author Johannes Kirschnick
 * @author Alan Akbik
 */
public class MatrixMarketStorageWithCounts extends PigStorage {

    private static final Log log = LogFactory.getLog(MatrixMarketStorageWithCounts.class);

    public static final String MATRIX_MARKET_MATRIX_HEADER = "%%MatrixMarket matrix coordinate real symmetric";
    TupleFactory tupleFactory = TupleFactory.getInstance();

    // Date in ISO 8601 Format
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
    Calendar cal = Calendar.getInstance();

    /**
     * Indicates that we have already written out the header.
     */
    private boolean storingFirstRecord = true;

    public MatrixMarketStorageWithCounts() {
        super(" ");
    }

    @Override
    public void putNext(Tuple f) throws IOException {
        if (storingFirstRecord) {
            // write out header information
            super.putNext(tupleFactory.newTuple(MATRIX_MARKET_MATRIX_HEADER));
            super.putNext(tupleFactory.newTuple("% MatrixMarket writer, see http://math.nist.gov/MatrixMarket/formats.html"));
            super.putNext(tupleFactory.newTuple("% Generated on " + df.format(cal.getTime())));
            super.putNext(tupleFactory.newTuple("% This ASCII file represents a sparse MxN matrix with L nonzeros"));
            super.putNext(tupleFactory.newTuple("%  M  N  L | <--- rows, columns, entries"));

            // we assume it's a sparse matrix
            // This ASCII file represents a sparse MxN matrix with L nonzeros

            // we assume the following schema
            // row:int, column:int, distance:double, MxN, global

            super.putNext(tupleFactory.newTuple(Joiner.on(" ").skipNulls().join(f.get(4), f.get(4), f.get(5))));
            storingFirstRecord = false;
        }
        super.putNext(tupleFactory.newTuple(Joiner.on(" ").skipNulls().join(f.get(0), f.get(1), f.get(2))));
    }

    // cached job object
    private Job job;

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location, job);
        this.job = job;
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        // check that we look like this
        Preconditions.checkNotNull(schema, "Schema is null");
        ResourceSchema.ResourceFieldSchema[] fields = schema.getFields();
        Preconditions.checkNotNull(fields, "Schema fields are undefined");
        Preconditions.checkArgument(6 == fields.length,
                "Expecting 6 schema fields but found %s, of type row:int, column:int, distance:double, MxN, global", fields.length);

        checkStoreKeySchema(fields[0], "row");
        checkStoreKeySchema(fields[1], "column");
        assertFieldTypeEquals(DataType.DOUBLE, fields[2].getType(), "distance");

        super.checkSchema(schema);
    }

    private void checkStoreKeySchema(ResourceSchema.ResourceFieldSchema schema, String fieldName) throws IOException {
        switch (schema.getType()) {
            case DataType.CHARARRAY:
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
                return;
        }
        throw new IOException(String.format("Expected %s of type '%s' but found type '%s'",
                fieldName, "Number", DataType.findTypeName(schema.getType())));
    }

    private static void assertFieldTypeEquals(byte expected, byte observed, String fieldName)
            throws IOException {
        if (expected != observed) {
            throw new IOException(String.format("Expected %s of type '%s' but found type '%s'",
                    fieldName, DataType.findTypeName(expected), DataType.findTypeName(observed)));
        }
    }

    private transient LRUMap<ElementDescriptor, Boolean> lookupCache = new LRUMap<ElementDescriptor, Boolean>(100, 1000);

    private boolean exists(ElementDescriptor e) throws IOException {
        if (lookupCache.containsKey(e)) {
            return lookupCache.get(e);
        } else {
            boolean res = e.exists();
            lookupCache.put(e, res);
            return res;
        }
    }

}
