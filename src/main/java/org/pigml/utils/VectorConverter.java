package org.pigml.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.List;

/**
 * Convert between Vector and Tuple.
 *
 * @see com.twitter.elephantbird.pig.mahout.VectorWritableConverter
 */
public class VectorConverter {

    public enum VectorType {
        DENSE("DENSE"),  // (double, double)
        SPARSE("SPARSE"),  // (entries: {entry: (index: int, value: double)})
        CARDINALITY_SPARSE("CARDINALITY_SPARSE");  // (cardinality: int, entries: {t: (index: int, value: double)})

        private String repr;

        VectorType(String repr) {
            this.repr = repr;
        }

        @Override
        public String toString() {
            return repr;
        }

        private VectorType fromString(String repr) throws IOException {
            for (VectorType type : ImmutableList.of(DENSE, SPARSE, CARDINALITY_SPARSE)) {
                if (type.repr.equalsIgnoreCase(repr)) {
                    return type;
                }
            }
            throw new IOException("invalid VectorType: " + repr);
        }
    }


    private static TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static BagFactory BAG_FACTORY = BagFactory.getInstance();


    public static VectorType getVectorTypeOfTuple(Tuple tuple) throws IOException {
        Preconditions.checkNotNull(tuple);
        if (tuple.size() == 1 && tuple.getType(0) == DataType.BAG) {
            DataBag entries = (DataBag) tuple.get(0);
            Preconditions.checkArgument(entries.size() > 0, "vector entries is empty");
            validateSparseVectorData(entries.iterator().next());
            return VectorType.SPARSE;
        } else if (tuple.size() == 2
                && tuple.getType(0) == DataType.INTEGER
                && tuple.getType(1) == DataType.BAG) {
            DataBag entries = (DataBag) tuple.get(1);
            Preconditions.checkArgument(entries.size() > 0, "vector entries is empty");
            validateSparseVectorData(entries.iterator().next());
            return VectorType.CARDINALITY_SPARSE;
        } else {
            validateDenseVector(tuple);
            return VectorType.DENSE;
        }
    }

    private static void validateSparseVectorData(Tuple tuple) throws IOException {
        assertTupleLength(2, tuple.size(), "entries[0]");
        assertFieldTypeEquals(DataType.INTEGER, tuple.getType(0), "entries[0][0]");
        assertFieldTypeIsNumeric(tuple.getType(1), "entries[0][1]");
    }

    public static VectorType getVectorTypeOfSchema(Schema schema) throws IOException {
        Preconditions.checkNotNull(schema, "Vector schema is null");
        List<Schema.FieldSchema> fields = schema.getFields();
        if (fields.size() == 1 && fields.get(0).type == DataType.BAG) {
            checkSparseVectorEntriesSchema(fields.get(0).schema);
            return VectorType.SPARSE;
        } else if (fields.size() == 2
                && fields.get(0).type == DataType.INTEGER
                && fields.get(1).type == DataType.BAG) {
            checkSparseVectorEntriesSchema(fields.get(1).schema);
            return VectorType.CARDINALITY_SPARSE;
        } else {
            checkDenseVectorSchema(schema);
            return VectorType.DENSE;
        }
    }

    private static void checkDenseVectorSchema(Schema schema) throws IOException {
        List<Schema.FieldSchema> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            assertFieldTypeIsNumeric(fields.get(i).type, "tuple[" + i + "]");
        }
    }

    private static void checkSparseVectorEntriesSchema(Schema entriesSchema) throws IOException {
        // check entries.length == 1
        assertNotNull(entriesSchema, "ResourceSchema of entries is null");
        List<Schema.FieldSchema> entriesFieldSchemas = entriesSchema.getFields();
        assertNotNull(entriesFieldSchemas, "Tuple field schemas are null");
        assertTupleLength(1, entriesFieldSchemas.size(), "entries");

        // check entries[0] == entry:tuple
        assertFieldTypeEquals(DataType.TUPLE, entriesFieldSchemas.get(0).type, "entries[0]");

        // check entries[0].length == 2
        Schema entriesTupleSchema = entriesFieldSchemas.get(0).schema;
        assertNotNull(entriesTupleSchema, "ResourceSchema of entries[0] is null");
        List<Schema.FieldSchema> entriesTupleFieldSchemas = entriesTupleSchema.getFields();
        assertNotNull(entriesTupleFieldSchemas, "Tuple field schemas are null");
        assertTupleLength(2, entriesTupleFieldSchemas.size(), "entries[0]");

        // check entries[0][0] == index:int
        assertFieldTypeEquals(DataType.INTEGER, entriesTupleFieldSchemas.get(0).type, "entries[0][0]");

        // check entries[0][1] == value:double
        assertFieldTypeIsNumeric(entriesTupleFieldSchemas.get(1).type, "entries[0][1]");
    }

    private static void validateDenseVector(Tuple value) throws IOException {
        assertNotNull(value, "Tuple is null");
        for (int i = 0; i < value.size(); ++i) {
            assertFieldTypeIsNumeric(value.getType(i), "tuple[" + i + "]");
        }
    }

    private static void assertTupleLength(int expected, int observed, String fieldName)
            throws IOException {
        if (expected != observed) {
            throw new IOException(String.format("Expected %s of length %s but found length %s",
                    fieldName, expected, observed));
        }
    }

    private static void assertFieldTypeEquals(byte expected, byte observed, String fieldName)
            throws IOException {
        if (expected != observed) {
            throw new IOException(String.format("Expected %s of type '%s' but found type '%s'",
                    fieldName, DataType.findTypeName(expected), DataType.findTypeName(observed)));
        }
    }

    private static void assertFieldTypeIsNumeric(byte observed, String fieldName) throws IOException {
        switch (observed) {
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
                break;
            default:
                throw new IOException(String.format("Expected %s of numeric type but found type '%s'",
                        fieldName, DataType.findTypeName(observed)));
        }
    }

    private static void assertNotNull(Object value, String msg, Object... values) throws IOException {
        if (value == null) {
            throw new IOException(String.format(msg, values));
        }
    }

    /**
     * Check if the schema is a Vector schema of any type.
     * @param schema the schema to be examined.
     * @throws IOException if schema is not a vector-convertable schema.
     */
    public static void checkVectorSchema(Schema schema) throws IOException {
        getVectorTypeOfSchema(schema);
    }

    /**
     * Check if the schema is a schema of specified type.
     * @param schema the schema to be examined.
     * @param type the schema type.
     * @throws IOException
     */
    public static void checkVectorSchema(Schema schema, VectorType type) throws IOException {
        VectorType actualType = getVectorTypeOfSchema(schema);
        if (actualType != type) {
            throw new IOException("Expect vector type " + type + ", observed type " + actualType);
        }
    }

    public static Vector convertToVector(Tuple tuple, VectorType type) throws IOException {
        if (type == null) {
            type = getVectorTypeOfTuple(tuple);
        }

        if (type == VectorType.SPARSE) {
            return convertToSparseVector(tuple);
        } else if (type == VectorType.CARDINALITY_SPARSE) {
            return convertToCardinalitySparseVector(tuple);
        } else {
            return convertToDenseVector(tuple);
        }
    }

    private static Vector convertToSparseVector(Tuple tuple) throws IOException {
        DataBag entries = (DataBag) tuple.get(0);
        // TODO(clay.chiang) we may need SequentialAccessSparseVector ...
        Vector vector = new RandomAccessSparseVector((int) entries.size());
        populateVectorData(entries, vector);
        return vector;
    }

    private static Vector convertToCardinalitySparseVector(Tuple tuple) throws IOException {
        int cardinality = (Integer) tuple.get(0);
        Vector vector = new RandomAccessSparseVector(cardinality);
        DataBag entries = (DataBag) tuple.get(1);
        populateVectorData(entries, vector);
        return vector;
    }

    private static void populateVectorData(DataBag entries, Vector vector) throws IOException {
        for (Tuple entry: entries) {
            int index = (Integer) entry.get(0);
            double value = (Double) entry.get(1);
            vector.setQuick(index, value);
        }
    }

    private static Vector convertToDenseVector(Tuple tuple) throws IOException {
        // TODO(clay.chiang) we may want to convert a dense vector tuple to sparse vector?
        // TODO(clay.chiang) we may want a customized cardinality?
        int cardinality = tuple.size();
        double[] values = new double[cardinality];
        for (int i = 0; i < tuple.size(); ++i) {
            values[i] = ((Number) tuple.get(i)).doubleValue();
        }
        return new DenseVector(values);
    }


    public static Tuple convertToTuple(Vector vector, VectorType type) {
        if (type == null) {
            if (vector.isDense()) {
                type = VectorType.DENSE;
            } else {
                type = VectorType.CARDINALITY_SPARSE;
            }
        }

        if (type == VectorType.DENSE) {
            return convertToDenseVectorTuple(vector);
        } else if (type == VectorType.CARDINALITY_SPARSE) {
            return convertToCardinalitySparseVectorTuple(vector);
        } else {
            return convertToSparseVectorTuple(vector);
        }
    }

    private static Tuple convertToDenseVectorTuple(Vector vector) {
        List<Number> values = Lists.newArrayListWithCapacity(vector.size());
        for (Vector.Element element : vector.all()) {
            values.add(element.get());
        }
        return TUPLE_FACTORY.newTupleNoCopy(values);
    }

    private static Tuple convertToCardinalitySparseVectorTuple(Vector vector) {
        DataBag entries = convertVectorToDataBag(vector);
        return TUPLE_FACTORY.newTupleNoCopy(
                Lists.newArrayList(vector.size(), entries));
    }

    private static Tuple convertToSparseVectorTuple(Vector vector) {
        DataBag entries = convertVectorToDataBag(vector);
        return TUPLE_FACTORY.newTupleNoCopy(ImmutableList.of(entries));
    }

    private static DataBag convertVectorToDataBag(Vector vector) {
        DataBag entries = BAG_FACTORY.newDefaultBag();
        for (Vector.Element e : vector.nonZeroes()) {
            entries.add(TUPLE_FACTORY.newTupleNoCopy(
                    ImmutableList.of(e.index(), e.get())));
        }
        return entries;
    }

    public static Schema newVectorSchema(VectorType type) throws FrontendException {
        if (type == VectorType.DENSE) {
            return new Schema(ImmutableList.of(
                    new Schema.FieldSchema(null, DataType.INTEGER),
                    new Schema.FieldSchema(null, DataType.DOUBLE)));
        } else {
            Schema entriesSchema = new Schema(
                    new Schema.FieldSchema(
                            null,
                            new Schema(ImmutableList.of(
                                    new Schema.FieldSchema(null, DataType.INTEGER),
                                    new Schema.FieldSchema(null, DataType.DOUBLE))),
                            DataType.BAG));
            if (type == VectorType.SPARSE) {
                return new Schema(ImmutableList.of(
                        new Schema.FieldSchema(null, entriesSchema)));
            } else {
                return new Schema(ImmutableList.of(
                        new Schema.FieldSchema(null, DataType.INTEGER),
                        new Schema.FieldSchema(null, entriesSchema)));
            }
        }
    }
}
