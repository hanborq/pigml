package org.pigml.utils;

import com.google.common.base.Preconditions;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-6
 * Time: 下午5:42
 * To change this template use File | Settings | File Templates.
 */
public class SchemaUtils {

    //ResourceSchema

    static public ResourceSchema.ResourceFieldSchema expect(
            ResourceSchema.ResourceFieldSchema rfs, byte... anyType) {
        for (byte t : anyType) {
            if (t == rfs.getType() || t == 0) {
                return rfs;
            }
        }
        throw new IllegalArgumentException(
                String.format("Field %s of type %s matched none of %s",
                rfs.getName(), DataType.findTypeName(rfs.getType()), nameTypes(anyType)));
    }

    static public ResourceSchema.ResourceFieldSchema claim(
            ResourceSchema.ResourceFieldSchema fs, int at, byte... anyType) {
        ResourceSchema sch = fs.getSchema();
        Preconditions.checkArgument(sch != null,
                "Field %s of type %s do not contain sub schema",
                fs.getName(), DataType.findTypeName(fs.getType()));
        try {
            return claim(sch, at, anyType);
        } catch (IllegalArgumentException ie) {
            throw new IllegalArgumentException(
                    String.format("Field %s of type %s do not contain sub-field of %s at %d",
                            fs.getName(), DataType.findTypeName(fs.getType()),
                            nameTypes(anyType), at), ie);
        }
    }

    static public ResourceSchema.ResourceFieldSchema claim(ResourceSchema s, int at, byte... anyType) {
        ResourceSchema.ResourceFieldSchema[] fields = s.getFields();
        Preconditions.checkArgument(fields.length > at,
                "No field at %s of type for %s",
                at, nameTypes(anyType), s);
        return expect(fields[at], anyType);
    }

    static public ResourceSchema.ResourceFieldSchema tryClaim(
            ResourceSchema.ResourceFieldSchema fs, int at, byte... anyType) {
        try {
            return claim(fs, at, anyType);
        } catch (IllegalArgumentException ignore) {
            return null;
        }
    }

    static public ResourceSchema.ResourceFieldSchema tryClaim(ResourceSchema s, int at, byte... anyType) {
        try {
            return claim(s, at, anyType);
        } catch (IllegalArgumentException ignore) {
            return null;
        }
    }

    static public boolean matches(ResourceSchema s, byte... types) {
        ResourceSchema.ResourceFieldSchema[] fields = s.getFields();
        if (fields.length >= types.length) {
            for (int i=0; i<types.length; i++) {
                if (fields[i].getType() != types[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static private String nameTypes(byte[] types) {
        String s = "";
        if (types.length > 0) {
            s = DataType.findTypeName(types[0]);
            for (int i=1; i<types.length; i++) {
                s += "|" + DataType.findTypeName(types[i]);
            }
        }
        return "[" + s + "]";
    }

    //FieldSchema

    static public Schema.FieldSchema expect(
            Schema.FieldSchema rfs, byte... anyType) {
        for (byte t : anyType) {
            if (t == rfs.type || t == 0) {
                return rfs;
            }
        }
        throw new IllegalArgumentException(
                String.format("Field %s of type %s matched none of %s",
                        rfs.alias, DataType.findTypeName(rfs.type), nameTypes(anyType)));
    }

    static public Schema.FieldSchema claim(
            Schema sch, int at, byte... anyType) {
        Preconditions.checkArgument(sch.size() > at, "No field at %s of type %s for '%s'",
                at, nameTypes(anyType), sch);
        try {
            Schema.FieldSchema field = sch.getField(at);
            return expect(field, anyType);
        } catch (FrontendException fe) {
            throw new RuntimeException(fe);
        }
    }

    static public Schema.FieldSchema claim(
            Schema.FieldSchema fsch, int at, byte... anyType) {
        Preconditions.checkArgument(fsch.schema != null,
                "Field %s of type %s do not contain a sub-schema",
                fsch.alias, DataType.findTypeName(fsch.type));
        try {
            return claim(fsch.schema, at, anyType);
        } catch (IllegalArgumentException ie) {
            throw new IllegalArgumentException(
                    String.format("Field %s of type %s do not contain sub-field of %s at %d",
                            fsch.alias, DataType.findTypeName(fsch.type),
                            nameTypes(anyType), at), ie);
        }
    }

    static public Schema.FieldSchema tryClaim(
            Schema sch, int at, byte... anyType) {
        try {
            return claim(sch, at, anyType);
        } catch (IllegalArgumentException ignore) {
            return null;
        }
    }

    static public Schema.FieldSchema tryClaim(
            Schema.FieldSchema fsch, int at, byte... anyType) {
        try {
            return claim(fsch, at, anyType);
        } catch (IllegalArgumentException ignore) {
            return null;
        }
    }

    static public boolean matches(Schema s, byte... types) {
        List<Schema.FieldSchema> fields = s.getFields();
        if (fields.size() >= types.length) {
            for (int i=0; i<types.length; i++) {
                if (fields.get(i).type != types[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    ///////////Schema composer

    //compose a Schema composed by fields
    static public Schema schemaOf(Schema.FieldSchema... fields) {
        return new Schema(Arrays.asList(fields));
    }

    //compose a field of type TUPLE
    static public Schema.FieldSchema tupleOf(Schema.FieldSchema... fields) {
        return fieldOf(null, DataType.TUPLE, schemaOf(fields));
    }

    //compose a field of type BAG
    static public Schema.FieldSchema bagOf(Schema.FieldSchema... fields) {
        return bagOf(null, fields);
    }

    //compose a field of primitive type, like INTEGER, CHARARRAY, DOUBLE, ...
    static public Schema.FieldSchema primitiveOf(byte type) {
        return primitiveOf(null, type);
    }

    static public Schema.FieldSchema bagOf(String name, Schema.FieldSchema... fields) {
        Schema inner = new Schema(Arrays.asList(fields));
        try {
            return new Schema.FieldSchema(name, inner, DataType.BAG);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    static public Schema.FieldSchema primitiveOf(String name, byte type) {
        return new Schema.FieldSchema(name, type);
    }

    static public Schema.FieldSchema fieldOf(String name, byte type, Schema schema) {
        try {
            return new Schema.FieldSchema(name, schema, type);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
