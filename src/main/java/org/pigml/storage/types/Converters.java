package org.pigml.storage.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.twitter.elephantbird.pig.mahout.VectorWritableConverter;
import com.twitter.elephantbird.pig.util.BytesWritableConverter;
import com.twitter.elephantbird.pig.util.IntWritableConverter;
import com.twitter.elephantbird.pig.util.LongWritableConverter;
import com.twitter.elephantbird.pig.util.TextConverter;
import org.apache.pig.data.DataType;
import org.pigml.lang.FX;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午6:08
 * To change this template use File | Settings | File Templates.
 */
public class Converters {

    static private final Map<String, Class> types;

    static {
        types = new HashMap<String, Class>();
        regist(IntWritableConverter.class, "int", "integer", DataType.findTypeName(DataType.INTEGER));
        regist(LongWritableConverter.class, "long", DataType.findTypeName(DataType.LONG));
        regist(TextConverter.class, "string", "text", DataType.findTypeName(DataType.CHARARRAY));
        regist(BytesWritableConverter.class, "bytes", "byte[]", DataType.findTypeName(DataType.BYTEARRAY));
        regist(VectorWritableConverter.class, "vector");
        regist(DoubleWritableConverter.class, "double");
    }

    static public String get(String aliasOrClass) {
        Class aClass = FX.any(types.get(aliasOrClass), types.get(aliasOrClass.toLowerCase()));
        if (aClass == null) {
            try {
                aClass = Class.forName(aliasOrClass);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unknown type "+aliasOrClass+". Known types are "
                        + Iterables.toString(types.keySet()), e);
            }
        }
        return aClass.getName();
    }

    static private void regist(Class cls, String ...aliases) {
        Preconditions.checkArgument(aliases.length > 0);
        for (String alias : aliases) {
            alias = alias.toLowerCase();
            Class ex = FX.any(types.get(alias), cls);
            Preconditions.checkState(ex.equals(cls),
                    "Conflict regist converter for type %s (%s, %s)",
                    alias, ex.getSimpleName(), cls.getSimpleName());
            types.put(alias, cls);
        }
    }
}
