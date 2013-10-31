package org.pigml.storage.vector;

import com.twitter.elephantbird.pig.store.SequenceFileStorage;
import org.apache.commons.cli.ParseException;
import org.pigml.common.Defines;

import java.io.IOException;
import static org.pigml.storage.vector.VectorLoader.*;

import org.pigml.lang.FX;
import org.pigml.storage.types.*;
/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorStore extends SequenceFileStorage implements Defines {

    public VectorStore() throws ParseException, IOException, ClassNotFoundException {
        this(null);
    }

    public VectorStore(String keyType) throws ParseException, IOException, ClassNotFoundException {
        this(keyType, "", "");
    }

    public VectorStore(String keyType, String valueArgs, String otherArgs) throws ParseException, IOException, ClassNotFoundException {
        super("-c " + Converters.get(FX.any(keyType, "int")),
                "-c " + Converters.get("vector") + formatValueArgs(valueArgs),
                otherArgs);
    }
}
