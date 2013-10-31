package org.pigml.storage.writable;

import com.twitter.elephantbird.pig.store.SequenceFileStorage;
import org.apache.commons.cli.ParseException;
import org.pigml.common.Defines;

import java.io.IOException;

import org.pigml.storage.types.*;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class WritableStore extends SequenceFileStorage implements Defines {

    public WritableStore(String keyType, String valueType) throws ParseException, IOException, ClassNotFoundException {
        super("-c " + Converters.get(keyType), "-c " + Converters.get(valueType), "");
    }
}
