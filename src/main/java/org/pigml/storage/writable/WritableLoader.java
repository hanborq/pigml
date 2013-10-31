package org.pigml.storage.writable;

import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class WritableLoader extends SequenceFileLoader implements Defines {

    public WritableLoader(String keyType, String valueType) throws IOException, ParseException {
        super("-c " + Converters.get(keyType), "-c " + Converters.get(valueType), "");
    }
}
