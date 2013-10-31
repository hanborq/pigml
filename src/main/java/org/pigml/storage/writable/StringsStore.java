package org.pigml.storage.writable;

import org.apache.commons.cli.ParseException;
import org.pigml.storage.types.StringTupleConverter;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class StringsStore extends WritableStore {

    public StringsStore() throws IOException, ParseException, ClassNotFoundException {
        this("string");
    }

    public StringsStore(String keyType) throws IOException, ParseException, ClassNotFoundException {
        super(keyType, StringTupleConverter.class.getName());
    }
}
