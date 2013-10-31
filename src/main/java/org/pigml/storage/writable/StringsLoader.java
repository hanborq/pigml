package org.pigml.storage.writable;

import org.apache.commons.cli.ParseException;

import java.io.IOException;

import org.pigml.storage.types.*;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class StringsLoader extends WritableLoader {

    public StringsLoader() throws IOException, ParseException {
        this("string");
    }

    public StringsLoader(String keyType) throws IOException, ParseException {
        super(keyType, StringTupleConverter.class.getName());
    }
}
