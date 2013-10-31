package org.pigml.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;
import org.pigml.lang.Procedure;
import org.pigml.storage.types.CloseAwareOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-25
 * Time: 下午4:19
 * To change this template use File | Settings | File Templates.
 */
public abstract class ProxyStore implements StoreFuncInterface {

    private static final Log LOG = LogFactory.getLog(ProxyStore.class);

    protected StoreFuncInterface store;

    protected ProxyStore(StoreFuncInterface store) {
        this.store = store;
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        return store.relToAbsPathForStoreLocation(location, curDir);
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new CloseAwareOutputFormat(store.getOutputFormat(),
                new CloseAwareOutputFormat.CloseHandler() {
                    @Override
                    public void onClose(TaskAttemptContext context) {
                        handleWriterClose(context);
                    }
                });
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        store.setStoreLocation(location, job);
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        store.checkSchema(s);
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        store.prepareToWrite(writer);
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        store.putNext(t);
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        store.setStoreFuncUDFContextSignature(signature);
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        store.cleanupOnFailure(location, job);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        store.cleanupOnSuccess(location, job);
    }

    protected void handleWriterClose(TaskAttemptContext ctx) {
    }
}
