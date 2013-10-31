package org.pigml.storage;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.storage.types.CloseAwareOutputFormat;
import org.pigml.utils.Env;

import java.io.IOException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-25
 * Time: 下午4:19
 * To change this template use File | Settings | File Templates.
 */

/**
 * Implements a close-aware base StoreFunc along with some trivial interface methods.
 */
public abstract class AbstractStore extends StoreFunc {

    private static final Log LOG = LogFactory.getLog(AbstractStore.class);

    private OutputFormat of;
    private RecordWriter writer;

    private String storeLocation;
    private String udfSignature;

    protected AbstractStore(OutputFormat of) {
        this.of = of;
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new CloseAwareOutputFormat(of,
                new CloseAwareOutputFormat.CloseHandler() {
                    @Override
                    public void onClose(TaskAttemptContext ctx) throws IOException {
                        handleWriterClose(ctx);
                    }
                });
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        forPigStoreLocation(location, job);
        this.storeLocation = location;
        setUDFProperty("storeLocation", location);
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.udfSignature = signature;
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        LOG.info("Do nothing for failure!");
    }

    protected void forPigStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    protected void handleWriterClose(TaskAttemptContext ctx) throws IOException {
        //OVERRIDE ME!
    }

    protected void write(Object key, Object value) throws IOException, InterruptedException {
        writer.write(key, value);
    }

    protected Properties getUDFProperties() {
        return UDFContext.getUDFContext()
                .getUDFProperties(AbstractStore.class, new String[] { udfSignature });
    }

    private static final String MAGIC = "__"+AbstractStore.class.getName()+"__";
    protected void setUDFProperty(String key, String value) {
        Env.setProperty(AbstractStore.class, udfSignature + key, value);
        Env.setProperty(AbstractStore.class, MAGIC + key, value);
    }

    protected String getUDFProperty(String key) {
        if (udfSignature == null) {
            LOG.warn("Read property "+key+" before signature ready");
            //TODO: reconsider it. this happens if called from Ctor
            String value = Env.getProperty(AbstractStore.class, MAGIC + key);
            return value;
        }
        return Env.getProperty(AbstractStore.class, udfSignature + key);
    }

    protected Path getStorePath() {
        if (storeLocation != null) { //this is the case of foreground
            return new Path(storeLocation);
        }
        Preconditions.checkState(!UDFContext.getUDFContext().isFrontend());
        return new Path(getUDFProperty("storeLocation"));
    }
}
