package org.pigml.udf.text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.pigml.lang.Procedure;
import org.pigml.storage.ProxyStore;
import org.pigml.storage.writable.WritableStore;
import org.pigml.utils.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */
public class DictStore extends ProxyStore implements Constants {

    private static final Log LOG = LogFactory.getLog(DictStore.class);

    public DictStore(String numOfParts) throws ParseException, IOException, ClassNotFoundException {
        super(new WritableStore("string", "int"));
        numPart = Integer.parseInt(numOfParts);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                sequence = Env.getPartID();
                tfac = TupleFactory.getInstance();
                try {
                    Preconditions.checkState(sequence < numPart,
                            "Bad num of part %s for %s", numPart, sequence);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                LOG.info("Instance "+sequence+" of "+numPart);
            }
        });
        Preconditions.checkArgument(this.numPart > 0);
    }

    private int sequence;
    private int numPart;
    private TupleFactory tfac;

    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple != null) {
            super.putNext(tfac.newTuple(Arrays.asList(tuple.get(0), sequence)));
            sequence += numPart;
        }
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        SchemaUtils.claim(s, 0, DataType.CHARARRAY);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location, job);
        Env.setProperty(this.getClass(), "storelocation", location);
    }

    /*
    dict storage layout
        <location>/part-...
        <location>/.MAXVAL-tmp-<attempt-id>-value
        <location>/.MAXVAL-value
     */
    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        store.cleanupOnSuccess(location, job);
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Iterable<Path> paths = PathUtils.listPathByPrefix(fs, new Path(location), MAGIC);
        if (Iterables.size(paths) != numPart) {
            String msg = String.format("Configured part num (%s) mismatch actual part num (%s)",
                    numPart, Iterables.size(paths));
            LOG.error(msg);
            LOG.error("will now clean output "+location);
            IOUtils.delete(new Path(location), job.getConfiguration());
            throw new IOException(msg);
        }
        Iterable<String> sequences = Iterables.transform(
                Iterables.transform(paths, Functors.PATHNAME),
                Functors.subStringAfter("#"));
        final int cardinality = Ordering.natural().max(
                Iterables.transform(sequences, Functors.AS_INT)) + 1;
        LOG.info(String.format("Cardinality %d generated from %s",
                cardinality, Iterables.toString(sequences)));
        Path file = new Path(location + Path.SEPARATOR + DICT_CARDINAL_FILE);
        IOUtils.withTextWriter(conf, file, new Procedure<BufferedWriter>() {
            @Override
            public void execute(BufferedWriter writer) {
                try {
                    writer.write(String.valueOf(cardinality));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    protected void handleWriterClose(TaskAttemptContext context) {
        Path file = new Path(Env.getProperty(this.getClass(), "storelocation"),
                MAGIC+Env.getPartID()+"#"+sequence);
        try {
            file.getFileSystem(context.getConfiguration()).create(file);
            LOG.info("Created " + file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static private final String MAGIC = ".SEQUENCE.PART.";
}
