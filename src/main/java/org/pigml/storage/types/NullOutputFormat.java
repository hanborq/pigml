package org.pigml.storage.types;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-30
 * Time: 上午11:38
 * To change this template use File | Settings | File Templates.
 */
public class NullOutputFormat extends OutputFormat {

    private static final Log LOG = LogFactory.getLog(NullOutputFormat.class);

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    private class NullWriter extends RecordWriter {

        @Override
        public void write(Object key, Object value) throws IOException, InterruptedException {
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        }
    }

    private class NullOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return true;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
        }
    }
}