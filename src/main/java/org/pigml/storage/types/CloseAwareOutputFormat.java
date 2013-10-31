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
public class CloseAwareOutputFormat extends OutputFormat {

    private static final Log LOG = LogFactory.getLog(CloseAwareOutputFormat.class);

    private final OutputFormat inner;
    private final CloseHandler handleWriterClose;

    static public interface CloseHandler {
        void onClose(TaskAttemptContext ctx) throws IOException;
    }

    public CloseAwareOutputFormat(OutputFormat inner, CloseHandler handleWriterClose) {
        this.inner = Preconditions.checkNotNull(inner);
        this.handleWriterClose = Preconditions.checkNotNull(handleWriterClose);
    }
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new WrappedWriter(inner.getRecordWriter(context));
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        inner.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return inner.getOutputCommitter(context);
    }

    private void onCloseAttempt(TaskAttemptContext context) throws IOException {
        handleWriterClose.onClose(context);
    }

    private class WrappedWriter extends RecordWriter {

        private final RecordWriter writer;

        WrappedWriter(RecordWriter writer) {
            this.writer = writer;
        }

        @Override
        public void write(Object key, Object value) throws IOException, InterruptedException {
            writer.write(key, value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            onCloseAttempt(context);
            writer.close(context);
        }
    }
}