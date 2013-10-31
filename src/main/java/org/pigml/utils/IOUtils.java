package org.pigml.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.*;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;
import org.pigml.lang.Procedure;

import java.io.*;
import java.util.List;
import java.util.NoSuchElementException;


public class IOUtils {

	static final Log LOG = LogFactory.getLog(IOUtils.class);

    static public Procedure<BufferedWriter> WRITE_NOTHING = new Procedure<BufferedWriter>() {
            @Override
            public void execute(BufferedWriter context) throws ProcedureEndException {
            }
        };

    static public void writeText(final Configuration conf, final Path file,
                                 final String text) throws IOException {
        withTextWriter(conf, file, new Procedure<BufferedWriter>() {
            @Override
            public void execute(BufferedWriter writer) {
                try {
                    writer.write(text);
                    writer.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    //read first line
    static public String readLine(final Configuration conf, final Path file)
            throws IOException {
        final String[] result = new String[1];
        forEachLines(conf, file, new Procedure<String>() {
            @Override
            public void execute(String line) throws ProcedureEndException {
                if (result[0] != null) {
                    throw new ProcedureEndException();
                }
                result[0] = line;
            }
        });
        return result[0];
    }

    static public String tryReadLine(final Configuration conf, final Path file)
            throws IOException {
        try {
            return readLine(conf, file);
        } catch (FileNotFoundException fnfe) {
            return null;
        }
    }

    static public void withTextWriter(final Configuration conf, final Path file,
                                      Procedure<BufferedWriter> procedure) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream os = fs.create(file, true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os));
        try {
            procedure.execute(br);
        } finally {
            br.close();
        }
    }

    static public void forEachLines(final Configuration conf, final Path file, Procedure<String> procedure)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(file)) {
            throw new FileNotFoundException("");
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            try {
                procedure.execute(line);
            } catch (Procedure.ProcedureEndException pee) {
                break;
            }
        }
        reader.close();
    }

    static public <K extends Writable, V extends Writable> void forEachKeyValue(
            final Configuration conf, final Path file, Procedure<Pair<K, V>> procedure)
            throws IOException {
        Preconditions.checkArgument(file != null);
        for (org.apache.mahout.common.Pair<K, V> record
                : new SequenceFileIterable<K, V>(file, true, conf)) {
            try {
                procedure.execute(Pair.of(record.getFirst(), record.getSecond()));
            } catch (Procedure.ProcedureEndException pee) {
                break;
            }
        }
    }

    static public void tryDelete(Path path, Configuration conf) {
        try {
            delete(path, conf);
        } catch (FileNotFoundException ignore) {
        } catch (IOException e) {
            LOG.error("failed delete "+path, e);
        }
    }

	static public void delete(Path path, Configuration conf) throws IOException {
        path.getFileSystem(conf).delete(path, true);
	}

	static public RecordWriter forTextWrite(Configuration conf, Path path) throws IOException {
		throw new UnsupportedOperationException("IMPLEMENT ME!");
	}

    static public SequenceFile.Writer forSequenceWrite(
            Configuration conf, Path path, Class keyClass, Class valClass) throws IOException {
        return SequenceFile.createWriter(
                path.getFileSystem(conf), conf, path, keyClass, valClass);
    }

    //save matrix preserving dimension
    public static void saveMatrix(Path outputDir, Configuration conf, Matrix matrix)
            throws IOException {
        FileSystem fs = outputDir.getFileSystem(conf);
        fs.delete(outputDir, true);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outputDir,
                IntWritable.class, VectorWritable.class);
        Vector marker = new DenseVector(
                new double[]{
                        matrix.viewRow(0).isDense() ? DENSE : SPARSE,
                        matrix.numRows(),
                        matrix.numCols()});
        writer.append(new IntWritable(-1),
                new VectorWritable(marker));
        IntWritable topic = new IntWritable();
        VectorWritable vector = new VectorWritable();
        for (MatrixSlice slice : matrix) {
            topic.set(slice.index());
            vector.set(slice.vector());
            writer.append(topic, vector);
        }
        writer.close();
    }

    //load matrix with dimension
    public static Matrix loadMatrix(Configuration conf, Path... modelPaths) throws IOException {
        Vector marker = null;
        List<org.apache.mahout.common.Pair<Integer, Vector>> rows = Lists.newArrayList();
        for (Path modelPath : modelPaths) {
            Vector marker0 = null;
            for (org.apache.mahout.common.Pair<IntWritable, VectorWritable> row
                    : new SequenceFileIterable<IntWritable, VectorWritable>(modelPath, true, conf)) {
                if (marker0 == null) {
                    marker0 = row.getSecond().get();
                    Preconditions.checkState(
                            row.getFirst().get() == -1 && marker0.isDense() && marker0.size()==3,
                            "Illegal marker. Is the matrix %s created by peer writer?", modelPath);
                    marker = FX.any(marker, marker0);
                    Preconditions.checkState(marker.equals(marker0),
                            "Marker from %s mismatch previous marker", modelPath);
                } else {
                    rows.add(org.apache.mahout.common.Pair.of(row.getFirst().get(), row.getSecond().get()));
                }
            }
        }
        if (rows.isEmpty()) {
            throw new IOException(Arrays.toString(modelPaths) + " have no vectors in it");
        }
        int numRows = (int) marker.get(1);
        int numCols = (int) marker.get(2);
        Vector[] arrayOfRows = new Vector[numRows];
        for (org.apache.mahout.common.Pair<Integer, Vector> pair : rows) {
            arrayOfRows[pair.getFirst()] = pair.getSecond();
        }
        Matrix matrix;
        if (marker.get(0) == SPARSE) {
            matrix = new SparseRowMatrix(numRows, numCols, arrayOfRows);
        } else {
            Preconditions.checkState(marker.get(0) == DENSE);
            matrix = new DenseMatrix(numRows, numCols);
            for (int i = 0; i < numRows; i++) {
                matrix.assignRow(i, arrayOfRows[i]);
            }
        }
        return matrix;
    }

    private static final double DENSE = 1;
    private static final double SPARSE = 2;

}
