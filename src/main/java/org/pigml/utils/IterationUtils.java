package org.pigml.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pigml.lang.FX;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-10
 * Time: 下午12:24
 * To change this template use File | Settings | File Templates.
 */
public class IterationUtils {
    static public int getLastIteration(Configuration conf, Path home) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Iterable<Path> paths = PathUtils.listPath(fs, home,
                PathUtils.isReduceOutputDirectory(fs), PathUtils.matchName("[\\d]+"));
        Iterable<Integer> names = Iterables.transform(
                PathUtils.pathName(paths), Functors.AS_INT);
        try {
            return Ordering.natural().max(names);
        } catch (NoSuchElementException ignore) {
            return -1;
        }
    }

    static private final String ITERATION_COMMITED = ".ITERATED";
    static private final String ITERATION_ALLOCATING = ".ITERATING";

    static public void commitIteration(Configuration conf, String home) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(home, ITERATION_ALLOCATING);
        if (fs.exists(src)) {
            Path dst = new Path(home, ITERATION_COMMITED);
            IOUtils.tryDelete(dst, conf);
            fs.rename(src, dst);
        } else {
            throw new IOException("Missing iterate marker "+src);
        }
    }

    static public void commitIteration(Configuration conf, String home, int iteration) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(home, ITERATION_ALLOCATING);
        Path dst = new Path(home, ITERATION_COMMITED);
        IOUtils.tryDelete(dst, conf);
        if (fs.exists(src)) {
            Preconditions.checkState(Integer.parseInt(IOUtils.readLine(conf, src)) == iteration);
            fs.rename(src, dst);
        } else {
            IOUtils.writeText(conf, new Path(home, ITERATION_COMMITED), String.valueOf(iteration));
        }
    }

    static public int allocIteration(Configuration conf, String home) throws IOException {
        int alloc = Integer.valueOf(FX.any(IOUtils.tryReadLine(conf, new Path(home, ITERATION_ALLOCATING)), "-1"));
        if (alloc >= 0) {
            return alloc;
        }
        alloc = 1 + Integer.valueOf(FX.any(IOUtils.tryReadLine(conf, new Path(home, ITERATION_COMMITED)), "-1"));
        IOUtils.writeText(conf, new Path(home, ITERATION_ALLOCATING), String.valueOf(alloc));
        return alloc;
    }
}
