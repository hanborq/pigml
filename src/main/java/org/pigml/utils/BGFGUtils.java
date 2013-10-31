package org.pigml.utils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-10
 * Time: 下午6:09
 * To change this template use File | Settings | File Templates.
 */
//this helps inter-work between background/foreground
public class BGFGUtils {

    static public void savePartialState(Path dir, Configuration conf, String state) throws IOException {
        Preconditions.checkState(!UDFContext.getUDFContext().isFrontend());
        Path file = PathUtils.enter(dir, "_partial_" + Env.getPartID());
        IOUtils.writeText(conf, file, state);
    }

    static public Iterable<String> collectPartialStates(Path dir, final Configuration conf) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        Iterable<Path> files = PathUtils.listPathByPrefix(fs, dir, "_partial_");
        return Iterables.transform(files, new Function<Path, String>() {
            @Override
            public String apply(Path input) {
                try {
                    return IOUtils.readLine(conf, input);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
