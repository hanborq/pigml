package org.pigml.utils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-10
 * Time: 下午12:09
 * To change this template use File | Settings | File Templates.
 */
public class PathUtils {

    static public Path enter(Path parent, String... intos) {
        Path result = parent;
        for (String child : intos) {
            if (StringUtils.isNotBlank(child)) {
                result = new Path(result, child);
            }
        }
        return result;
    }

    /**
     * resolve deepest input paths recursively.
     * refer: https://issues.apache.org/jira/browse/MAPREDUCE-3193
     * @param source
     * @param conf
     * @return
     * @throws java.io.IOException
     */
    static public List<Path> resolveInputPaths(Path source, Configuration conf) throws IOException {
        List<Path> resolved = new ArrayList<Path>();
        final FileSystem fs = source.getFileSystem(conf);
        FileStatus[] globs = fs.globStatus(source, hiddenFileFilter);
        for (FileStatus glob : globs) {
            if (glob.isDir()) {
                resolved.addAll(resolveInputPathsGlobbed(glob.getPath(), conf));
            } else {
                resolved.add(glob.getPath());
            }
        }
        if (resolved.size() == 0) {
            resolved.add(source);
        }
        return resolved;
    }

    static public List<Path> resolveInputPathsGlobbed(Path source, Configuration conf) throws IOException {
        final FileSystem fs = source.getFileSystem(conf);
        ArrayList<Path> result = new ArrayList<Path>();
        for(FileStatus stat: fs.listStatus(source, and(isDir(fs), hiddenFileFilter))) {
            result.addAll(resolveInputPathsGlobbed(stat.getPath(), conf));
        }
        if (result.size() == 0) {
            result.add(source);
        }
        return result;
    }

    static public Iterable<Path> listPath(FileSystem fs, Path parent, PathFilter... filts) throws IOException {
        PathFilter filter = hiddenFileFilter;
        if (filts.length == 1) {
            filter = filts[0];
        } else if (filts.length > 1) {
            filter = and(filts);
        }
        FileStatus[] st = fs.listStatus(parent, filter);
        return Iterables.transform(Arrays.asList(st), new Function<FileStatus, Path>() {
            @Override
            public Path apply(FileStatus fileStatus) {
                return fileStatus.getPath();
            }
        });
    }

    static public Iterable<String> pathName(Iterable<Path> paths) {
        return Iterables.transform(paths, new Function<Path, String>() {
            @Override
            public String apply(Path input) {
                return input != null ? input.getName() : null;
            }
        });
    }

    static public Iterable<Path> listPathByPrefix(FileSystem fs, Path location, String prefix)
            throws IOException {
        return listPath(fs, location, matchPrefix(prefix));
    }

    public static Path getParts(Configuration conf, String location, final int iter)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Iterable<String> parts = Iterables.transform(
                listPathByPrefix(fs, new Path(location), "part-"), Functors.PATHNAME);
        Iterator<String> part = Iterables.filter(
                parts,
                new Predicate<String>() {
                    @Override
                    public boolean apply(String input) {
                        return iter == Integer.parseInt(
                                input.substring(input.lastIndexOf("-") + 1));
                    }
                }).iterator();
        return part.hasNext() ? new Path(location, part.next()) : null;
    }

    static public int countParts(Configuration conf, String location) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Iterable<Path> parts = listPathByPrefix(fs, new Path(location), "part-");
        return Iterables.size(parts);
    }

    static public PathFilter isDir(final FileSystem fs) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                try {
                    return fs.isDirectory(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    static public PathFilter matchPrefix(final String prefix) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(prefix);
            }
        };
    }

    static public PathFilter and(final PathFilter... filters) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                for (PathFilter f : filters) {
                    if (!f.accept(path)) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    static public PathFilter not(final PathFilter filter) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return !filter.accept(path);
            }
        };
    }


    static public final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    private static final PathFilter PART_FILE_INSTANCE = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            return name.startsWith("part-") && !name.endsWith(".crc");
        }
    };

    static public PathFilter isReduceOutputDirectory(final FileSystem fs) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                try {
                    return fs.isDirectory(path) &&
                            fs.listStatus(path, PathFilters.partFilter()).length > 0;
                } catch (IOException e) {
                }
                return false;
            }
        };
    }

    static public PathFilter matchName(final String patten) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches(patten);
            }
        };
    }

    static public Function<FileStatus, String> filename = new Function<FileStatus, String>() {
        @Override
        public String apply(org.apache.hadoop.fs.FileStatus fileStatus) {
            return fileStatus != null ? fileStatus.getPath().getName() : null;
        }
    };

}
