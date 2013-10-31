package org.pigml.utils;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/17/13
 * Time: 4:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class Functors {

    static public Function<String, String> AS_STRING = Functions.identity();

    static public Function<String, Integer> AS_INT = new Function<String, Integer>() {
        @Override
        public Integer apply(String input) {
            return input != null ? Integer.parseInt(input) : null;
        }
    };

    static public Function<String, Long> AS_LONG = new Function<String, Long>() {
        @Override
        public Long apply(String input) {
            return input != null ? Long.parseLong(input) : null;
        }
    };

    static public Function<String, Double> AS_DOUBLE = new Function<String, Double>() {
        @Override
        public Double apply(String input) {
            return input != null ? Double.parseDouble(input) : null;
        }
    };

    static public Function<String, Float> AS_FLOAT = new Function<String, Float>() {
        @Override
        public Float apply(String input) {
            return input != null ? Float.parseFloat(input) : null;
        }
    };

    static public Function<String, Boolean> AS_BOOLEAN = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String input) {
            return input != null ? Boolean.parseBoolean(input) : null;
        }
    };

    static public Function<String, String> subString(final int from) {
        return new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input != null ? input.substring(from) : null;
            }
        };
    }

    static public Function<String, String> subStringAfter(final String pattern) {
        return new Function<String, String>() {
            @Override
            public String apply(String input) {
                if (input == null) {
                    return null;
                }
                int n = input.indexOf(pattern);
                return n >= 0 ? input.substring(n + pattern.length()) : "";
            }
        };
    }

    static public Function<Path, String> PATHNAME = new Function<Path, String>() {
        @Override
        public String apply(Path path) {
            return path.getName();
        }
    };

    static public Function<Path, String> PATH2STRING = new Function<Path, String>() {
        @Override
        public String apply(Path path) {
            return path.toString();
        }
    };

    static public Function<Path, FileStatus> path2Status(final FileSystem fs) {
        return new Function<Path, FileStatus>() {
            @Override
            public FileStatus apply(Path path) {
                try {
                    return fs.getFileStatus(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    static public PathFilter pathFilter(final Function<Path, Boolean> filt) {
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return filt.apply(path);
            }
        };
    }

    static public <F, M, T> Function<F, T> compose(final Function<F, M> f1, final Function<M, T> f2) {
        return new Function<F, T>() {
            @Override
            public T apply(F f) {
                return f2 != null ? f2.apply(f1 != null ? f1.apply(f) : null) : null;
            }
        };
    }

    static public Function<FileStatus, Boolean> isDirectory = new Function<FileStatus, Boolean>() {
        @Override
        public Boolean apply(org.apache.hadoop.fs.FileStatus fileStatus) {
            return fileStatus != null ? fileStatus.isDir() : null;
        }
    };

    static public Function<String, Integer> integer = new Function<String, Integer>() {
        @Override
        public Integer apply(String input) {
            try {
                return input != null ? Integer.parseInt(input) : null;
            } catch (NumberFormatException nfe) {
                return null;
            }
        }
    };

    static public Function<String, String> toMrJobId = new Function<String, String>() {
        @Override
        public String apply(String input) {
            try {
                JobID jid = JobID.forName(input);
                return jid != null ? jid.toString() : null;
            } catch (Exception ignore) {
                return null;
            }
        }
    };
}
