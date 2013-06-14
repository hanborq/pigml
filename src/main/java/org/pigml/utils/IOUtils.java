package org.pigml.utils;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.lang.FX;


public class IOUtils {

	static final Log LOG = LogFactory.getLog(IOUtils.class);
	
	static public void delete(Path path) {
		try {
			FileSystem fs = FileSystem.get(UDFContext.getUDFContext().getJobConf());
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
		} catch (IOException e) {
			LOG.info("failed delete "+path+". "+e.getMessage());
		}
	}
	static public LineWriter openForWrite(Path path) throws IOException {
		return new LineWriter(UDFContext.getUDFContext().getJobConf(), path);
	}
	static public class LineWriter {
		static private final byte[] CRLF = "\n".getBytes();
		private final FSDataOutputStream os;
		private final byte[] separator;
		
		public LineWriter(Configuration configuration, Path path) throws IOException {
			this(configuration, path, ",");
		}
		public LineWriter(Configuration configuration, Path path, String separator) throws IOException {
			FileSystem fs = FileSystem.get(configuration);
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			this.os = fs.create(path, false);
			this.separator = FX.any(separator, "").getBytes();
		}
		public void writeLine(String ...objects) throws IOException {
			if (objects.length > 0) {
				Object s = objects[0];
				os.write(String.valueOf(s).getBytes());
				for (int i=1; i<objects.length; i++) {
					os.write(separator);
					os.write(objects[i].getBytes());
				}
				os.write(CRLF);
			}
		}
		public void close() throws IOException {
			os.close();
		}
	}
	
	static public class LineReader extends org.apache.hadoop.util.LineReader {
		private final Text line;
		private final String separator;
		
		public LineReader(Configuration conf, Path path) throws IOException {
			this(conf, path, ",");
		}
		
		public LineReader(Configuration conf, Path path, String separator) throws IOException {
			super(FileSystem.get(conf).open(path), conf);
			this.line = new Text();
			this.separator = separator;
		}
		
		public String[] getLine() throws IOException {
			if (super.readLine(line) <= 0)
				return null;
			String s = line.toString();
			if (s.endsWith("\n")) {
				s = s.substring(0, s.length() - 1);
			}
			return StringUtils.splitByWholeSeparatorPreserveAllTokens(s, separator);
		}
	}
}
