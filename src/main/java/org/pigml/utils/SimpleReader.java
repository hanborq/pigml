package org.pigml.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class SimpleReader extends LineReader {
	private final Text line;
	
	public SimpleReader(Path path, Configuration conf) throws IOException {
		super(FileSystem.get(conf).open(path), conf);
		this.line = new Text();
	}
	
	public String nextLine() throws IOException {
		if (super.readLine(line) <= 0)
			return null;
		return line.toString().replaceAll("(\\r\\n|\\r|\\n)", "");
	}
}
