package org.pigml.utils;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;

public class FunctionUtils {
	static public final Function<Path, String> PATH_TO_STRING = new Function<Path, String>(){
		@Override
		public String apply(Path arg0) {
			return arg0.toString();
		}
	};

}
