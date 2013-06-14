package org.pigml.classify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.utils.SimpleReader;

import com.google.common.base.Preconditions;

public class Classifier extends EvalFunc<Object> {
	static final Log LOG = LogFactory.getLog(Classifier.class);
	private BaseModel model;
	private String path;
	private String modelPara;
	private final byte type;
	
	public Classifier(String path, String typeString) throws IOException {
		this(path, typeString, "");
	}
	public Classifier(String path, String typeString, String modelPara) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotBlank(typeString));
		this.path = path;
		this.modelPara = modelPara;
		Map<String, Byte> n2t = new HashMap<String, Byte>(DataType.genNameToTypeMap());
		n2t.put("INT", DataType.INTEGER);
		n2t.put("STRING", DataType.CHARARRAY);
		Preconditions.checkArgument(n2t.containsKey(typeString.toUpperCase()), "Unknown typeString '%s'", typeString);
		this.type = n2t.get(typeString.toUpperCase());
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() != 1) {
            warn("invalid number of arguments to LRClassifier", PigWarning.UDF_WARNING_1);
            return null;
        }
		if (model == null) {
			loadModel();
		}
		Map x = (Map) input.get(0);
		return model.predict(x);
	}
	
	private void loadModel() throws IOException {
		Configuration conf = UDFContext.getUDFContext().getJobConf();
		FileSystem fs = FileSystem.get(conf);
		for (FileStatus sub : fs.listStatus(new Path(path))) {
	    	if (!sub.isDir() && sub.getLen() > 0) {
	    		model = loadEssemble(model, fs, sub.getPath(), conf);
	    	}
	    }
		model.finish();
	}
	
	private BaseModel loadEssemble(BaseModel model, FileSystem fs, Path path, Configuration conf) throws IOException {
	    LOG.info("Loading model from "+path);
	    int numlines = countLines(fs, path, conf);
	    if (numlines <= 2) {
	    	return model;
	    }
		SimpleReader in = new SimpleReader(path, conf);
	    String modelClass = in.nextLine();
	    String datatype = in.nextLine();
	    if (model == null) {
	    	try {
			    model = (BaseModel) ReflectionUtils.newInstance(Class.forName(modelClass), conf);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
	    	model.initialize(Byte.valueOf(datatype), modelPara);
	    }
		model.initEssemble(numlines - 2);
		String line;
	    while ((line = in.nextLine()) != null) {
	    	model.load(line.split(","));
	    }
	    in.close();
	    LOG.info("Model loaded from "+path);
	    return model;
	}
	
	static private int countLines(FileSystem fs, Path path, Configuration conf) throws IOException {
	    SimpleReader in = new SimpleReader(path, conf);
	    Text text = new Text();
	    int result = 0;
	    while (in.readLine(text) > 0) {
	    	result++;
	    }
	    return result;
	}
	@Override
    public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, type));
    }
}
