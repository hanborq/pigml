package org.pigml.storage.misc;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;

/*
 * load file with format "label attribute:value [attribute:value]*", e.g.,
-1 4:0.0539419 5:0.0827586 6:0.117647 16:0.2 17:0.42105 18:0.759938 21:0.000990412 22:0.000496032 23:0.000496032 24:1 25:1 33:0.0555556 37:1 41:0.1 44:1 45:1 54:1 56:1 64:1 66:1 68:1 70:1 72:1 74:1 76:1 81:0.05 82:1 83:0.333333 84:1 86:1 88:1 90:1 92:1 94:1 96:1 104:1 106:1 108:1 110:1 112:1 129:1 131:1 133:1 135:1 141:1 143:1 145:1 147:1 149:1 151:1 153:1 457:1 702:1 705:1 707:1 709:1 1032:1 1035:1 1036:1 1809:1 4638:1 8977:1 8978:1 8980:1 8981:1 8984:1 8985:1 8986:1 8987:1 8988:1 8989:1 8990:1 8991:1 8992:1 8993:1 8994:1 115683:1 155153:1 155154:1 155155:1 155156:1 155157:1 155158:1 155159:1 155161:1 155163:1 155164:1 155165:1 155166:1 155167:1 155168:1 155169:1 155170:1 155171:1 155172:1 155173:1 155174:1 155175:1 155176:1 155177:1 155178:1 155179:1 155180:1 155181:1 155182:1 155183:1 155184:1 155185:1 155186:1 155187:1 155188:1 155189:1 155190:1 155191:1 155192:1 155193:1 155194:1 155195:1 155196:1 155197:1 155198:1 155199:1 155200:1 155201:1 155202:1 155203:1 155204:1 155205:1 155206:1 155207:1 155208:1 155209:1 155210:1 155211:1 155212:1 155213:1 182603:1 2206146:1
-1 4:0.13278 5:0.0965517 6:0.176471 11:0.285714 16:0.1 17:0.637515 18:0.844078 19:0.129741 21:0.142856 22:0.142857 24:1 25:1 28:1 33:0.0555556 36:1 41:0.1 44:1 54:1 56:1 62:1 64:1 66:1 68:1 70:1 72:1 74:1 76:1 79:0.0769231 81:0.05 82:1 84:1 86:1 88:1 90:1 92:1 94:1 96:1 102:1 104:1 106:1 108:1 110:1 112:1 151:1 155:1 185:1 188:1 189:1 190:1 530:1 1283:1 1286:1 1490:1 1748:1 1751:1 1752:1 2038:1 2039:1 4504:1 4505:1 4507:1 4508:1 4509:1 4510:1 4511:1 155153:1 155154:1 155155:1 155156:1 155157:1 155158:1 155159:1 155160:1 155161:1 155163:1 155164:1 155166:1 155168:1 155169:1 155170:1 155172:1 155173:1 155174:1 155175:1 155176:1 155177:1 155178:1 155179:1 155180:1 155181:1 155182:1 155183:1 155194:1 155195:1 155196:1 155197:1 155198:1 155199:1 155200:1 155201:1 155202:1 155203:1 155204:1 155205:1 155206:1 155207:1 155208:1 155209:1 155210:1 155211:1 155212:1 155213:1 644942:1 1200325:1 1253159:1 1721795:1 2705452:1 3103063:1 3158074:1
-1 2:1 4:0.0705394 5:0.110345 6:0.117647 10:1 11:0.142857 17:0.675915 18:0.840116 19:0.184718 21:0.00148644 22:0.0014881 28:1 33:0.0555556 41:0.1 54:1 56:1 62:1 64:1 66:1 68:1 70:1 72:1 74:1 76:1 79:0.0769231 81:0.05 82:1 84:1 86:1 88:1 90:1 92:1 94:1 96:1 102:1 104:1 106:1 108:1 110:1 112:1 151:1 153:1 155:1 498:1 2940:1 2941:1 3202:1 3206:1 3716:1 4307:1 4818:1 4986:1 6332:1 6737:1 9871:1 155153:1 155154:1 155155:1 155156:1 155157:1 155158:1 155159:1 155160:1 155163:1 155164:1 155166:1 155168:1 155169:1 155170:1 155172:1 155173:1 155174:1 155175:1 155176:1 155177:1 155178:1 155179:1 155180:1 155181:1 155182:1 155183:1 155194:1 155195:1 155196:1 155197:1 155198:1 155199:1 155200:1 155201:1 155202:1 155203:1 155204:1 155205:1 155206:1 155207:1 155208:1 155209:1 155210:1 155211:1 155212:1 155213:1 258057:1 1377976:1 2193453:1 2202743:1 2230321:1 2341786:1
 */
public class LabeledMapLoader extends LoadFunc implements LoadMetadata {
	static final Log LOG = LogFactory.getLog(LabeledMapLoader.class);
	private RecordReader<Object, Text> reader;
	private byte fieldDel = '\t';
	private byte kvDel = ' ';
	
	public LabeledMapLoader() throws FrontendException {
		this(" ", ":");
	}
	
	public LabeledMapLoader(String fd, String kvd) throws FrontendException {
		if (fd.length() != 1 || kvd.length() != 1) {
			throw new FrontendException("only single-char delimiter is allowed");
		}
		this.fieldDel = (byte) fd.charAt(0);
		this.kvDel = (byte)kvd.charAt(0);
	}
	
	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = reader;
	}
	
	private int elstimatedDimension = 20;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple getNext() throws IOException {
        try {
			if (!reader.nextKeyValue()) {
			    return null;
			}
			Tuple t =  mTupleFactory.newTuple(2);
	        Text value = (Text) reader.getCurrentValue();
			byte[] buf = value.getBytes();
	        int len = value.getLength();
	        int start = 0;
	        while (start < len && buf[start] != fieldDel) {
	        	start++;
	        }
	        if (start == len) {
	        	LOG.error("malformed svm record "+value);
            	return null;
	        }
	        t.set(0, readField(buf, 0, start));
	        start++;
	        Map<String, Double> features = new LinkedHashMap<String, Double>(elstimatedDimension);
	        int j = -1;
	        for (int i=start; i < len; i++) {
	            if (buf[i] == fieldDel) {
	                if (j < 0 || start == j || j == i-1) {
	                	LOG.error("malformed svm record "+value);
	                	return null;
	                }
	            	String k = readField(buf, start, j);
	            	String v = readField(buf, j+1, i);
	            	features.put(k, Double.valueOf(v));
	                start = i + 1;
	                j = -1;
	            } else if (buf[i] == kvDel && j < 0){
	            	j = i;
	            }
	        }
	        if (start < len && j > 0 && start < j && j < len -1) {
	        	String k = readField(buf, start, j);
            	String v = readField(buf, j+1, len);
            	features.put(k, Double.valueOf(v));
	        }
	        t.set(1, features);
	        elstimatedDimension = features.size() + 1;
	        return t;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	private String readField(byte[] buf, int start, int end) {
	    if (start == end) {
	        return "";
	    } else {
	        return new String(buf, start, end-start);
	    }
	}

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		ResourceSchema mapSch = new ResourceSchema();
		mapSch.setFields(new ResourceFieldSchema[]{
				new ResourceFieldSchema()
				.setName("key").setDescription("key")
				.setType(DataType.CHARARRAY).setSchema(null),
				new ResourceFieldSchema()
				.setName("value").setDescription("value")
				.setType(DataType.DOUBLE).setSchema(null)
		});
		
		ResourceSchema schema = new ResourceSchema();
		schema.setFields(new ResourceFieldSchema[]{
				new ResourceFieldSchema()
				.setName("lable").setDescription("lable")
				.setType(DataType.CHARARRAY).setSchema(null),
				new ResourceFieldSchema()
				.setName("features").setDescription("features")
				.setType(DataType.MAP).setSchema(mapSch)	
		});
		return schema;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
	}
}
