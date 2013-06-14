package org.pigml.cluster.km;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Preconditions;

public class KMeanCluster extends EvalFunc<Integer> {
	static final Log LOG = LogFactory.getLog(KMeanCluster.class);
	private Map<Integer, Double[]> centroids;
	private String centroidsPath;
	private final int K;
	private int roundrobin;
	
	public KMeanCluster(String pathToSideData, String K) {
		this.centroidsPath = pathToSideData;
		this.K = Integer.valueOf(K);
	}
		
	@Override
	public Integer exec(Tuple input) throws IOException {
		if (input == null) {
            return null;
        }
		if (centroids == null) {
			init();
		}
		if (centroids.size() == 0) {
			return roundrobin++ % K;
		}
		int k = 0;
		double min = Double.MAX_VALUE;
		for (Map.Entry<Integer, Double[]> entry : centroids.entrySet()) {
			double d = eucDist(input, entry.getValue());
			if (d < min) {
				k = entry.getKey();
				min = d;
			}
		}
		return k;
	}
	
	static private Double eucDist(Tuple a, Double[] b) throws ExecException {
		double r = 0;
		for (int i=0; i<a.size(); i++) {
			double d = (Double)a.get(i) - b[i];
			r += d * d;
		}
		return r;
	}
	
	private void init() throws IOException {
		centroids = new HashMap<Integer, Double[]>();
		Configuration conf = UDFContext.getUDFContext().getJobConf();
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path(centroidsPath))) {
    		for (FileStatus sub : fs.listStatus(new Path(centroidsPath))) {
    			if (sub.getPath().getName().endsWith(".centroids")) {
    				load(fs, conf, sub.getPath());
    			}
    		}
    	    LOG.info("Loaded "+centroids.size() +" side-data from "+centroidsPath);
    	}
	}
	
	private void load(FileSystem fs, Configuration conf, Path path) throws IOException {
		FSDataInputStream fileIn = fs.open(path);
	    LineReader in = new LineReader(fileIn, conf);
	    Text text = new Text();
	    int dim = 0;
	    while (in.readLine(text) > 0) {
	    	String line = text.toString().replaceAll("(\\r\\n|\\r|\\n)", "");
	    	if (StringUtils.isBlank(line)) {
	    		continue;
	    	}
	    	String[] ss = line.split("\\s+");
	    	Preconditions.checkArgument(dim == 0 || dim == ss.length);
	    	dim = ss.length;
	    	final Integer k = Integer.valueOf(ss[0]);
	    	final Double[] vector = new Double[ss.length - 1];
	    	for (int i=0; i<vector.length; i++) {
	    		vector[i] = Double.valueOf(ss[1 + i]);
	    	}
	    	Preconditions.checkArgument(!centroids.containsKey(k));
	    	LOG.info("Load centroid "+k+" "+StringUtils.join(vector, ","));
	    	centroids.put(k, vector);
	    }
	    in.close();
	    fileIn.close();
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
    }
}
