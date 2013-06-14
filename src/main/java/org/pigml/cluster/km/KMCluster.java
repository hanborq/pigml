package org.pigml.cluster.km;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;
import org.pigml.utils.EuclideanUtils;
import org.pigml.utils.FieldSchemaUtils;
import org.pigml.utils.FunctionUtils;
import org.pigml.utils.IOUtils;
import org.pigml.utils.MRUtils;
import org.pigml.utils.TupleVector;
import org.pigml.utils.FieldSchemaUtils.BadSchemaException;
import org.pigml.utils.IOUtils.LineReader;
import org.pigml.utils.IOUtils.LineWriter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class KMCluster extends EvalFunc<Tuple> {
	static final String EXTENSION = ".centroids";
	static final Log LOG = LogFactory.getLog(KMCluster.class);
	private final TupleFactory TF = TupleFactory.getInstance();
	private final int K;
	private final String path;
	private Map<Integer, Centroids> lastCentroids;
	private Map<Integer, Centroids> nextCentroids;
	private List<Path> oldfiles;
	
	public KMCluster(String K, String path) {
		this.K = Integer.valueOf(K);
		this.path = path;
		Preconditions.checkArgument(this.K > 0);
		LOG.info("KMCluster will use "+path+" as centroids storage");
	}
	
	@Override
	public Tuple exec(Tuple t) throws IOException {
		if (t == null || t.size() == 0) {
			return null;
		}
		if (nextCentroids == null) {
			init();
		}
		return TF.newTupleNoCopy(Arrays.asList((Object)cluster(t), t));
	}
	
	@Override
    public void finish() {
		for (Path old : oldfiles) {
			IOUtils.delete(old);
		}
		try {
			LineWriter writer = IOUtils.openForWrite(new Path(path+Path.SEPARATOR+MRUtils.identifyTask()+EXTENSION));
			boolean diffs = false;
			for (Map.Entry<Integer, Centroids> e : nextCentroids.entrySet()) {
				final Centroids c = e.getValue();
				Map<String, Double> m1 = c.freeze();
				String[] line = new String[m1.size() + 2];
				line[0] = e.getKey().toString();
				line[1] = String.valueOf(c.count);
				int i = 2;
				for (Map.Entry<String, Double> ee : m1.entrySet()) {
					line[i++] = ee.getKey()+"#"+ee.getValue();
				}
				writer.writeLine(line);
				diffs = diffs || !lastCentroids.containsKey(e.getKey()) ||
						!m1.equals(lastCentroids.get(e.getKey()).freeze());
			}
			writer.close();
			if (!diffs) {
				LOG.info("MERGED");
			}
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
    }
	
	@SuppressWarnings("unchecked")
	private Integer cluster(Tuple t) throws ExecException {
		Object obj = t.get(0);
		Map<String, Double> tv = obj instanceof Double ? new TupleVector(t) : (Map<String, Double>)obj;
		Integer k = null;
		if (lastCentroids.size() > 0) {
			Double dmax = Double.MAX_VALUE;
			for (Map.Entry<Integer, Centroids> e : lastCentroids.entrySet()) {
				double d = e.getValue().distance(tv);
				if (d < dmax) {
					dmax = d;
					k = e.getKey();
				}
			}
		} else {
			k = (int)(Math.random() * K) % K;
		}
		nextCentroids.get(k).update(tv);
		return k;
	}
	
	private void init() throws IOException {
		lastCentroids = new HashMap<Integer, Centroids>();
		oldfiles = new ArrayList<Path>();
		Configuration conf = UDFContext.getUDFContext().getJobConf();
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path(path))) {
    		for (FileStatus sub : fs.listStatus(new Path(path))) {
    			final String name = sub.getPath().getName();
    			if (name.endsWith(EXTENSION)) {
    				oldfiles.add(sub.getPath());
    			}
    		}
    		Preconditions.checkArgument(FX.transitiveSatisfy(oldfiles,
    				FunctionUtils.PATH_TO_STRING,
					new Function<Pair<String, String>, Boolean>(){
						@Override
						public Boolean apply(Pair<String, String> arg0) {
							return true; //TODO: CHECK THEY ARE FROM SAME JOB
						}
						
					}));
    		for (Path path : oldfiles) {
    			load(fs, conf, path);
    		}
    	    LOG.info("Loaded "+lastCentroids.size() +" side-data from "+path);
    	}
    	nextCentroids = new HashMap<Integer, Centroids>(K);
    	for (int i=0; i<K; i++) {
    		nextCentroids.put(i, new Centroids());
    	}
	}
	
	private void load(FileSystem fs, Configuration conf, Path path) throws IOException {
	    LineReader in = new LineReader(conf, path);
	    for (String[] ss = in.getLine(); ss != null; ss = in.getLine()) {
	    	if (ss.length == 0) {
	    		continue;
	    	}
	    	final Integer k = Integer.valueOf(ss[0]);
	    	final Integer w = Integer.valueOf(ss[1]);
	    	final Map<String, Double> vector = new HashMap<String, Double>(ss.length - 2);
	    	Preconditions.checkArgument(k < K);
	    	for (int i=2; i<ss.length; i++) {
	    		String[] sss = ss[i].split("#");
	    		Preconditions.checkArgument(sss.length == 2);
	    		vector.put(sss[0], Double.valueOf(sss[1]));
	    	}
	    	Centroids c = lastCentroids.get(k);
	    	if (c == null) {
				c = new Centroids();
				lastCentroids.put(k, c);
			}
	    	c.update(vector, w);
	    }
	    in.close();
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		try {
			FieldSchemaUtils.Assertor[] assertors = new FieldSchemaUtils.Assertor[input.size()];
			for (int i=0; i<input.size(); i++) {
				assertors[i] = FieldSchemaUtils.IS_DOUBLE;
			}
			FieldSchemaUtils.asserts(input, assertors);
		} catch (BadSchemaException e) {
			FieldSchemaUtils.Assertor m =
					FieldSchemaUtils.mapAssertor(FieldSchemaUtils.IS_DOUBLE);
			FieldSchemaUtils.asserts(input, m);
		}
		try {
			return new Schema(new Schema.FieldSchema(null, new Schema(Arrays.asList(
					new Schema.FieldSchema("cluster", DataType.INTEGER),
					new Schema.FieldSchema("t", input, DataType.TUPLE))), DataType.TUPLE));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
	
	static class Centroids {
		private final Map<String, Double> coordinates = new HashMap<String, Double>();
		private int count;
		private boolean freezed;
		
		public void update(Map<String, Double> vector) {
			update(vector, 1);
		}
		public void update(Map<String, Double> vector, int weight) {
			Preconditions.checkState(!freezed);
			for (Map.Entry<String, Double> e : vector.entrySet()) {
				Double d = FX.any(coordinates.get(e.getKey()), 0.0);
				coordinates.put(e.getKey(), d + e.getValue() * weight);
			}
			count+=weight;
		}
		public synchronized Map<String, Double> freeze() {
			if (!freezed) {
				if (count > 0) {
					for (Entry<String, Double> e : coordinates.entrySet()) {
						e.setValue(e.getValue() / count);
					}
				} else {
					LOG.warn("no object for this centroid");
				}
				freezed = true;
			}
			return coordinates;
		}
		public double distance(Map<String, Double> vector) {
			if (!freezed) {
				freeze();
			}
			return EuclideanUtils.distance(coordinates, vector);
		}
	}
	
}
