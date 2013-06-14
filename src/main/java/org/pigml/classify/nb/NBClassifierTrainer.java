package org.pigml.classify.nb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataType;
import org.pigml.classify.AbstractTrainer;
import org.pigml.classify.BaseModel;
import org.pigml.lang.Pair;

import com.google.common.base.Preconditions;

/*
 * the trainer for NBClassifier (naive-bayes-classifier)
 */
public class NBClassifierTrainer extends AbstractTrainer {
	static final Log LOG = LogFactory.getLog(NBClassifierTrainer.class);
	
	private Map<Object, Map<Object, Map<Object, Integer>>> stats = new HashMap<Object, Map<Object, Map<Object, Integer>>>();
	
	@SuppressWarnings("rawtypes")
	@Override
	protected void trainModel(Object label, Map features) {
		Map<Object, Map<Object, Integer>> sub1 = stats.get(label);
		if (sub1 == null) {
			sub1 = new HashMap<Object, Map<Object, Integer>>(features.size());
			stats.put(label, sub1);
		}
		for (Object k : features.keySet()) {
			Map<Object, Integer> sub2 = sub1.get(k);
			if (sub2 == null) {
				sub2 = new HashMap<Object, Integer>(1);
				sub1.put(k, sub2);
			}
			Object v = features.get(k);
			Integer n = sub2.get(v);
			sub2.put(v, n != null ? n + 1 : 1);
		}
	}
	
	@Override
	protected Class<? extends BaseModel> getModelClass() {
		return Model.class;
	}

	@Override
	protected void saveModel() {
		for (Map.Entry<Object, Map<Object, Map<Object, Integer>>> ent0 : stats.entrySet()) {
			final int n = ent0.getValue().size();
			persist(ent0.getKey(), n);
			for (Map.Entry<Object, Map<Object, Integer>> ent1 : ent0.getValue().entrySet()) {
				for (Entry<Object, Integer> ent2 : ent1.getValue().entrySet())
					persist(ent0.getKey(), ent1.getKey(), ent2.getKey(), ent2.getValue());
			}
		}
	}

	@Override
	protected byte getPredictType() {
		return stats.size() > 0 ? DataType.findType(stats.keySet().iterator().next()) : DataType.UNKNOWN;
	}
	
	static public class Model implements BaseModel {
		private Map<Object, Map<Pair<String, String>, Long>> pconditionals;
		private Map<Object, Long> prios;
		private long total = 0;
		private byte type;
		
		@Override
		public void initialize(byte type, String modelPara) {
			this.type = type;
			this.pconditionals = new HashMap<Object, Map<Pair<String, String>, Long>>();
			this.prios = new HashMap<Object, Long>();
		}

		@Override
		public void initEssemble(int numPara) {
		}
		
		@Override
		public void load(String[] para) {
			Preconditions.checkArgument(para.length == 2 || para.length == 4);
			Long p = Long.valueOf(para[para.length - 1]);
			final Object label = deType(para[0]);
			Preconditions.checkArgument(label != null && p > 0, "invalid label %s or count %s", label, p);
			if (para.length == 2) {
				increase(prios, label, p);
				if (!pconditionals.containsKey(label)) {
					pconditionals.put(label, new HashMap<Pair<String, String>, Long>());
				}
			} else {
				Map<Pair<String, String>, Long> pc = pconditionals.get(label);
				increase(pc, new Pair<String, String>(para[1], para[2]), p);
			}
		}
		@Override
		public void finish() {
			LOG.info("Loaded labels are "+StringUtils.join(prios.keySet(), ","));
			for (Map.Entry<Object, Long> entry : prios.entrySet()) {
				total += entry.getValue();
			}
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public Object predict(Map features) {
			Preconditions.checkState(total > 0, "Model is not successfully initialized");
			double max = 0;
			Object label = null;
			for (Object k : prios.keySet()) {
				double p = naiveBayes(k, features);
				if (p <= 0) {
					LOG.warn("get 0 for "+features+" with label "+k);
				}
				if (p > max) {
					label = k;
					max = p;
				}
			}
			if (label == null) {
				LOG.warn("Model cannot predict "+features+". fallback to randomization");
				final double g = 1.0 / prios.keySet().size();
				while (label == null) {
					for (Object k : prios.keySet()) {
						if (Math.random() <= g) {
							label = k;
							break;
						}
					}
				}
			}
			return label;
		}
		
		@SuppressWarnings("rawtypes")
		private double naiveBayes(Object label, Map features) {
			Map<Pair<String, String>, Long> pc = pconditionals.get(label);
			long N = prios.get(label);
			double r = 1.0 * N / total;
			N +=  pc.size();
			for (Object key : features.keySet()) {
				final String skey1 = key.toString();
				final String skey2 = features.get(key).toString();
				Long x = pc.get(new Pair<String, String>(skey1, skey2));
				x = x == null ? 1 : x + 1;
				r = r * x / N;
			}
			return r;
		}
		
		private Object deType(String label) {
			switch (type) {
			case DataType.INTEGER:
				return Integer.valueOf(label);
			case DataType.CHARARRAY:
				return label;
			default:
				throw new UnsupportedOperationException("TODO");
			}
		}
	}
	
	static <K> void increase(Map<K, Long> map, K k, long value) {
		Long old = map.get(k);
		map.put(k, value + (old != null ? old : 0));
	}

}
