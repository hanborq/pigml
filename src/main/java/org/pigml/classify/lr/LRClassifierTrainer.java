package org.pigml.classify.lr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataType;
import org.pigml.classify.AbstractTrainer;
import org.pigml.classify.BaseModel;

import com.google.common.base.Preconditions;

public class LRClassifierTrainer extends AbstractTrainer {
	static final Log LOG = LogFactory.getLog(LRClassifierTrainer.class);
	private final double alpha;
	
	public LRClassifierTrainer() {
		this("0.1");
	}
	
	public LRClassifierTrainer(String alpha) {
		this.alpha = Double.valueOf(alpha);
	}
	
	private Map<Object, Double> theta = new HashMap<Object, Double>(1024, 0.5f);

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void trainModel(Object label, Map features) {
		final Map<Object, Double> newtheta = new HashMap<Object, Double>(features.size());
		for (Object key : features.keySet()) {
			final double value = (Double)features.get(key);
			double j = theta.containsKey(key) ? theta.get(key) : initialTheta();
			j = j + alpha * ((Integer)label - hypotheses(theta, features)) * value;
			newtheta.put(key, j);
		}
		theta.putAll(newtheta);
	}

	@Override
	protected byte getPredictType() {
		return DataType.INTEGER;
	}
	
	private double initialTheta() {
		return 0.0;
	}

	@Override
	protected Class<? extends BaseModel> getModelClass() {
		return Model.class;
	}

	@Override
	protected void saveModel() {
		for (Map.Entry<Object, Double> entry : theta.entrySet()) {
			persist(entry.getKey(), entry.getValue());
		}
	}
	
	static public class Model implements BaseModel {
		private List<Map<Object, Double>> models;
		@Override
		public void initialize(byte type, String para) {
			Preconditions.checkArgument(type == DataType.INTEGER);
			this.models = new ArrayList<Map<Object, Double>>();
		}

		@Override
		public void initEssemble(int numPara) {
			this.models.add(new HashMap<Object, Double>(numPara));
		}

		@Override
		public void finish() {
		}

		@Override
		public void load(String[] para) {
			Preconditions.checkState(models.size() > 0);
			Map<Object, Double> theta = models.get(models.size() - 1);
			theta.put(para[0], Double.valueOf(para[1]));
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public Object predict(Map x) {
			double h = 0;
			for (Map<Object, Double> m : models) {
				h += hypotheses(m, x);
			}
			return h >= 0.5 * models.size() ? 1 : 0;
		}
	}
	
	/*
	 * compute hypotheses h_theta(x), evaluate to (1+e**-(theta . X))**-1
	 */
	static private double hypotheses(Map<Object, Double> theta, Map<Object, Double> x) {
		double r = 0.0;
		for (Map.Entry<Object, Double> entry : x.entrySet()) {
			final Object key = entry.getKey();
			Double xi = theta.get(key);
			if (xi != null) {
				r += entry.getValue() * xi;
			}
		}
		r = 1.0 + Math.pow(Math.E, -r);
		return 1.0 / r; //Math.pow(r, -1);
	}
}
