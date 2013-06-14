package org.pigml.utils;

import java.util.HashMap;
import java.util.Map;

import org.pigml.lang.FX;


public class EuclideanUtils {
	
	static public <T extends Object> double length(Map<T, Double> v1) {
		double r = 0;
		for (Double v : v1.values()) {
			r += v * v;
		}
		return Math.sqrt(r);
	}
	
	static public <T extends Object> double product(Map<T, Double> v1, Map<T, Double> v2) {
		final Map<T, Double> small = v1.size() <= v2.size() ? v1 : v2;
		final Map<T, Double> big = small == v1 ? v2 : v1;
		double prod = 0;
		for (Map.Entry<? extends Object, Double> kv : small.entrySet()) {
			Double r = big.get(kv.getKey());
			if (r != null) {
				prod += kv.getValue() * r;
			}
		}
		return prod;
	}
	
	static public <T extends Object> double cosine(Map<T, Double> v1, Map<T, Double> v2) {
		return product(v1, v2) / (length(v1) * length(v2));
	}

	static public <T extends Object> double cosine(Map<T, Double> v1, Map<T, Double> v2, double lengthV1) {
		return product(v1, v2) / (lengthV1 * length(v2));
	}
	
	static public <T extends Object> double distance(Map<T, Double> v1, Map<T, Double> v2) {
		double r = 0;
		Map<T, Double> vv2 = new HashMap<T, Double>(v2);
		for (Map.Entry<T, Double> e : v1.entrySet()) {
			Double x = e.getValue() - FX.any(vv2.remove(e.getKey()), 0.0);
			r += x * x;
		}
		for (Map.Entry<T, Double> e : vv2.entrySet()) {
			r += e.getValue() * e.getValue();
		}
		return Math.sqrt(r);
	}
}
