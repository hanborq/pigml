package org.pigml.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.pigml.lang.FX;

/**
 * a light wrapper decorate tuple as Map<String, Double> 
 * @author g
 *
 */
public class TupleVector implements Map<String, Double> {
	private final Tuple tuple;
	public TupleVector(Tuple tuple) {
		this.tuple = tuple;
	}
	@Override
	public int size() {
		return tuple.size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		return index(key) < tuple.size();
	}

	@Override
	public boolean containsValue(Object value) {
		for (int i=0; i<tuple.size(); i++) {
			try {
				if (FX.equals(value, tuple.get(i))) {
					return true;
				}
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}
		return false;
	}

	@Override
	public Double get(Object key) {
		int k = index(key);
		try {
			return (Double) (k < tuple.size() ? tuple.get(k) : null);
		} catch (ExecException e) {
			throw new RuntimeException(e);
		}
	}
	
	private int index(Object key) {
		return Integer.getInteger(String.valueOf(key));
	}

	@Override
	public Double put(String key, Double value) {
		try {
			int k = index(key);
			Double p = (Double) tuple.get(k);
			tuple.set(k, value);
			return p;
		} catch (ExecException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Double remove(Object key) {
		return put(String.valueOf(key), null);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Double> m) {
		for (java.util.Map.Entry<? extends String, ? extends Double> e : m.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public Set<String> keySet() {
		Set<String> s = new HashSet<String>(tuple.size());
		for (int i=0; i<tuple.size(); i++) {
			try {
				if (!tuple.isNull(i)) {
					s.add(String.valueOf(i));
				}
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}
		return s;
	}

	@Override
	public Collection<Double> values() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public Set<java.util.Map.Entry<String, Double>> entrySet() {
		Set<java.util.Map.Entry<String, Double>> s = new HashSet<java.util.Map.Entry<String, Double>>(tuple.size());
		for (int i=0; i<tuple.size(); i++) {
			try {
				Double d = (Double) tuple.get(i);
				if (d != null) {
					s.add(entry(String.valueOf(i), d));
				}
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}
		return s;
	}

	private java.util.Map.Entry<String, Double> entry(final String key, final Double d) {
		return new java.util.Map.Entry<String, Double>(){
			@Override
			public String getKey() {
				return key;
			}
			@Override
			public Double getValue() {
				return d;
			}
			@Override
			public Double setValue(Double value) {
				throw new UnsupportedOperationException("TODO");
			}
		};
	}
}
