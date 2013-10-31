package org.pigml.lang;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.pig.data.Tuple;

public class FX {
	//private static final Logger LOG = LoggerFactory.getLogger(FUNCTIONS.class);

    static public <T> T[] repeat(T t, int size) {
        T[] res = (T[]) Array.newInstance(t.getClass(), size);
        for (int i=0; i<size; i++) {
            res[i] = t;
        }
        return res;
    }

    static public <T> T get(Iterator<T> itor, Predicate<? super T> filt) {
        if (itor != null) {
            while (itor.hasNext()) {
                T n = (T) itor.next();
                if (filt.apply(n)) {
                    return n;
                }
            }
        }
        return null;
    }

	static public <F, T> boolean transitiveSatisfy(Collection<F> collect,
			Function<F, T> transform, Function<Pair<T, T>, Boolean> test) {
		if (collect.size() > 1) {
			Iterator<F> its = collect.iterator();
			T t1 = transform.apply(its.next());
			while (its.hasNext()) {
				T t2 = transform.apply(its.next());
				if (!test.apply(new Pair<T, T>(t1, t2))) {
					return false;
				}
			}
		}
		return true;
	}

	static public <K> void increase(Map<K, Integer> map, K k, int value) {
		Integer old = map.get(k);
		map.put(k, value + (old != null ? old : 0));
	}
	

	static <K> void increase(Map<K, Long> map, K k, long value) {
		Long old = map.get(k);
		map.put(k, value + (old != null ? old : 0));
	}
	
	static public <T extends Comparable<T>> List<T> sort(Collection<T> elements) {
		List<T> list = new ArrayList<T>(elements);
		Collections.sort(list);
		return list;
	}
	
	static public <T> Set<T> intersect(Collection<T> A, Collection<T> B) {
		Set<T> set = new HashSet<T>(A);
		set.retainAll(B);
		return set;
	}
	
	static public <T> Set<T> substract(Collection<T> from, Collection<T> by) {
		Set<T> set = new HashSet<T>(from);
		set.removeAll(by);
		return set;
	}
	
	static public Object varargs(Class<?> clazz, int length) {
		return Array.newInstance(clazz, length);
	}
	static public Object varargs(Object[] input) {
		Object result = Array.newInstance(Object.class, input.length);
		for (int i=0; i<input.length; i++) {
			Array.set(result, i, input[i]);
		}
		return result;
	}
	static public <T> Object varargs(Class<T> clazz, T[] input) {
		Object result = Array.newInstance(clazz, input.length);
		for (int i=0; i<input.length; i++) {
			Array.set(result, i, input[i]);
		}
		return result;
	}
	static public <T, C extends Collection<T>> C addAll(C collection, T... elements) {
		for (T e : elements) {
			collection.add(e);
		}
		return collection;
	}
	static public <T, C extends Collection<T>> C removeAll(C collection, T... elements) {
		for (T e : elements) {
			collection.remove(e);
		}
		return collection;
	}
	static public <T, C extends Collection<T>> C removeAllBut(C collection, T[] elements, T[] exceptions) {
		for (T e : elements) {
			if (index(exceptions, e) < 0) {
				collection.remove(e);
			}
		}
		return collection;
	}
	
	/**
	 * 
	 * @param from starting value, inclusive
	 * @param to ending value, exclusive
	 * @return an array of generated integers
	 */
	static public Integer[] series(int from, int to) {
		return series(from, to, to >= from ? 1 : -1);
	}
	
	/**
	 * 
	 * @param from starting value, inclusive
	 * @param to ending value, exclusive
	 * @param step a non-zero step size
	 * @return an array of generated integers
	 */
	static public Integer[] series(int from, int to, int step) {
		final int N = (to - from) / step;
		Integer[] r = new Integer[N];
		for (int i=0; i<N; i++) {
			r[i] = from + i * step;
		}
		return r;
	}
	static public boolean isEmpty(Collection<?> collection) {
		return collection == null || collection.size() == 0;
	}
	static public <T> boolean isEmpty(T[] array) {
		return array == null || array.length == 0;
	}
	static public <T> int index(T[] array, T e) {
		int index = -1;
		for (int i=0; i<array.length; i++) {
			T t = array[i];
			if (t == e || (e != null && e.equals(t))) {
				index = i;
				break;
			}
		}
		return index;
	}
	static public <T> int[] index(List<T> list, T... elements) {
		int[] result = new int[elements.length];
		for (int i=0; i<result.length; i++) {
			result[i] = list.indexOf(elements[i]);
		}
		return result;
	}
	static public <T> int[] index(List<T> list, Iterator<T> itor) {
		List<Integer> result = new ArrayList<Integer>();
		for (; itor.hasNext(); ) {
			result.add(list.indexOf(itor.next()));
		}
		int[] r = new int[result.size()];
		int i = 0;
		for (Integer x : result) {
			r[i++] = x;
		}
		return r;
	}
	static public <T> T[] copyByIndices(T[] from, int[] fromIndices, T[] to, int[] toIndices) {
		Preconditions.checkArgument(fromIndices.length == toIndices.length);
		for (int i=0; i<fromIndices.length; i++) {
			to[toIndices[i]] = from[fromIndices[i]];
		}
		return to;
	}
	static public <T> T[] copyByIndices(T[] from, T[] to, int[] toIndices) {
		for (int i=0; i<toIndices.length; i++) {
			to[toIndices[i]] = from[i];
		}
		return to;
	}
	static public <T> T[] copyByIndices(T[] from, int[] fromIndices, T[] to) {
		for (int i=0; i<fromIndices.length; i++) {
			to[i] = from[fromIndices[i]];
		}
		return to;
	}
	
	/*static public <K, V> Map<K, V> mapDrop(Map<K, V> map, Collection<K> excepts) {
		for (K k : excepts) {
			map.remove(k);
		}
		return map;
	}*/
	static public <K, V> Map<K, V> mapExtract(Map<K, V> src, Map<K, V> dst, Collection<K> keys) {
		for (K k : keys) {
			if (src.containsKey(k)) {
				dst.put(k, src.get(k));
			}
		}
		return dst;
	}
	static public <K, V> Map<V, K> mapInvert(Map<K, V> map) {
		Map<V, K> result = new LinkedHashMap<V, K>();
		for (Map.Entry<K, V> e : map.entrySet()) {
			result.put(e.getValue(), e.getKey());
		}
		return result;
	}
	static public <T extends Comparable<T>> T max(Collection<T> col) {
		List<T> list = new ArrayList<T>(col);
		Collections.sort(list);
		return list.get(list.size() - 1);
	}
	static public <K, V1, V2> Map<V1, V2> joinmap(Map<K, V1> first, Map<K, V2> last) {
		Map<V1, V2> out = new LinkedHashMap<V1, V2>();
		for (Map.Entry<K, V2> entry : last.entrySet()) {
			V1 k = first.get(entry.getKey());
			V2 v = entry.getValue();
			Preconditions.checkState(k != null && v != null);
			out.put(k, v);
		}
		return out;
	}
	static public <K, V, M extends Map<K, V>> M enmap(M map, K[] keys, V[] values) {
		return enmap(map, iteratorOf(keys), iteratorOf(values));
	}
	static public <K, V, M extends Map<K, V>> M enmap(M map, Iterator<K> keys, Iterator<V> values) {
		for (; keys.hasNext(); ) {
			map.put(keys.next(), values.next());
		}
		Preconditions.checkState(!values.hasNext());
		return map;
	}
	static public <K, V> void demap(Map<K, V> map, Iterable<K> keys) {
		for (K k : keys) {
			map.remove(k);
		}
	}
	static public <K, V> void alias(Map<K, V> map, K key, K alias) {
		Preconditions.checkArgument(map.containsKey(key) && !map.containsKey(alias));
		map.put(alias, map.get(key));
	}
	
	static public <T> List<T> asList(T[] array) {
		List<T> list = new ArrayList<T>(array.length);
		for (T t : array) {
			list.add(t);
		}
		return list;
	}
	
	static public <F, T> T reflect(F[] froms, F value, T[] tos, T defval) {
		Preconditions.checkArgument(froms.length == tos.length);
		for (int i=0; i<froms.length; i++) {
			if ((value == froms[i]) || (value != null && value.equals(froms[i])))
				return tos[i];
		}
		return defval;
	}
	
	static public <F, T> Object transform(F[] froms, Class<?> clazz, Function<F, T> function) {
		Object varargs = varargs(clazz, froms.length);
		for (int i=0; i<froms.length; i++) {
			Array.set(varargs, i, function.apply(froms[i]));
		}
		return varargs;
	}
	static public <F, T> T[] transform(F[] froms, T[] tos, Function<F, T> function) {
		for (int i=0; i<froms.length; i++) {
			tos[i] = function.apply(froms[i]);
		}
		return tos;
	}
	static public <F, T> List<T> extract(Collection<F> col, Function<F, T> function) {
		List<T> list = new ArrayList<T>();
		for (F v : col) {
			T t = function.apply(v);
			if (t != null) {
				list.add(t);
			}
		}
		return list;
	}
	static public <T> List<T> filter(Iterable<T> col, Function<T, Boolean> function) {
		List<T> list = new ArrayList<T>();
		for (T v : col) {
			if (function.apply(v)) {
				list.add(v);
			}
		}
		return list;
	}
	static public <T> boolean among(T target, T... arrays) {
		for (T t : arrays) {
			if (t == target || (t != null && t.equals(target)))
				return true;
		}
		return false;
	}
	static public boolean valid(String... ss) {
		for (String s : ss)
			if (s == null || s.length() == 0)
				return false;
		return true;
	}
	static public <T> boolean valid(T[] array, int index) {
		return array != null && array.length > index && array[index] != null;
	}
	static public <T> T any(T... candidates) {
		for (T t : candidates) {
			if (t != null)
				return t;
		} 
		return null;
	}

    static public <T> T any(Callable<T>... candidates) {
        for (Callable<T> c : candidates) {
            try {
                T t = c.call();
                if (t != null) {
                    return t;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    static public <T> T lazyResolve(T t, Callable<T> lazy, T... skips) {
        try {
            if (t != null) {
                for (T x : skips) {
                    if (t == x) {
                        return lazy.call();
                    }
                }
                return t;
            }
            return lazy.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
	static public interface Callback <R, T> {
		void call(R r, T t, int index);
	}
	static public <T, R> R foreach(R r, T[] array, Callback<R, T> f) {
		for (int i=0; i<array.length; i++) {
			f.call(r, array[i], i);
		}
		return r;
	}
	static public <T> T[] foreach(T[] array, Function<T, T> f) {
		if (array != null) {
			for (int i=0; i<array.length; i++) {
				array[i] = f.apply(array[i]);
			}
		}
		return array;
	}
	static public <T> int until(T[] array, Function<T, Boolean> f) {
		for (int i=0; i<array.length; i++) {
			if (f.apply(array[i]))
				return i;
		}
		return -1;
	}
	static public <T> T find(T[] array, Function<T, Boolean> f) {
		for (T t : array) {
			if (f.apply(t))
				return t;
		}
		return null;
	}
	static public <T> T find(Iterable<T> itor, Function<T, Boolean> f) {
		for (T t : itor) {
			if (f.apply(t))
				return t;
		}
		return null;
	}
	
	static public <T> T[] add(T[] arr1, T[] arr2) {
		T[] result = Arrays.copyOf(arr1, arr1.length + arr2.length);
		for (int i=0; i<arr2.length; i++) {
			result[arr1.length+i] = arr2[i];
		}
		return result;
	}
	
	static public <T> Iterator<T> iteratorOf(final T[] array) {
		return new Iterator<T>(){
			int pos;
			@Override
			public boolean hasNext() {
				return pos < array.length;
			}
			@Override
			public T next() {
				return array[pos++];
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	static public Iterator<Character> iteratorOf(final String string) {
		return new Iterator<Character>(){
			int index = 0;
			@Override
			public boolean hasNext() {
				return index < string.length();
			}
			@Override
			public Character next() {
				return string.charAt(index++);
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			@Override
			public String toString() {
				return string.substring(index);
			}
		};
	}
	
	static public <T> T modulo(T[] array, int index) {
		return array[index % array.length];
	}
	
	static public List<String> readlines(final String file) throws IOException {
		List<String> lines = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			lines.add(line);
		}
		reader.close();
		return lines;
	}
	
	static public void writelines(final String file, List<String> lines) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		for (String line : lines) {
			writer.write(line);
			writer.write("\n");
		}
		writer.close();
	}
	
	static public <T> boolean equals(Object o1, Object o2) {
		return o1 != null ? o1.equals(o2) : o1 == o2;
	}
	static public <T> boolean equals(T[] arr1, T[] arr2) {
		if (arr1.length == arr2.length) {
			for (int i=0; i<arr1.length; i++) {
				if (!equals(arr1[i], arr2[i]))
					return false;
			}
			return true;
		}
		return false;
	}
	
	static public <T> boolean containsAt(T[] arr1, int index1, T... arr2) {
		if (arr2.length <= arr1.length - index1) {
			for (int i=0; i<arr2.length; i++) {
				if (!equals(arr1[index1 + i], arr2[i])) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	static public int roundup(int n, int by) {
		return (n + by - 1) / by * by;
	}
	
	static public int confine(int value, int min, int max) {
		return value < min ? min : (value > max ? max : value);
	}
	
	static public byte[] concat(byte[]... bytes) {
		int n = 0;
		for (byte[] bs : bytes) {
			n += bs.length;
		}
		byte[] result = new byte[n];
		int idx = 0;
		for (byte[] bs : bytes) {
			System.arraycopy(bs, 0, result, idx, bs.length);
			idx += bs.length;
		}
		return result;
	}
	
	static public long randlong() { //should be stronger than java.utils.Random
		UUID uuid = UUID.randomUUID();
		return uuid.getMostSignificantBits() * 1076691823 + uuid.getLeastSignificantBits();
	}
}
