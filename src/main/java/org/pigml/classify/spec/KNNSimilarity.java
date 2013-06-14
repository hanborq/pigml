package org.pigml.classify.spec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.Pair;
import org.pigml.utils.EuclideanUtils;
import org.pigml.utils.FieldSchemaUtils;

import com.google.common.base.Preconditions;

public class KNNSimilarity extends EvalFunc<DataBag> {
	private final BagFactory BF = DefaultBagFactory.getInstance();
	private final TupleFactory TF = TupleFactory.getInstance();
	private final int K;
	private PriorityQueue<Pair<Long, Double>> queue;
	
	public KNNSimilarity(String strk) {
		this.K = Integer.valueOf(strk);
		Preconditions.checkArgument(this.K > 0);
		this.queue = new PriorityQueue<Pair<Long, Double>>(this.K+1, new Comparator<Pair<Long, Double>>(){
			@Override
			public int compare(Pair<Long, Double> o1, Pair<Long, Double> o2) {
				double r = o1.getSecond() - o2.getSecond();
				return r < 0 ? -1 : r > 0 ? 1 : 0;
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public DataBag exec(Tuple t) throws IOException {
		if (t == null || t.size() == 0) {
			return null;
		}
		queue.clear();
		final Long id1 = (Long)t.get(0);
		final Map<String, Double> m1 = (Map<String, Double>)t.get(1);
		final double len1 = EuclideanUtils.length(m1);
		final Iterator<Tuple> b = ((DataBag)t.get(3)).iterator();
		while (b.hasNext()) {
			final Tuple it = b.next();
			final Long id2 = (Long)(it.get(0));
			final Map<String, Double> m2 = (Map<String, Double>)it.get(1);
			final double sim = EuclideanUtils.cosine(m1, m2, len1);
			if (sim > 0) {
				queue.add(new Pair<Long, Double>(id2, sim));
				if (queue.size() > K) {
					Pair<Long, Double> p = queue.remove();
					Preconditions.checkState(p.getSecond() <= sim);
				}
			}
		}
		final DataBag outputBag = BF.newDefaultBag();
		for (Pair<Long, Double> o : queue) {
			outputBag.add(TF.newTupleNoCopy(Arrays.asList(id1, o.getFirst(), o.getSecond())));
		}
		return outputBag;
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		FieldSchemaUtils.Assertor map = FieldSchemaUtils.mapAssertor(FieldSchemaUtils.IS_DOUBLE);
		FieldSchemaUtils.asserts(input, 
				FieldSchemaUtils.IS_LONG,
				map,
				FieldSchemaUtils.IGNORE,
				FieldSchemaUtils.bagAssertor(
						FieldSchemaUtils.IS_LONG, map));
		Schema s = new Schema(Arrays.asList(
				new Schema.FieldSchema("row", DataType.LONG),
				new Schema.FieldSchema("col", DataType.LONG),
				new Schema.FieldSchema("w", DataType.DOUBLE)));
		try {
			return new Schema(new Schema.FieldSchema(null, s, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
