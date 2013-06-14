package org.pigml.utils;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Preconditions;

public class FieldSchemaUtils {
	public interface Assertor {
		void asserts(final Schema.FieldSchema rfs);
	}
	static public final Assertor IGNORE = new Assertor() {
		@Override
		public void asserts(Schema.FieldSchema rfs) {
		}
	};
	static public final Assertor IS_LONG = typeAssertor(DataType.LONG);
	static public final Assertor IS_INTEGER = typeAssertor(DataType.INTEGER);
	static public final Assertor IS_DOUBLE = typeAssertor(DataType.DOUBLE);
	static public final Assertor IS_STRING = typeAssertor(DataType.CHARARRAY);
	static public final Assertor IS_FLOAT = typeAssertor(DataType.FLOAT);
	static public final Assertor IS_BYTES = typeAssertor(DataType.BYTEARRAY);
	static public final Assertor IS_BYTE = typeAssertor(DataType.BYTE);
	
	static public Assertor typeAssertor(final byte type) {
		return new Assertor() {
			@Override
			public void asserts(final Schema.FieldSchema rfs) {
				if (rfs.type != type) {
					throw new BadSchemaException(
							String.format("Expect '%s' to be type %d while got %s",
							rfs.alias, type, DataType.findTypeName(rfs.type)));
				}
			}
		};
	}
	static public Assertor mapAssertor(final Assertor valueAssertors) {
		return new Assertor() {
			@Override
			public void asserts(final Schema.FieldSchema rfs) {
				if (rfs.type != DataType.MAP) {
					throw new BadSchemaException(
							String.format("Expect '%s' to be a map while got %s",
									rfs.alias, DataType.findTypeName(rfs.type)));
				}
				FieldSchemaUtils.asserts(rfs.schema, IS_STRING, valueAssertors);
			}
		};
	}
	static public Assertor bagAssertor(final Assertor...fieldAssertors) {
		return new Assertor() {
			@Override
			public void asserts(final Schema.FieldSchema rfs) {
				if (rfs.type != DataType.BAG) {
					throw new BadSchemaException(
							String.format("Expect '%s' to be a bag while got %s",
									rfs.alias, DataType.findTypeName(rfs.type)));
				}
				Schema.FieldSchema t;
				try {
					t = rfs.schema.getField(0);
				} catch (FrontendException e) {
					throw new BadSchemaException(e);
				}
				Preconditions.checkArgument(t != null && t.type == DataType.TUPLE);
				FieldSchemaUtils.asserts(t.schema, fieldAssertors);
			}
		};
	}
	static public void asserts(Schema schema, final Assertor...assertors) {
		if (schema.size() < assertors.length) {
			throw new BadSchemaException(
					String.format("Expect %d fields for '%s' while got %s",
							assertors.length, "", schema.size()));
		}
		for (int i=0; i<assertors.length; i++) {
			try {
				assertors[i].asserts(schema.getField(i));
			} catch (FrontendException e) {
				throw new BadSchemaException(e);
			}
		}
	}
	
	static public class BadSchemaException extends RuntimeException {

		public BadSchemaException(String m) {
			super(m);
		}

		public BadSchemaException(FrontendException e) {
			super(e);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
	}
}
