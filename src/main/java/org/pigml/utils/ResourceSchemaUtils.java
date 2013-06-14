package org.pigml.utils;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Preconditions;

public class ResourceSchemaUtils {
	public interface SRSAssertor {
		void asserts(final Schema.FieldSchema sfs);
	}
	public interface Assertor {
		void asserts(final ResourceFieldSchema rfs) throws FrontendException;
	}
	static public final Assertor IGNORE = new Assertor() {
		@Override
		public void asserts(ResourceFieldSchema rfs) {
		}
	};
	static public final Assertor IS_LONG = typeAssertor(DataType.LONG);
	static public final Assertor IS_INTEGER = typeAssertor(DataType.INTEGER);
	static public final Assertor IS_DOUBLE = typeAssertor(DataType.DOUBLE);
	static public final Assertor IS_STRING = typeAssertor(DataType.CHARARRAY);
	static public final Assertor IS_FLOAT = typeAssertor(DataType.FLOAT);
	static public final Assertor IS_BYTES = typeAssertor(DataType.BYTEARRAY);
	static public final Assertor IS_BYTE = typeAssertor(DataType.BYTE);
	static public final Assertor IS_TUPLE = typeAssertor(DataType.TUPLE);
	static public final Assertor IS_MAP = typeAssertor(DataType.MAP);
	static public final Assertor IS_BAG = typeAssertor(DataType.BAG);
	
	static public Assertor typeAssertor(final byte type) {
		return new Assertor() {
			@Override
			public void asserts(final ResourceFieldSchema rfs) throws FrontendException {
				if (rfs.getType() != type) {
					throw new FrontendException(
							String.format("Expect '%s' to be type %d while got %d",
							rfs.getName(), type, DataType.findTypeName(rfs.getType())));
				}
			}
		};
	}

	static public Assertor tupleAssertor(final Assertor...fieldAssertors) {
		return new Assertor() {
			@Override
			public void asserts(final ResourceFieldSchema rfs) throws FrontendException {
				if (rfs.getType() != DataType.TUPLE) {
					throw new FrontendException(
							String.format("Expect '%s' to be a map while got %d",
									rfs.getName(), DataType.findTypeName(rfs.getType())));
				}
				ResourceSchemaUtils.asserts(rfs.getSchema(), fieldAssertors);
			}
		};
	}
	
	static public Assertor mapAssertor(final Assertor...fieldAssertors) {
		return new Assertor() {
			@Override
			public void asserts(final ResourceFieldSchema rfs) throws FrontendException {
				if (rfs.getType() != DataType.MAP) {
					throw new FrontendException(
							String.format("Expect '%s' to be a map while got %d",
									rfs.getName(), DataType.findTypeName(rfs.getType())));
				}
				ResourceSchemaUtils.asserts(rfs.getSchema(), fieldAssertors);
			}
		};
	}
	static public Assertor bagAssertor(final Assertor...fieldAssertors) {
		return new Assertor() {
			@Override
			public void asserts(final ResourceFieldSchema rfs) throws FrontendException {
				if (rfs.getType() != DataType.BAG) {
					throw new FrontendException(
							String.format("Expect '%s' to be a bag while got %d",
									rfs.getName(), DataType.findTypeName(rfs.getType())));
				}
				ResourceFieldSchema[] t = rfs.getSchema().getFields();
				Preconditions.checkArgument(t != null && t.length == 1 && t[0].getType() == DataType.TUPLE);
				ResourceSchemaUtils.asserts(t[0].getSchema(), fieldAssertors);
			}
		};
	}
	static public void asserts(ResourceSchema schema, final Assertor...assertors) throws FrontendException {
		ResourceFieldSchema[] rsf = schema.getFields();
		if (rsf == null || rsf.length < assertors.length) {
			throw new FrontendException(
					String.format("Expect %d fields for '%s' while got %s",
							assertors.length, "", rsf==null?"null":rsf.length));
		}
		for (int i=0; i<assertors.length; i++) {
			assertors[i].asserts(rsf[i]);
		}
	}
}
