package org.pigml.utils;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.binarysortable.InputByteBuffer;
import org.apache.hadoop.hive.serde2.binarysortable.OutputByteBuffer;

//from BinarySortableSerDe
public class BinarySortableUtils {
	
	static public void serialize(OutputByteBuffer buffer, int v, boolean invert) {
		buffer.write((byte) ((v >> 24) ^ 0x80), invert);
		buffer.write((byte) (v >> 16), invert);
		buffer.write((byte) (v >> 8), invert);
		buffer.write((byte) v, invert);
	}
	
	static public void serialize(OutputByteBuffer buffer, long v, boolean invert) {
		buffer.write((byte) ((v >> 56) ^ 0x80), invert);
		buffer.write((byte) (v >> 48), invert);
		buffer.write((byte) (v >> 40), invert);
		buffer.write((byte) (v >> 32), invert);
		buffer.write((byte) (v >> 24), invert);
		buffer.write((byte) (v >> 16), invert);
		buffer.write((byte) (v >> 8), invert);
		buffer.write((byte) v, invert);
	}
	
	static public void serialize(OutputByteBuffer buffer, double r, boolean invert) {
		long v = Double.doubleToLongBits((Double) r);
		if ((v & (1L << 63)) != 0) {
			v = ~v;
		} else {
			v = v ^ (1L << 63);
		}
		buffer.write((byte) (v >> 56), invert);
		buffer.write((byte) (v >> 48), invert);
		buffer.write((byte) (v >> 40), invert);
		buffer.write((byte) (v >> 32), invert);
		buffer.write((byte) (v >> 24), invert);
		buffer.write((byte) (v >> 16), invert);
		buffer.write((byte) (v >> 8), invert);
		buffer.write((byte) v, invert);
	}
	
	static public void serialize(OutputByteBuffer buffer, float r, boolean invert) {
		int v = Float.floatToIntBits((Float) r);
		if ((v & (1 << 31)) != 0) {
			v = ~v;
		} else {
			v = v ^ (1 << 31);
		}
		buffer.write((byte) (v >> 24), invert);
		buffer.write((byte) (v >> 16), invert);
		buffer.write((byte) (v >> 8), invert);
		buffer.write((byte) v, invert);
	}

	static public void serialize(OutputByteBuffer buffer, String string, boolean invert) {
		byte[] bytes = string.getBytes();
		serializeBytes(buffer, bytes, bytes.length, invert);
	}
	
	static public void serialize(OutputByteBuffer buffer, boolean v, boolean invert) {
		buffer.write((byte) (v ? 2 : 1), invert);
	}
	
	static public void serialize(OutputByteBuffer buffer, byte[] toSer, boolean invert) {
		serializeBytes(buffer, toSer, toSer.length, invert);
	}
	
	private static void serializeBytes(OutputByteBuffer buffer, byte[] data,
			int length, boolean invert) {
		for (int i = 0; i < length; i++) {
			if (data[i] == 0 || data[i] == 1) {
				buffer.write((byte) 1, invert);
				buffer.write((byte) (data[i] + 1), invert);
			} else {
				buffer.write(data[i], invert);
			}
		}
		buffer.write((byte) 0, invert);
	}

	static public int deserializeInt(InputByteBuffer buffer, boolean invert) throws IOException {
		int v = buffer.read(invert) ^ 0x80;
		for (int i = 0; i < 3; i++) {
			v = (v << 8) + (buffer.read(invert) & 0xff);
		}
		return v;
	}
	
	static public double deserializeDouble(InputByteBuffer buffer, boolean invert) throws IOException {
		long v = 0;
		for (int i = 0; i < 8; i++) {
			v = (v << 8) + (buffer.read(invert) & 0xff);
		}
		if ((v & (1L << 63)) == 0) {
			// negative number, flip all bits
			v = ~v;
		} else {
			// positive number, flip the first bit
			v = v ^ (1L << 63);
		}
		return Double.longBitsToDouble(v);
	}
}
