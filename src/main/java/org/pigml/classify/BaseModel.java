package org.pigml.classify;

import java.util.Map;

public interface BaseModel {
	void initialize(byte estimatedType, String para);
	void initEssemble(int numPara);
	void load(String[] para);
	void finish();
	Object predict(@SuppressWarnings("rawtypes") Map features);
}
