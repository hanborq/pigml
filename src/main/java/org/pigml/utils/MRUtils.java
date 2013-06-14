package org.pigml.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.impl.util.UDFContext;

public class MRUtils {
	
	static public String identifyJob() {
		return getAttemptID().getJobID().toString();
	}
	static public String identifyTask() {
		return getAttemptID().getTaskID().toString();
	}
	static private TaskAttemptID getAttemptID() {
		return TaskAttemptID.forName(getRuntimeConf("mapred.task.id"));
	}
	static public String getTmpDir() {
		return getRuntimeConf("hadoop.tmp.dir");
	}
	static public String getRuntimeConf(String key) {
		return UDFContext.getUDFContext().getJobConf().get(key);
	}
	static public Configuration getConf() {
		return UDFContext.getUDFContext().getJobConf();
	}
}
