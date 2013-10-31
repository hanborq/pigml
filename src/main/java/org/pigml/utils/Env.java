package org.pigml.utils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.common.Defines;
import org.pigml.lang.FX;

import java.io.IOException;

public class Env {

    static public interface BackgroundProcedure {
        void execute(Configuration conf) throws IOException;
    }

    static public void inBackground(BackgroundProcedure proc) throws IOException {
        UDFContext uc = UDFContext.getUDFContext();
        if (!uc.isFrontend()) {
            proc.execute(Preconditions.checkNotNull(uc.getJobConf()));
        }
    }

    static public void setProperty(Class c, String key, String value) {
        UDFContext uc = UDFContext.getUDFContext();
        uc.getUDFProperties(c).setProperty(key, value);
    }

    static public void setProperty(Class c, String key, Object value) {
        UDFContext uc = UDFContext.getUDFContext();
        uc.getUDFProperties(c).setProperty(key, String.valueOf(value));
    }

    static public String getProperty(Class c, String key) {
        UDFContext uc = UDFContext.getUDFContext();
        return uc.getUDFProperties(c).getProperty(key);
    }

    static public <T> T getPropertyAs(Class c, String key, Function<String, T> as) {
        UDFContext uc = UDFContext.getUDFContext();
        return as.apply(getProperty(c, key));
    }

    //the part-id is the mapreduce task-id
    static public int getPartID() {
        //return UDFContext.getUDFContext().getJobConf().getInt(JobContext.TASK_PARTITION, -1);
        return getAttemptID().getTaskID().getId();
    }

    static public String identifyJob() {
        return getAttemptID().getJobID().toString();
    }

    static public String identifyTask() {
        return getAttemptID().getTaskID().toString();
    }

    static private TaskAttemptID getAttemptID() {
        return TaskAttemptID.forName(UDFContext.getUDFContext().getJobConf().get("mapred.task.id"));
    }

    static public String getJobDefine(Configuration conf, String name) {
        return getJobDefine(conf, name, Functors.AS_STRING);
    }
    static public <T> T getJobDefine(Configuration conf, String name, Function<String, T> map) {
        return getJobDefine(conf, name, map, null);
    }

    static public <T> T getJobDefine(Configuration conf, String name, Function<String, T> map, String defval) {
        String jobid = conf.get(Defines.PIGML_JOBID, "");
        String key = "pigml." + jobid + "." + name;
        String val = FX.any(conf.get(key), conf.get("pigml."+name), defval);
        return map.apply(Preconditions.checkNotNull(val, "Please define %s", key));
    }
}
