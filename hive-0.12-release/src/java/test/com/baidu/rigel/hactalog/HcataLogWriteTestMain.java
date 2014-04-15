package com.baidu.rigel.hactalog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.FileOutputCommitterContainer;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class HcataLogWriteTestMain {

	public static void main(String[] args) throws IOException, InterruptedException {
		Map<String,String> partitions = new HashMap<String, String>(1);
		partitions.put("pdate", "2011-09-24");
		Configuration conf =new Configuration();
		Job job = new Job(conf, "GroupByAge");
		OutputJobInfo outPutInfo=OutputJobInfo.create("analyse_db","contline_revenue_day_cut", partitions);
        HCatOutputFormat.setOutput(job, outPutInfo);
        HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
        System.out.println("INFO: output schema explicitly set for writing:"
                + s);
        //同样，序列化，然后设置到conf配置中  
        HCatOutputFormat.setSchema(job, s);
        HCatOutputFormat outFormat= new HCatOutputFormat();
        // attempt_20140328173000_500575_m000_000000_0     task_20131314_0758_r_000100 
        job.getConfiguration().set("mapred.task.partition", "1");
        TaskAttemptID tmp= new TaskAttemptID("20140328173000",500575,true,0,0);
        job.getConfiguration().set("mapred.task.id", "attempt_20140328173000_500575_m_000000_0");
		TaskAttemptContext taskTmp=new TaskAttemptContext(job.getConfiguration(),tmp);
        RecordWriter<WritableComparable<?>, HCatRecord> hcataWrite=outFormat.getRecordWriter(taskTmp);
        hcataWrite.close(null);
        for(int i=0;i<100;i++){
        	HCatRecord record = new DefaultHCatRecord(s.getFields().size());
        	for(int k=0;k<s.getFields().size();k++){
        		if(s.get(k).getType()==Type.BIGINT)
        			record.set(k, Long.parseLong(i+""));
        		else if(s.get(k).getType()==Type.STRING)
        			record.set(k, i+"_ddctest");
        	}
        	hcataWrite.write(null, record);
        }
        hcataWrite.close(taskTmp);
        
//		System.out.println(outPutInfo.getDatabaseName());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getHarRequested());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getLocation());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getMaxDynamicPartitions());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getTableName());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getOutputSchema());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getPartitionValues());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getProperties());
//		System.out.println("******************************************************");
//		System.out.println(outPutInfo.getTableInfo());
	}

}
