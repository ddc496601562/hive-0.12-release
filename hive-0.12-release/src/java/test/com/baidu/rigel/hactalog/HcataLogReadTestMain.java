package com.baidu.rigel.hactalog;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

public class HcataLogReadTestMain {

	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = new  Configuration();
		JobConf job=new JobConf(conf);
//		InputJobInfo inputInfo=InputJobInfo.create("analyse_db", "contline_revenue_day","pdate=\"2014-03-09\"",new Properties());
		HCatInputFormat hIntput=HCatInputFormat.setInput(job, "analyse_db", "contline_revenue_day");
		hIntput.setFilter("pdate=\"2014-03-09\"");
		Job jobContext=new Job(job);
		System.out.println(job.get(HCatConstants.HCAT_KEY_JOB_INFO));
		hIntput=new HCatInputFormat();
		List<InputSplit> splitList=hIntput.getSplits(jobContext);
		HCatSchema hCatSchema=HCatInputFormat.getTableSchema(job);
		System.out.println(splitList.size());
		TaskAttemptID tmp= new TaskAttemptID("20131314",758,false,100,200);
		TaskAttemptContext taskTmp=new TaskAttemptContext(job,tmp);
		RecordReader<WritableComparable, HCatRecord> reader=hIntput.createRecordReader(splitList.get(0), taskTmp);
		reader.initialize(splitList.get(0), taskTmp);
		int cout=0 ;
		while(reader.nextKeyValue()){
			HCatRecord row=reader.getCurrentValue();
			System.out.println(row.get("click_amt", hCatSchema)+"    "+row.get("pdate", hCatSchema));
			cout++  ;
		}
		System.out.println(cout);
	}

}
