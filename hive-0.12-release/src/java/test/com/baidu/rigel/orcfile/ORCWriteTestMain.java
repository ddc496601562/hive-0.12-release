package com.baidu.rigel.orcfile;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class ORCWriteTestMain {

	public static void main(String[] args) throws IOException, SerDeException {
		 
		Configuration conf = new Configuration(); 
		conf.set("mapred.input.dir", "D:/tmp_data/000000_0");
		JobConf job=new JobConf(conf);
		OrcInputFormat inputFormat=new OrcInputFormat();
		InputSplit[] splits=inputFormat.getSplits(job, 10);
		System.out.println(splits.length);
		int counter=0;
		RecordReader<NullWritable, OrcStruct> orcReader=inputFormat.getRecordReader(splits[0], job, null);
		NullWritable key=orcReader.createKey();
		OrcStruct value=orcReader.createValue();
	
		
		//write  
		Properties tableProperties=new  Properties();
		 LineNumberReader lineReader=new LineNumberReader(new FileReader("D:/tmp_data/tableProperties.conf"));
		 String line =null ;
		 while((line=lineReader.readLine())!=null&&!line.equals("")){
			 String[] key_val=line.split("						");
			 tableProperties.put(key_val[0], key_val[1]);
		 }
		 lineReader.close();
		 OrcSerde serDe=new OrcSerde();
		 //conf参数其实没用
		 serDe.initialize(conf, tableProperties);
		Path orcFile=new Path("D:/tmp_data/write_out_snappy");
		OrcOutputFormat orcOutput=new OrcOutputFormat();
		FileSinkOperator.RecordWriter write=orcOutput.getHiveRecordWriter(new JobConf(conf), orcFile, null, true, tableProperties, null);
		
		while(orcReader.next(key, value)){
			counter++;
			write.write(serDe.serialize(value, serDe.getObjectInspector()));
		}
		orcReader.close();
		System.out.println(counter);
		write.close(true);
		
		
	}

}
