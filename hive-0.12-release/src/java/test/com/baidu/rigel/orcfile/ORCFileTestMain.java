package com.baidu.rigel.orcfile;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct.OrcStructInspector;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class ORCFileTestMain {
	public static void main(String[] args) throws Exception {
		LineNumberReader lineReader=new LineNumberReader(new FileReader("D:/tmp_data/conf.conf"));
		Configuration conf = new Configuration(); 
		String line =null ;
		String keyConf=null;
		String valueConf=null;
		String split="						";
		while((line=lineReader.readLine())!=null){
			if(line.contains(split)){
				if(keyConf!=null&&valueConf!=null){
					conf.set(keyConf,valueConf);
					keyConf=null;
					valueConf=null;
				}
				keyConf=line.split(split)[0];
				valueConf=line.split(split)[1];
			}else{
				valueConf=valueConf+"\n"+line;
			}
		}
		conf.set("mapred.input.dir", "D:/tmp_data/000000_0");
		lineReader.close();
		FileSystem fs=FileSystem.getLocal(conf);
		Path orcFile=new Path("D:/tmp_data/000000_0");
		Reader fileReader=OrcFile.createReader(fs, orcFile);
		List<OrcProto.Type> types=fileReader.getTypes();
		System.out.println("***********************************");
		int i=-1 ;
		for(OrcProto.Type type:types){
			System.out.println((++i)+" "+type.getKind());
		}
		System.out.println(fileReader.getCompression());
		OrcStructInspector orcInspector=(OrcStructInspector)fileReader.getObjectInspector();
		List<StructField> fileds=orcInspector.getAllStructFieldRefs();
		for(StructField field:fileds){
			System.out.println(field.getFieldName()+"   "+field.getFieldObjectInspector());
		}
		JobConf job=new JobConf(conf);
		OrcInputFormat inputFormat=new OrcInputFormat();
		InputSplit[] splits=inputFormat.getSplits(job, 10);
		System.out.println(splits.length);
		int counter=0;
		RecordReader<NullWritable, OrcStruct> orcReader=inputFormat.getRecordReader(splits[0], job, null);
		NullWritable key=orcReader.createKey();
		OrcStruct value=orcReader.createValue();
		while(orcReader.next(key, value)){
			counter++;
		}
		orcReader.close();
		System.out.println(counter);
		
		
		
	}
}
