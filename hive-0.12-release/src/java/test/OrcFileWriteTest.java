import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.OrcRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat.OrcRecordWriter;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

public class OrcFileWriteTest {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		//reader  
		FileSystem fs=FileSystem.getLocal(conf);
		File orcFile=new File("D:/tmp_data/000000_0");
		int counter=0;
		RecordReader<NullWritable, OrcStruct> orcReader=
				new OrcRecordReader(OrcFile.createReader(fs, new Path(orcFile.getPath())), conf, 0,orcFile.length());
		NullWritable key=orcReader.createKey();
		OrcStruct value=orcReader.createValue();
		//writer 
		Properties tableProperties=new  Properties();
		tableProperties.put("columns.types", 
				"string:bigint:bigint:bigint:bigint:bigint:bigint:bigint:int:int:bigint:bigint:bigint:string:string:int:int:string:string:string:string:string:string:string:string:string:string:string:string:string:string:int:string:int:int:int:int");
		 OrcSerde serDe=new OrcSerde();
		 //conf参数其实没用
		 serDe.initialize(conf, tableProperties);
		 OrcFile.WriterOptions options = OrcFile.writerOptions(conf);
		 options.fileSystem(fs);
		 options.compress(CompressionKind.ZLIB);
		 FileSinkOperator.RecordWriter write=new OrcRecordWriter(new Path(""), options);
		 while(orcReader.next(key, value)&&counter<3000000){
				counter++;
				write.write(serDe.serialize(value, serDe.getObjectInspector()));
			}
			orcReader.close();
			System.out.println(counter);
			write.close(true);

			
			
	}

}
