
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.OrcRecordReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

public class OrcFileReadTest {
	public static void main(String[] args) throws Exception {
		LineNumberReader lineReader=new LineNumberReader(new FileReader("/home/work/local/hive-0.12.0-bin/orc_conf/conf.conf"));
		Configuration conf = new Configuration(); 
		String line =null ;
		String keyConf=null;
		String valueConf=null;
		String split="                                         ";
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
		lineReader.close();
		FileSystem fs=FileSystem.getLocal(conf);
		File orcFile=new File(
				"/home/work/data/hive/warehouse/analyse_db.db/dwd_log_fc_click_pay_info_orc/pdate=2014-03-17/000000_0"
				);
		Reader fileReader=OrcFile.createReader(fs, new Path(orcFile.getPath()));
		StructObjectInspector insp = (StructObjectInspector) fileReader.getObjectInspector();
		System.out.println("getCompression "+fileReader.getCompression());
		int stripeCounter= 0 ;
		Iterable<StripeInformation> iterable=fileReader.getStripes();
		for(StripeInformation stripe:iterable){
			stripeCounter++ ;
			System.out.println(stripe.getDataLength()+" "+stripe.getIndexLength()+" "+stripe.getFooterLength()+" "+stripe.getNumberOfRows());
		}
		System.out.println("stripeCounter "+stripeCounter);	
		int counter=0;
		conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
		RecordReader<NullWritable, OrcStruct> orcReader=
				new OrcRecordReader(OrcFile.createReader(fs, new Path(orcFile.getPath())), conf, 0,orcFile.length());
		NullWritable key=orcReader.createKey();
		OrcStruct value=orcReader.createValue();
		int[] count_sum=new int[10];
		long start=System.currentTimeMillis();
		while(orcReader.next(key, value)){
			counter++;
		}
		orcReader.close();
		System.out.println(counter);
		System.out.println("cost is "+(System.currentTimeMillis()-start)/1000);
		for(int i=0;i<count_sum.length;i++)
			System.out.println(i+" "+count_sum[i]);
	}
}