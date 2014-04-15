package com.baidu.rigel.io.serde;

import java.util.Properties;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

public class OreSeDeTestMain {

	public static void main(String[] args) throws SerDeException {
		Properties tableProperties=new  Properties();
		tableProperties.put("columns.types", 
				"string:bigint:bigint:bigint:bigint:bigint:bigint:bigint:int:int:bigint:bigint:bigint:string:string:int:int:string:string:string:string:string:string:string:string:string:string:string:string:string:string:int:string:int:int:int:int");
		 OrcSerde serDe=new OrcSerde();
		 //conf参数其实没用
		 serDe.initialize(null, tableProperties);
		 
		 StructObjectInspector rowObjectSpector=(StructObjectInspector)serDe.getObjectInspector();
		 
		 serDe.deserialize(null);
		 serDe.serialize(null, null);
		 
		 
		 SerDe  serDeAbstract=new AvroSerDe();
		 serDeAbstract.initialize(null, tableProperties);
		 Class<? extends Writable> serDestClassWritable=serDeAbstract.getSerializedClass();
		 
		 serDeAbstract.serialize(null, null);
		 
	}

}
