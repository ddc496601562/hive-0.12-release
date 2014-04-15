package com.baidu.rigel.orcfile;

import org.apache.hadoop.hive.ql.io.orc.CompressionKind;

public class BytesMain {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(CompressionKind.valueOf("NONE"));
		System.out.println(CompressionKind.valueOf("ZLIB"));
		System.out.println(CompressionKind.valueOf("SNAPPY"));
		System.out.println(CompressionKind.valueOf("LZO"));
	}

}
