package com.baidu.rigel.hactalog;

public class ByteTestMain {

	public static void main(String[] args) {
//		byte i=113;
//	//      strBuf.append((char) (((bytes[i] >> 4) & 0xF) + ('a')));
//	  //    strBuf.append((char) (((bytes[i]) & 0xF) + ('a')));
//		System.out.println(i>>4);
//		System.out.println((i>>4)&0xF);
//		System.out.println(((i>>4)&0xF)+('a'));
//		System.out.println((char)(((i>>4)&0xF)+('a')));
//		System.out.println("**********************");
//		System.out.println(i);
		
		byte i=-128 ;
		while(i<=127){
			char hight=(char)(((i>>4)&0xF)+('a'));
			char low=(char)((i&0xF)+('a'));
			System.out.println(i+"  "+hight+"  "+low);
			i++;
		}
		
	}

}
