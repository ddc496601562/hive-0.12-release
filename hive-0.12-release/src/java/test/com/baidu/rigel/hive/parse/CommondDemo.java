package com.baidu.rigel.hive.parse;

public class CommondDemo {
	public static String commond1="insert into table  contline_revenue_day_orc partition(pdate='2013-09-28')"
                                 +"select /*+mapjoin(all_col)*/ "
			                     +"min(cut.contract_line_id) ,all_col.st_date as happy_date ,count(distinct cut.contract_line_id) "
			                     +"from "
			             		 +"(select contract_line_id ,st_date from analyse_db.contline_revenue_day_cut pph where pdate='2013-11-12') cut "
			             		 +"join "
			             		 +"(select contract_line_id ,st_date from analyse_db.contline_revenue_day_orc where pdate='2014-03-09') all_col "
			             		 +"on (cut.contract_line_id=all_col.contract_line_id and cut.st_date=all_col.st_date ) "
			             		 +"join "
			             		 +"(select contract_line_id ,st_date from analyse_db.contline_revenue_day_orc where pdate='2014-03-09') all_ppl "
			             		 +"on (cut.st_date=all_ppl.st_date ) "
			             		 +"group by 1  ,all_col.st_date  "
			             		 +"limit 1 ";
	public static String command2="insert overwrite table contline_revenue_day_orc select distinct contract_line_id ,st_date from analyse_db.contline_revenue_day_orc where prodline_id>100 limit 10 ";
	
	public static String command3="insert overwrite directory '/home/work/data/tmp'  "
			                     +"select "
			                     +"contract_line_id,st_date,pdate,sum(alb_cust_id) "
			                     +"from analyse_db.contline_revenue_day_cut "
			                     +"where pdate='2013-11-12' and prodline_id>10 "
			                     +"group by contract_line_id,st_date,pdate "
			                     +"sort by contract_line_id "
			                     +"limit 10";
	
	public static String command4="insert overwrite directory '/home/work/data/tmp'  "
            +"select "
            +"cc.contract_line_id,cc.st_date,cc.pdate,sum(cc.alb_cust_id) "
            +"from "
            + "analyse_db.contline_revenue_day_cut cc "
            + "left outer  join "
            + "analyse_db.contline_revenue_day_cut bb "
            + "on(cc.contract_line_id=bb.contract_line_id) "
            +"where cc.pdate='2013-11-12' and cc.prodline_id>10 "
            +"group by cc.contract_line_id,cc.st_date,cc.pdate  "
            +"sort  by cc.st_date   "
            +"limit 10";
}
