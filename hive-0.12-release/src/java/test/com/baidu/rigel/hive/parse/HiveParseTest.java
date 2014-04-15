package com.baidu.rigel.hive.parse;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveParseTest {
	
	public static void main(String[] args) throws IOException, ParseException, SemanticException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		System.out.println( classLoader.getResource("hive-default.xml"));
		System.out.println( classLoader.getResource("hive-site.xml"));
		HiveConf hiveConf = new HiveConf(SessionState.class);
		SessionState.start(new SessionState(hiveConf));
		Context ctx = new Context(hiveConf);
		String commond=CommondDemo.command3;
		System.out.println(commond);
		ctx.setTryCount(10);
		ctx.setCmd(commond);
		ctx.setHDFSCleanup(true);
		ParseDriver pd = new ParseDriver();
		ASTNode tree = pd.parse(commond, ctx);
		tree = ParseUtils.findRootNonNullToken(tree);
		SemanticAnalyzer sem =(SemanticAnalyzer)SemanticAnalyzerFactory.get(hiveConf, tree);
		sem.analyze(tree, ctx);
		sem.validate();		
		PerfLogger perfLogger = PerfLogger.getPerfLogger();
		QueryPlan plan = new QueryPlan(commond, sem, perfLogger.getStartTime(PerfLogger.DRIVER_RUN));
		Schema schema = Driver.getSchema(sem, hiveConf);
		System.out.println(plan.getRootTasks().size());
	}
}
