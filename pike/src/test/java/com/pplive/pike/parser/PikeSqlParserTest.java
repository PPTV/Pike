package com.pplive.pike.parser;


import net.sf.jsqlparser.JSQLParserException;

import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.metadata.TextFileTableInfoProvider;
import com.pplive.pike.parser.LogicalQueryPlan;
import com.pplive.pike.parser.ParseErrorsException;
import com.pplive.pike.parser.PikeSqlParser;
import com.pplive.pike.parser.SemanticErrorException;

public class PikeSqlParserTest {
		
	public static void main(String[] args) {
		
		testParser(args);
	}
	
	private static void testParser(String[] args) {
		TextFileTableInfoProvider tableInfoProvider = new TextFileTableInfoProvider("tableInfoXml.txt");
		TableManager tableManager = new TableManager(tableInfoProvider);
		
		try {
			LogicalQueryPlan queryPlan = PikeSqlParser.parseSQL(args[0], tableManager);
			System.out.println(queryPlan.toExplainString());
		}
		catch(JSQLParserException e){
			System.out.println("sql syntax  error:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
		}
		catch(SemanticErrorException e){
			System.out.println("sql semantic  error:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
		}
		catch(UnsupportedOperationException e){
			System.out.println("sql implementation problem:");
			System.out.println(e.getCause() != null ? e.getCause() : e);
		}
		catch(ParseErrorsException e){
			System.out.println("there are errors.");
			for(Exception err : e.getParseErrors()){
				System.out.println(err.getCause() != null ? err.getCause() : err);
			}
		}
	}

}
