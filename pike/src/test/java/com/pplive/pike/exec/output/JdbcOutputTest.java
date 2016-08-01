package com.pplive.pike.exec.output;

import java.text.SimpleDateFormat;
import java.util.*;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.SizeAwareIterable;
import com.pplive.pike.base.Period;
import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.function.builtin.DateTime;

public class JdbcOutputTest {

//  MySQL scripts for the table test:	
//	CREATE TABLE `dac_traffic_fail` (
//			  `ID` int(11) NOT NULL AUTO_INCREMENT,
//			  `Type` int(11) NOT NULL DEFAULT '0',
//			  `TheID` int(11) NOT NULL,
//			  `TheName` varchar(50) DEFAULT NULL,
//			  `TheMonth` varchar(6) NOT NULL,
//			  `TheDate` date NOT NULL,
//			  `TheTime` varchar(6) DEFAULT NULL,
//			  `Mins` int(11) NOT NULL DEFAULT '0',
//			  `TheIndex` int(11) NOT NULL DEFAULT '0',
//			  `TheCount` bigint(21) NOT NULL DEFAULT '0',
//			  PRIMARY KEY (`ID`),
//			  KEY `TheDate` (`TheDate`),
//			  KEY `TheID` (`TheID`),
//			  KEY `TheMonth` (`TheMonth`)
//			) ENGINE=InnoDB AUTO_INCREMENT=37964435 DEFAULT CHARSET=utf8$$
	
	public static void main(String[] args) throws java.io.IOException {
		
		HashMap<String, String> conf = new HashMap<String, String>();
		conf.put(Configuration.OutputJdbcDriver, "com.mysql.jdbc.Driver");
		conf.put(Configuration.OutputJdbcDbUser, "root");
		conf.put(Configuration.OutputJdbcDbPassword, "123456");
		conf.put(Configuration.OutputJdbcDbUrl, "jdbc:mysql://localhost:3306/pplive_online?useUnicode=true&characterEncoding=utf-8");
		
		ArrayList<OutputField> fields = new ArrayList<OutputField>(10);
		fields.add(new OutputField("Type", ColumnType.Int));
		fields.add(new OutputField("TheID", ColumnType.Int));
		fields.add(new OutputField("TheName", ColumnType.String));
		fields.add(new OutputField("TheMonth", ColumnType.String));
		fields.add(new OutputField("TheDate", ColumnType.Date));
		fields.add(new OutputField("TheTime", ColumnType.String));
		fields.add(new OutputField("Mins", ColumnType.Int));
		fields.add(new OutputField("TheIndex", ColumnType.Int));
		fields.add(new OutputField("TheCount", ColumnType.Long));
		
		OutputSchema outputSchema = new OutputSchema("test", fields);

		JdbcOutput jdbcOutput = new JdbcOutput();
		jdbcOutput.init(conf, outputSchema,"dac_traffic_fail", Period.secondsOf(10));
		
		for(int n = 0; n < 100; n += 1) {
			List<Object> data = createData();
			List<List<Object>> coll = Arrays.asList(data);
			jdbcOutput.write(Calendar.getInstance(), SizeAwareIterable.of(coll));
		}
		
		jdbcOutput.close();
	}
	
	private static ArrayList<Object> createData(){
		java.sql.Time t = DateTime.CurTime.evaluate();
		ArrayList<Object> data = new ArrayList<Object>();
		data.add(Integer.valueOf(0)); // Type
		data.add(Integer.valueOf(1)); // TheID
		data.add(null); // TheName
		data.add(new SimpleDateFormat("yyyyMM").format(DateTime.CurDate.evaluate())); // TheMonth
		data.add(DateTime.CurDate.evaluate()); // TheDate
		data.add(new SimpleDateFormat("HH:mm").format(t)); // TheTime
		data.add(t.getHours() * 60 + t.getMinutes()); // Mins
		data.add((t.getHours() * 60 + t.getMinutes()) / 5); // TheIndex
		data.add(Long.valueOf(200)); // TheCount
		return data;
	}

}
