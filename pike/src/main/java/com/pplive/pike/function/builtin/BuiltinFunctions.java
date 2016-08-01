package com.pplive.pike.function.builtin;

import java.util.HashMap;
import java.util.List;

import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.base.CaseIgnoredString;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.expression.FunctionExpression;

public class BuiltinFunctions {

	private static HashMap<CaseIgnoredString, IBuiltinFunctionParser> _functions;
	
	static {
		_functions = new HashMap<CaseIgnoredString, IBuiltinFunctionParser>();
		
		_functions.put(new CaseIgnoredString("Convert"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Boolean"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("String"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Str"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Byte"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Short"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Int"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Long"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Float"), new Convert.Parser());
		_functions.put(new CaseIgnoredString("Double"), new Convert.Parser());
		
		_functions.put(new CaseIgnoredString("MapGet"), new DefaultParser(MapGet.class));
        _functions.put(new CaseIgnoredString("Get"), new DefaultParser(MapGet.class));
        _functions.put(new CaseIgnoredString("GetField"), new DefaultParser(Struct.GetField.class));

		_functions.put(new CaseIgnoredString("Hash"), new DefaultParser(Hash.class));
		
		_functions.put(new CaseIgnoredString("StrEqual"), new DefaultParser(Str.StrEqual.class));
		_functions.put(new CaseIgnoredString("StrNotEqual"), new DefaultParser(Str.StrNotEqual.class));
		_functions.put(new CaseIgnoredString("StrCompare"), new DefaultParser(Str.StrCompare.class));
		_functions.put(new CaseIgnoredString("StrLen"), new DefaultParser(Str.StrLen.class));
		_functions.put(new CaseIgnoredString("StrBeginWith"), new DefaultParser(Str.StrBeginWith.class));
		_functions.put(new CaseIgnoredString("StrEndsWith"), new DefaultParser(Str.StrEndsWith.class));
		_functions.put(new CaseIgnoredString("StrIsNullOrEmpty"), new DefaultParser(Str.StrIsNullOrEmpty.class));
		_functions.put(new CaseIgnoredString("StrIsNotEmpty"), new DefaultParser(Str.StrIsNotEmpty.class));
		_functions.put(new CaseIgnoredString("StrLeft"), new DefaultParser(Str.StrLeft.class));
		_functions.put(new CaseIgnoredString("StrIndex"), new DefaultParser(Str.StrIndex.class));
		_functions.put(new CaseIgnoredString("StrLastIndex"), new DefaultParser(Str.StrLastIndex.class));
		_functions.put(new CaseIgnoredString("StrSub"), new DefaultParser(Str.StrSub.class));
		
		_functions.put(new CaseIgnoredString("Concat"), new DefaultParser(Str.Concat.class));
		_functions.put(new CaseIgnoredString("ConcatTriple"), new DefaultParser(Str.ConcatTriple.class));
		
		_functions.put(new CaseIgnoredString("E"), new DefaultParser(Math.E.class));
		_functions.put(new CaseIgnoredString("PI"), new DefaultParser(Math.PI.class));
		_functions.put(new CaseIgnoredString("Abs"), new DefaultParser(Math.Abs.class));
		_functions.put(new CaseIgnoredString("Ceil"), new DefaultParser(Math.Ceil.class));
        _functions.put(new CaseIgnoredString("Floor"), new DefaultParser(Math.Floor.class));
        _functions.put(new CaseIgnoredString("Round"), new DefaultParser(Math.Round.class));
        _functions.put(new CaseIgnoredString("Rand"), new DefaultParser(Math.Rand.class));

        _functions.put(new CaseIgnoredString("CurPeriodBeginDate"), new DefaultParser(DateTime.CurPeriodBeginDate.class));
        _functions.put(new CaseIgnoredString("CurPeriodEndDate"), new DefaultParser(DateTime.CurPeriodEndDate.class));
        _functions.put(new CaseIgnoredString("CurPeriodBeginTime"), new DefaultParser(DateTime.CurPeriodBeginTime.class));
        _functions.put(new CaseIgnoredString("CurPeriodEndTime"), new DefaultParser(DateTime.CurPeriodEndTime.class));
        _functions.put(new CaseIgnoredString("CurPeriodBeginLong"), new DefaultParser(DateTime.CurPeriodBeginLong.class));
        _functions.put(new CaseIgnoredString("CurPeriodEndLong"), new DefaultParser(DateTime.CurPeriodEndLong.class));
        _functions.put(new CaseIgnoredString("CurDate"), new DefaultParser(DateTime.CurDate.class));
        _functions.put(new CaseIgnoredString("CurTime"), new DefaultParser(DateTime.CurTime.class));
		_functions.put(new CaseIgnoredString("Date_Format"), new DefaultParser(DateTime.Date_Format.class));
		_functions.put(new CaseIgnoredString("Hour"), new DefaultParser(DateTime.Hour.class));
        _functions.put(new CaseIgnoredString("Minute"), new DefaultParser(DateTime.Minute.class));
        _functions.put(new CaseIgnoredString("Second"), new DefaultParser(DateTime.Second.class));
		_functions.put(new CaseIgnoredString("Time_Format"), new DefaultParser(DateTime.Time_Format.class));
		_functions.put(new CaseIgnoredString("AddSeconds"), new DefaultParser(DateTime.AddSeconds.class));

        _functions.put(new CaseIgnoredString("Size"), new DefaultParser(Size.class));
        _functions.put(new CaseIgnoredString("Explode"), new DefaultParser(Explode.class));
		_functions.put(new CaseIgnoredString("MJoin"), new DefaultParser(Join.MemoryJoin.class));
        _functions.put(new CaseIgnoredString("MTJoin"), new DefaultParser(Join.MemoryTransJoin.class));

		//FOR PPTV
		_functions.put(new CaseIgnoredString("dt"), new DefaultParser(DateTime.CurPeriodEndLong.class));

        // For Cloud IP address library
        _functions.put(new CaseIgnoredString("IPInfo"), new DefaultParser(CloudIpUtil.IPInfoMap.class));
        _functions.put(new CaseIgnoredString("IPCodeInfo"), new DefaultParser(CloudIpUtil.IPCodeInfoMap.class));
        _functions.put(new CaseIgnoredString("IPRangeInfo"), new DefaultParser(CloudIpUtil.IPRange.class));
	
        _functions.put(new CaseIgnoredString("VersionGreaterThan"), new DefaultParser(VersionCompare.class));
	}

	public static boolean isBuiltinFunction(String funcName) {
		return isBuiltinFunction(new CaseIgnoredString(funcName));
	}

	public static boolean isBuiltinFunction(CaseIgnoredString funcName) {
		return _functions.containsKey(funcName);
	}
	
	public static IBuiltinFunctionParser getFunctionParser(String funcName) {
		return getFunctionParser(new CaseIgnoredString(funcName));
	}
	
	public static IBuiltinFunctionParser getFunctionParser(CaseIgnoredString funcName) {
		return _functions.get(funcName);
	}

    static class DefaultParser implements IBuiltinFunctionParser {

        private final Class<? extends AbstractUDF> _t;
        public DefaultParser(Class<? extends AbstractUDF> t) {
            this._t = t;
        }

        public AbstractExpression parse(String funcName, List<AbstractExpression> params) {
            return new FunctionExpression(funcName, params, this._t);
        }
    }

	private BuiltinFunctions(){}
}
