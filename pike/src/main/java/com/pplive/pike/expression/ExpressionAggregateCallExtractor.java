package com.pplive.pike.expression;

import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.parser.TransformField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ExpressionAggregateCallExtractor implements IExpressionVisitor {

    private static boolean not(boolean expr) { return !expr; }

    public static List<TransformField> extract(AbstractExpression expr) {
        assert not( isAggregateCall(expr) );
        return new ExpressionAggregateCallExtractor().visit(null, expr);
    }

    private static boolean isAggregateCall(AbstractExpression expr) {
        return expr instanceof  AggregateExpression;
    }

	private ExpressionAggregateCallExtractor(){}

    private static TransformField create(AggregateExpression expr) {
        return new TransformField(expr);
    }
	
	private List<TransformField> visit(Object context, AbstractExpression expr) {
		return (ArrayList<TransformField>)expr.visit(null, this);
	}

	public List<TransformField> visit(Object context, AggregateExpression expr) {
        assert false;
		throw new RuntimeException("bug: should never come here");
	}

	public List<TransformField> visit(Object context, BinaryOpExpression expr) {
        ArrayList<TransformField> fields = new ArrayList<TransformField>();

        if (not(isAggregateCall(expr.left()))) {
            fields.addAll(visit(context, expr.left()));
        }
        else {
            TransformField f = create((AggregateExpression)expr.left());
            ColumnType colType = ColumnType.convert(expr.left().exprType());
            expr.setLeft(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        if (not(isAggregateCall(expr.right()))) {
            fields.addAll(visit(context, expr.right()));
        }
		else {
            TransformField f = create((AggregateExpression)expr.right());
            ColumnType colType = ColumnType.convert(expr.right().exprType());
            expr.setRight(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        return fields;
	}

	public List<TransformField> visit(Object context, UnaryOpExpression expr) {
        ArrayList<TransformField> fields = new ArrayList<TransformField>();

        if (not(isAggregateCall(expr.getParam()))) {
            return visit(context, expr.getParam());
        }
        else {
            TransformField f = create((AggregateExpression)expr.getParam());
            ColumnType colType = ColumnType.convert(expr.getParam().exprType());
            expr.setParam(new ColumnExpression(colType, f.getAlias().value()));

            return Arrays.asList(f);
        }
	}

	private List<TransformField> visit(Object context, BinaryLogicalExpression expr) {
        ArrayList<TransformField> fields = new ArrayList<TransformField>();

        if (not(isAggregateCall(expr.left()))) {
            fields.addAll(visit(context, expr.left()));
        }
        else {
            TransformField f = create((AggregateExpression)expr.left());
            ColumnType colType = ColumnType.convert(expr.left().exprType());
            expr.setLeft(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        if (not(isAggregateCall(expr.right()))) {
            fields.addAll(visit(context, expr.right()));
        }
        else {
            TransformField f = create((AggregateExpression)expr.right());
            ColumnType colType = ColumnType.convert(expr.right().exprType());
            expr.setRight(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        return fields;
	}

    public List<TransformField> visit(Object context, AndExpression expr) {
        return visit(context, (BinaryLogicalExpression)expr);
	}

    public List<TransformField> visit(Object context, OrExpression expr) {
        return visit(context, (BinaryLogicalExpression)expr);
    }

    public List<TransformField> visit(Object context, InExpression expr) {
        return new ArrayList<TransformField>();
    }

	public List<TransformField> visit(Object context, ColumnExpression expr) {
        return new ArrayList<TransformField>();
	}

	public List<TransformField> visit(Object context, ConstantExpression expr) {
        return new ArrayList<TransformField>();
	}

	public List<TransformField> visit(Object context, FunctionExpression expr) {
        List<AbstractExpression> params = expr.getParamsRef();
        ArrayList<TransformField> fields = new ArrayList<TransformField>();
        for(int n = 0; n < params.size(); n += 1){
            AbstractExpression paramExpr = params.get(n);
            if (not(isAggregateCall(paramExpr))) {
                fields.addAll(visit(context, paramExpr));
            }
            else {
                TransformField f = create((AggregateExpression)paramExpr);
                ColumnType colType = ColumnType.convert(paramExpr.exprType());
                params.set(n, new ColumnExpression(colType, f.getAlias().value()));

                fields.add(f);
            }
        }
        return fields;
	}

	public List<TransformField> visit(Object context, IfExpression expr) {
        ArrayList<TransformField> fields = new ArrayList<TransformField>();

        if (not(isAggregateCall(expr.condition()))) {
            fields.addAll(visit(context, expr.condition()));
        }
        else {
            TransformField f = create((AggregateExpression)expr.condition());
            ColumnType colType = ColumnType.convert(expr.condition().exprType());
            expr.setCondition(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        if (not(isAggregateCall(expr.trueResult()))) {
            fields.addAll(visit(context, expr.trueResult()));
        }
        else {
            TransformField f = create((AggregateExpression)expr.trueResult());
            ColumnType colType = ColumnType.convert(expr.trueResult().exprType());
            expr.setTrueResult(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        if (not(isAggregateCall(expr.falseResult()))) {
            fields.addAll(visit(context, expr.falseResult()));
        }
        else {
            TransformField f = create((AggregateExpression)expr.falseResult());
            ColumnType colType = ColumnType.convert(expr.falseResult().exprType());
            expr.setFalseResult(new ColumnExpression(colType, f.getAlias().value()));

            fields.add(f);
        }

        return fields;
	}

	public List<TransformField> visit(Object context, OutputExpression expr) {
        assert false;
        throw new RuntimeException("bug: should never come here");
	}
}
