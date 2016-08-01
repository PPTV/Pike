package com.pplive.pike.expression;

import com.pplive.pike.base.HashFailThrower;
import com.pplive.pike.expression.compare.EqualToOp;
import com.pplive.pike.parser.SemanticErrorException;
import com.pplive.pike.util.CollectionUtil;
import com.pplive.pike.util.ReflectionUtils;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InExpression extends BuiltinOpExpression {

	private static final long serialVersionUID = 1L;

	private AbstractExpression _left;
	public AbstractExpression left() { return this._left; }
    public void setLeft(AbstractExpression expr) { this._left = expr; }

    private List<AbstractExpression> _candidatesExprs;
    public List<AbstractExpression> getCandidateExprs() { return CollectionUtil.copyArrayList(_candidatesExprs); }

    private List<InCandidate> _candidates;
    public List<InCandidate> getCandidates() { return CollectionUtil.copyArrayList(_candidates); }

    class InCandidate implements Serializable
    {
        public boolean isNull = false;
        public AbstractExpression expr;
        public transient Method method;
    }

	public InExpression(AbstractExpression left, Iterable<AbstractExpression> candidates) {
        super(EqualToOp.Op, EqualToOp.class);
		if (left == null)
			throw new IllegalArgumentException("left cannot be null");
		if (candidates == null)
			throw new IllegalArgumentException("candidates cannot be null");
		
		this._left = left;
		this._candidatesExprs = CollectionUtil.copyArrayList(candidates);

        Class<?> leftType = this._left.exprType();
        this._candidates = new ArrayList<InCandidate>();
        for(AbstractExpression expr : candidates) {
            InCandidate candidate = new InCandidate();
            this._candidates.add(candidate);
            candidate.expr = expr;
            if (expr instanceof ConstantExpression) {
                Object obj = expr.eval(null);
                if (obj == null) {
                    candidate.isNull = true;
                }
            }
            if (not(candidate.isNull)) {
                Class<?> rightType = candidate.expr.exprType();
                candidate.method = ReflectionUtils.tryGetMethod("eval", Arrays.asList(new Class<?>[]{leftType, rightType}), this._opType);
                if (candidate.method == null){
                    String msg = String.format("IN (...) type incompatible: <%s> = <%s> is not supported",
                                    left.exprType().getSimpleName(), rightType.getSimpleName());
                    throw new SemanticErrorException(msg);
                }
            }
        }

	}

	@Override
	public void init() {
		this._left.init();
        for(InCandidate candidate : this._candidates)
            candidate.expr.init();
	}
	
	@Override
	public Class<?> exprType() {
		return Boolean.class;
	}

    private void checkInitialized(InCandidate candidate) {
        if (candidate.method != null)
            return;

        Class<?> leftType = this._left.exprType();
        Class<?> rightType = candidate.expr.exprType();
        candidate.method = ReflectionUtils.getMethod("eval", Arrays.asList(new Class<?>[]{leftType, rightType}), this._opType);
        assert candidate.method != null;
    }

    @Override
    public Object eval(TridentTuple tuple) {
        Object left = this._left.eval(tuple);

        for(InCandidate candidate : this._candidates) {
            if (candidate.isNull) {
                if (left == null)
                    return Boolean.TRUE;
                else
                    continue;
            }

            checkInitialized(candidate);

            Object right = candidate.expr.eval(tuple);
            try {
                Object equal = candidate.method.invoke(null, left, right);
                if (equal == null) {
                    continue;
                }
                if ((Boolean)equal) {
                    return equal;
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(String.format("calling builtin op class %s failed.", this._opType), e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(String.format("calling builtin op class %s failed.", this._opType), e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(String.format("calling builtin op class %s failed.", this._opType), e);
            }
        }

        return Boolean.FALSE;
    }

    @Override
    public Object visit(Object context, IExpressionVisitor visitor) {
        return visitor.visit(context, this);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s IN (", this._left));
        int n = 0;
        for(AbstractExpression expr : this._candidatesExprs) {
            n += 1;
            if (n > 1) sb.append(", ");
            sb.append(expr);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o == null || o.getClass() != InExpression.class)
            return false;
        InExpression other = (InExpression)o;

        if( not(this._left.equals(other._left)) )
            return false;
        if (this._candidates.size() != other._candidates.size())
            return false;
        for(int n = 0; n < this._candidates.size(); n += 1) {
            AbstractExpression l = this._candidatesExprs.get(n);
            AbstractExpression r = other._candidatesExprs.get(n);
            if (not(l.equals(r)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode(){
        return HashFailThrower.throwOnHash(this);
    }

    private static boolean not(boolean expr) { return !expr; }
}
