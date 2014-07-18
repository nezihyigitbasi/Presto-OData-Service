package com.presto.odata.sql.visitor;

import org.odata4j.expression.AddExpression;
import org.odata4j.expression.AggregateAllFunction;
import org.odata4j.expression.AggregateAnyFunction;
import org.odata4j.expression.AndExpression;
import org.odata4j.expression.BinaryLiteral;
import org.odata4j.expression.BoolParenExpression;
import org.odata4j.expression.BooleanLiteral;
import org.odata4j.expression.ByteLiteral;
import org.odata4j.expression.CastExpression;
import org.odata4j.expression.CeilingMethodCallExpression;
import org.odata4j.expression.ConcatMethodCallExpression;
import org.odata4j.expression.DateTimeLiteral;
import org.odata4j.expression.DateTimeOffsetLiteral;
import org.odata4j.expression.DayMethodCallExpression;
import org.odata4j.expression.DecimalLiteral;
import org.odata4j.expression.DivExpression;
import org.odata4j.expression.DoubleLiteral;
import org.odata4j.expression.EndsWithMethodCallExpression;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.EqExpression;
import org.odata4j.expression.ExpressionVisitor;
import org.odata4j.expression.FloorMethodCallExpression;
import org.odata4j.expression.GeExpression;
import org.odata4j.expression.GtExpression;
import org.odata4j.expression.GuidLiteral;
import org.odata4j.expression.HourMethodCallExpression;
import org.odata4j.expression.IndexOfMethodCallExpression;
import org.odata4j.expression.Int64Literal;
import org.odata4j.expression.IntegralLiteral;
import org.odata4j.expression.IsofExpression;
import org.odata4j.expression.LeExpression;
import org.odata4j.expression.LengthMethodCallExpression;
import org.odata4j.expression.LtExpression;
import org.odata4j.expression.MinuteMethodCallExpression;
import org.odata4j.expression.ModExpression;
import org.odata4j.expression.MonthMethodCallExpression;
import org.odata4j.expression.MulExpression;
import org.odata4j.expression.NeExpression;
import org.odata4j.expression.NegateExpression;
import org.odata4j.expression.NotExpression;
import org.odata4j.expression.NullLiteral;
import org.odata4j.expression.OrExpression;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.expression.ParenExpression;
import org.odata4j.expression.ReplaceMethodCallExpression;
import org.odata4j.expression.RoundMethodCallExpression;
import org.odata4j.expression.SByteLiteral;
import org.odata4j.expression.SecondMethodCallExpression;
import org.odata4j.expression.SingleLiteral;
import org.odata4j.expression.StartsWithMethodCallExpression;
import org.odata4j.expression.StringLiteral;
import org.odata4j.expression.SubExpression;
import org.odata4j.expression.SubstringMethodCallExpression;
import org.odata4j.expression.SubstringOfMethodCallExpression;
import org.odata4j.expression.TimeLiteral;
import org.odata4j.expression.ToLowerMethodCallExpression;
import org.odata4j.expression.ToUpperMethodCallExpression;
import org.odata4j.expression.TrimMethodCallExpression;
import org.odata4j.expression.YearMethodCallExpression;

public class OrderByVisitor implements ExpressionVisitor {
    private StringBuffer orderByClause = new StringBuffer();

    @Override
    public void beforeDescend() {

    }

    @Override
    public void afterDescend() {

    }

    @Override
    public void betweenDescend() {

    }

    @Override
    public void visit(String s) {

    }

    @Override
    public void visit(OrderByExpression orderByExpression) {
        EntitySimpleProperty p = ((EntitySimpleProperty) (orderByExpression.getExpression()));
        String orderField = p.getPropertyName();
        orderByClause.append("ORDER BY ").append(orderField).append(" ");
    }

    @Override
    public void visit(OrderByExpression.Direction direction) {
        switch (direction) {
            case ASCENDING:
                orderByClause.append("ASC");
                break;
            case DESCENDING:
                orderByClause.append("DESC");
                break;
            default:
                throw new IllegalArgumentException("Unknown direction " + direction);
        }
    }

    @Override
    public void visit(AddExpression addExpression) {

    }

    @Override
    public void visit(AndExpression andExpression) {

    }

    @Override
    public void visit(BooleanLiteral booleanLiteral) {

    }

    @Override
    public void visit(CastExpression castExpression) {

    }

    @Override
    public void visit(ConcatMethodCallExpression concatMethodCallExpression) {

    }

    @Override
    public void visit(DateTimeLiteral dateTimeLiteral) {

    }

    @Override
    public void visit(DateTimeOffsetLiteral dateTimeOffsetLiteral) {

    }

    @Override
    public void visit(DecimalLiteral decimalLiteral) {

    }

    @Override
    public void visit(DivExpression divExpression) {

    }

    @Override
    public void visit(EndsWithMethodCallExpression endsWithMethodCallExpression) {

    }

    @Override
    public void visit(EntitySimpleProperty entitySimpleProperty) {

    }

    @Override
    public void visit(EqExpression eqExpression) {

    }

    @Override
    public void visit(GeExpression geExpression) {

    }

    @Override
    public void visit(GtExpression gtExpression) {

    }

    @Override
    public void visit(GuidLiteral guidLiteral) {

    }

    @Override
    public void visit(BinaryLiteral binaryLiteral) {

    }

    @Override
    public void visit(ByteLiteral byteLiteral) {

    }

    @Override
    public void visit(SByteLiteral sByteLiteral) {

    }

    @Override
    public void visit(IndexOfMethodCallExpression indexOfMethodCallExpression) {

    }

    @Override
    public void visit(SingleLiteral singleLiteral) {

    }

    @Override
    public void visit(DoubleLiteral doubleLiteral) {

    }

    @Override
    public void visit(IntegralLiteral integralLiteral) {

    }

    @Override
    public void visit(Int64Literal int64Literal) {

    }

    @Override
    public void visit(IsofExpression isofExpression) {

    }

    @Override
    public void visit(LeExpression leExpression) {

    }

    @Override
    public void visit(LengthMethodCallExpression lengthMethodCallExpression) {

    }

    @Override
    public void visit(LtExpression ltExpression) {

    }

    @Override
    public void visit(ModExpression modExpression) {

    }

    @Override
    public void visit(MulExpression mulExpression) {

    }

    @Override
    public void visit(NeExpression neExpression) {

    }

    @Override
    public void visit(NegateExpression negateExpression) {

    }

    @Override
    public void visit(NotExpression notExpression) {

    }

    @Override
    public void visit(NullLiteral nullLiteral) {

    }

    @Override
    public void visit(OrExpression orExpression) {

    }

    @Override
    public void visit(ParenExpression parenExpression) {

    }

    @Override
    public void visit(BoolParenExpression boolParenExpression) {

    }

    @Override
    public void visit(ReplaceMethodCallExpression replaceMethodCallExpression) {

    }

    @Override
    public void visit(StartsWithMethodCallExpression startsWithMethodCallExpression) {

    }

    @Override
    public void visit(StringLiteral stringLiteral) {

    }

    @Override
    public void visit(SubExpression subExpression) {

    }

    @Override
    public void visit(SubstringMethodCallExpression substringMethodCallExpression) {

    }

    @Override
    public void visit(SubstringOfMethodCallExpression substringOfMethodCallExpression) {

    }

    @Override
    public void visit(TimeLiteral timeLiteral) {

    }

    @Override
    public void visit(ToLowerMethodCallExpression toLowerMethodCallExpression) {

    }

    @Override
    public void visit(ToUpperMethodCallExpression toUpperMethodCallExpression) {

    }

    @Override
    public void visit(TrimMethodCallExpression trimMethodCallExpression) {

    }

    @Override
    public void visit(YearMethodCallExpression yearMethodCallExpression) {

    }

    @Override
    public void visit(MonthMethodCallExpression monthMethodCallExpression) {

    }

    @Override
    public void visit(DayMethodCallExpression dayMethodCallExpression) {

    }

    @Override
    public void visit(HourMethodCallExpression hourMethodCallExpression) {

    }

    @Override
    public void visit(MinuteMethodCallExpression minuteMethodCallExpression) {

    }

    @Override
    public void visit(SecondMethodCallExpression secondMethodCallExpression) {

    }

    @Override
    public void visit(RoundMethodCallExpression roundMethodCallExpression) {

    }

    @Override
    public void visit(FloorMethodCallExpression floorMethodCallExpression) {

    }

    @Override
    public void visit(CeilingMethodCallExpression ceilingMethodCallExpression) {

    }

    @Override
    public void visit(AggregateAnyFunction aggregateAnyFunction) {

    }

    @Override
    public void visit(AggregateAllFunction aggregateAllFunction) {

    }

    public void append(StringBuilder sb) {
        sb.append(" ").append(orderByClause);
    }
}
