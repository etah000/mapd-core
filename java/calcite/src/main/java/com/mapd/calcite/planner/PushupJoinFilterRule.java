package com.mapd.calcite.planner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

public class PushupJoinFilterRule extends RelOptRule {

    public PushupJoinFilterRule(RelOptRuleOperand operand) {
        super(operand);
    }

//    public PushupJoinFilterRule(RelOptRuleOperand operand, String description) {
//        super(operand, description);
//    }
//
//    public PushupJoinFilterRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
//        super(operand, relBuilderFactory, description);
//    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);

        //currently only handle "INNER" join
        if (join.getJoinType() != JoinRelType.INNER) {
            return;
        }

        if (join.getCondition().isAlwaysTrue()) {
            return;
        }

        if (!join.getSystemFieldList().isEmpty()) {
            return;
        }

        final RelBuilder builder = call.builder();

        RexNode condition = join.getCondition();
        if (condition != null && condition instanceof RexCall) {
            RexCall rexCallCond = (RexCall) condition;
            if (rexCallCond.getOperator().getKind() != SqlKind.AND ||
                    rexCallCond.getOperands().size() == 1)  return;

            RexCall subCond = null;
            RexCall equiCond = null;
            List<RexNode> restOperands = new ArrayList<>();
            for (RexNode node : rexCallCond.getOperands()) {
                if (node instanceof  RexCall) {
                    subCond = (RexCall) node;
                    if (subCond.getOperator().getKind() == SqlKind.EQUALS) {
                        equiCond = subCond;
                    }
                    else {
                        restOperands.add(node);
                    }
                }
            }

            final RelNode equijoin = join.copy(
                    join.getTraitSet(),
                    equiCond,
                    join.getLeft(),
                    join.getRight(),
                    join.getJoinType(),
                    join.isSemiJoinDone());
            builder.push(equijoin);
            RelNode filter = null;
            if (restOperands.size() > 1) {
                filter = RelOptUtil.createFilter(equijoin, restOperands);
            } else {
               filter = RelOptUtil.createFilter(equijoin, rexCallCond.getOperands().get(1));
            }
            builder.push(filter);

            call.transformTo(builder.build());
        }

    }
}
