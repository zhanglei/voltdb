/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.rules.physical;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.voltdb.calciteadapter.planner.CalcitePlanner;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPAggregate;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPHashAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingletonExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPUnionExchange;

import com.google.common.collect.ImmutableList;

/**
 * Push Aggregate down trough an Exchange.
 * Transform Aggregate / SingletonExchange rel to SingletonExchange / Aggregate
 * Transform Aggregate / Merge(Union)Exchange rel to
 *                 SingletonExchange / Coordinator Aggregate / Merge(Union)Exchange / Fragment Aggregate
 */
public class VoltDBPAggregateExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPAggregateExchangeTransposeRule INSTANCE_1 =
            new VoltDBPAggregateExchangeTransposeRule(
                    operand(AbstractVoltDBPAggregate.class,
                            RelDistributions.ANY,
                            new RelOptRuleOperandChildren(
                                    RelOptRuleOperandChildPolicy.ANY,
                                    ImmutableList.of(
                                            operand(VoltDBPSingletonExchange.class, any())))),
                    true, "VoltDBPAggregateSingletonExchangeTransposeRule");

    public static final VoltDBPAggregateExchangeTransposeRule INSTANCE_2 =
            new VoltDBPAggregateExchangeTransposeRule(
                    operand(AbstractVoltDBPAggregate.class,
                            RelDistributions.ANY,
                            new RelOptRuleOperandChildren(
                                    RelOptRuleOperandChildPolicy.ANY,
                                    ImmutableList.of(
                                            operand(VoltDBPUnionExchange.class, any())))),
                    false, "VoltDBPAggregateUnionExchangeTransposeRule");

    public static final VoltDBPAggregateExchangeTransposeRule INSTANCE_3 =
            new VoltDBPAggregateExchangeTransposeRule(
                    operand(AbstractVoltDBPAggregate.class,
                            RelDistributions.ANY,
                            new RelOptRuleOperandChildren(
                                    RelOptRuleOperandChildPolicy.ANY,
                                    ImmutableList.of(
                                            operand(VoltDBPMergeExchange.class, any())))),
                    false, "VoltDBPAggregateMergeExchangeTransposeRule");

    // Aggregate functions that require transformation for a distributed query
    private static final Set<SqlKind> COORDINATOR_AGGR_FUNCTIONS =
            EnumSet.of(SqlKind.AVG, SqlKind.COUNT);

    private final boolean m_isSingleton;

    private VoltDBPAggregateExchangeTransposeRule(RelOptRuleOperand operand, boolean isSingleton, String description) {
        super(operand, description);
        m_isSingleton = isSingleton;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AbstractVoltDBPAggregate aggr = call.rel(0);
        AbstractVoltDBPExchange exchange = call.rel(1);

        RelNode result;
        if (m_isSingleton) {
            assert (exchange instanceof VoltDBPSingletonExchange);
            result = transformSingletonExchange(aggr, (VoltDBPSingletonExchange) exchange);
        } else {
            result = transformDistributedExchange(call, aggr, exchange);
            // Is there a better place for that?
            // Add Limit / SerialAggregate / MergeExchange Transpose rule
            // to push the Limit to the fragment
            RelDistribution distributionTrait = exchange.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            CalcitePlanner.addRule(call.getPlanner(), VoltDBPLimitExchangeTransposeRule.makeRule(distributionTrait));

        }

        call.transformTo(result);
    }

    private RelNode transformSingletonExchange(AbstractVoltDBPAggregate aggr, VoltDBPSingletonExchange exchange) {
        // Preserve Aggregate's collation trait if any
        // If aggregation is serial, the exchange better has the collation matching the group by
        RelTraitSet exchangeTraits = exchange.getTraitSet();
        RelTraitSet aggrTraits = aggr.getTraitSet();
        if (aggr instanceof VoltDBPSerialAggregate) {
            assert(aggrTraits.getTrait(RelCollationTraitDef.INSTANCE).equals(
                    exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE)));
        } else {
            // Hash aggregate's outptu is unordered
            exchangeTraits = exchangeTraits.replace(RelCollations.EMPTY);
        }
        if (!exchange.isTopExchange()) {
            // This is not a distributed query so we can can set a new aggregate's distribution
            // right here. For a distributed query it will be done when aggregate will be transposed
            // with a union / merge exchange
            aggrTraits = aggrTraits.replace(exchange.getDistribution());
        }
        // Not used
        boolean isCoordinatorAggregate = exchange.isTopExchange();
        // Simply push the aggregate below the exchange
        AbstractVoltDBPAggregate newAggr = aggr.copy(
                aggr.getCluster(),
                aggrTraits,
                exchange.getInput(),
                aggr.indicator,
                aggr.getGroupSet(),
                aggr.getGroupSets(),
                aggr.getAggCallList(),
                aggr.getPostPredicate(),
                aggr.getSplitCount(),
                isCoordinatorAggregate);

        AbstractVoltDBPExchange newExchange = exchange.copy(
                exchangeTraits,
                newAggr,
                exchange.getDistribution(),
                exchange.isTopExchange());
        return newExchange;
    }

    private RelNode transformDistributedExchange(
            RelOptRuleCall call,
            AbstractVoltDBPAggregate aggr,
            AbstractVoltDBPExchange exchange) {

        // Preserve Aggregate's collation trait if any
        // If agg is serial, the exchange better has a collation matching the group by
        RelTraitSet exchangeTraits = exchange.getTraitSet();
        RelTraitSet aggrTraits = aggr.getTraitSet();
        if (aggr instanceof VoltDBPSerialAggregate) {
            assert(aggrTraits.getTrait(RelCollationTraitDef.INSTANCE).equals(
                    exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE)));
        } else {
            // Hash aggregate's outptu is unordered
            exchangeTraits = exchangeTraits.replace(RelCollations.EMPTY);
        }

        // Fragment aggregate
        RelNode fragmentAggr = aggr.copy(
                        aggr.getCluster(),
                        aggrTraits.replace(exchange.getDistribution()),
                        exchange.getInput(),
                        aggr.indicator,
                        aggr.getGroupSet(),
                        aggr.getGroupSets(),
                        aggr.getAggCallList(),
                        aggr.getPostPredicate(),
                        exchange.getSplitCount(),
                        false);

        AbstractVoltDBPExchange distributedExchange;
        if (fragmentAggr instanceof VoltDBPSerialAggregate) {
            distributedExchange = exchange.copy(
                    exchangeTraits,
                    fragmentAggr,
                    exchange.getDistribution(),
                    exchange.isTopExchange());
        } else {
            distributedExchange = new VoltDBPUnionExchange(
                exchange.getCluster(),
                exchangeTraits.replace(RelCollations.EMPTY),
                fragmentAggr,
                exchange.getSplitCount(),
                exchange.isTopExchange());
        }

        if (!needCoordinatorAggregate(aggr)) {
            return distributedExchange;
        }

        // Coordinator aggregate
        RelNode topAggr;
        // Do we need to transform a coordinator's aggregate?
        if (!needTransformation(COORDINATOR_AGGR_FUNCTIONS, fragmentAggr)) {
            if (distributedExchange instanceof VoltDBPUnionExchange) {
                topAggr = new VoltDBPHashAggregate(
                        aggr.getCluster(),
                        aggrTraits.replace(exchange.getDistribution()).replace(RelCollations.EMPTY),
                        distributedExchange,
                        aggr.indicator,
                        aggr.getGroupSet(),
                        aggr.getGroupSets(),
                        aggr.getAggCallList(),
                        aggr.getPostPredicate(),
                        distributedExchange.getSplitCount(),
                        true);
            } else {
                topAggr = aggr.copy(
                        aggr.getCluster(),
                        aggrTraits.replace(exchange.getDistribution()),
                        distributedExchange,
                        aggr.indicator,
                        aggr.getGroupSet(),
                        aggr.getGroupSets(),
                        aggr.getAggCallList(),
                        aggr.getPostPredicate(),
                        distributedExchange.getSplitCount(),
                        true);
            }
        } else {
            // Don't forget to replace aggrTraits
            topAggr = transformAggregates(COORDINATOR_AGGR_FUNCTIONS, aggr, aggrTraits.replace(exchange.getDistribution()), distributedExchange);
        }

        return topAggr;
    }

    private boolean needCoordinatorAggregate(AbstractVoltDBPAggregate aggregate) {
        return true;
    }

    private boolean needTransformation(Set<SqlKind> aggrFunctionSet, RelNode relNode) {
        Aggregate aggregate = null;
        do {
            if (relNode instanceof Aggregate) {
                aggregate = (Aggregate) relNode;
                break;
            }
            relNode = relNode.getInput(0);
        }
        while (relNode != null);
        assert (aggregate != null);

        List<AggregateCall> aggrCalls = aggregate.getAggCallList();
        for(AggregateCall aggrCall : aggrCalls) {
            if (aggrFunctionSet.contains(aggrCall.getAggregation().getKind())) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param ruleCall
     * @param oldAggRel
     * @param newInput
     * @return
     */
    private RelNode transformAggregates(
            Set<SqlKind> aggrFunctionSet,
            AbstractVoltDBPAggregate oldAggRel,
            RelTraitSet newTraits,
            RelNode newInput) {
        List<AggregateCall> oldAggrCalls = oldAggRel.getAggCallList();
        List<AggregateCall> newAggrCalls = new ArrayList<>();

        // The input to the new aggregate call that replaces the old COUNT is the fragment's COUNT itself.
        // The argument's input / output record layout is GROUP BY columns first followed by fields for each
        // argument Call in order
        int argCallIdx = oldAggRel.getGroupCount();
        for(AggregateCall oldAggrCall : oldAggrCalls) {
            AggregateCall newAggrCall;
            if (aggrFunctionSet.contains(oldAggrCall.getAggregation().getKind())) {
                final SqlKind kind = oldAggrCall.getAggregation().getKind();
                switch (kind) {
                case COUNT:
                    List<Integer> newArgInputList = ImmutableList.of(argCallIdx);
                    newAggrCall = AggregateCall.create(
                            SqlStdOperatorTable.SUM0,
                            oldAggrCall.isDistinct(),
                            oldAggrCall.isApproximate(),
                            newArgInputList,
                            oldAggrCall.filterArg,
                            oldAggRel.getGroupCount(),
                            newInput,
                            oldAggrCall.getType(),
                            oldAggrCall.getName());
                    break;

                default:
                    // anything else: preserve original call
                    newAggrCall = oldAggrCall.copy(oldAggrCall.getArgList(), oldAggrCall.filterArg);
                }

            } else {
                newAggrCall = oldAggrCall.copy(oldAggrCall.getArgList(), oldAggrCall.filterArg);
            }
            newAggrCalls.add(newAggrCall);
            argCallIdx++;
        }
        AbstractVoltDBPAggregate newAggRel = oldAggRel.copy(
                oldAggRel.getCluster(),
                newTraits,
                newInput,
                oldAggRel.indicator,
                oldAggRel.getGroupSet(),
                oldAggRel.getGroupSets(),
                newAggrCalls,
                oldAggRel.getPostPredicate(),
                oldAggRel.getSplitCount(),
                oldAggRel.isCoordinatorAggr());

        return newAggRel;
    }

}
