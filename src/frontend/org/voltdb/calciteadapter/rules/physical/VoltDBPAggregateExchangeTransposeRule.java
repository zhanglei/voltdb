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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.voltdb.calciteadapter.planner.CalcitePlanner;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPAggregate;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPHashAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPProject;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingletonExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPUnionExchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
    private static final Set<SqlKind> FRAGEMENT_AGGR_FUNCTIONS =
            EnumSet.of(SqlKind.AVG);
    private static final Set<SqlKind> COORDINATOR_AGGR_FUNCTIONS =
            EnumSet.of(SqlKind.AVG, SqlKind.COUNT);

    /**
     * A visitor to convert transformed LogicalAggregate and LogicalProject RelNodes
     * to the VoltDB convention
     *
     */
    private class RewriteRelVisitor extends RelVisitor {
        // Original Aggregate relation
        private final AbstractVoltDBPAggregate m_origAggrRelNode;

        private final boolean m_isCoordinatorAggr;
        private final boolean m_mustHaveHashAggregate;

        // The root of the converted relation
        private RelNode m_voltRootNode = null;

        RewriteRelVisitor(AbstractVoltDBPAggregate origAggrRelNode, boolean isCoordinatorAggr, boolean mustHaveHashAggregate) {
            m_origAggrRelNode = origAggrRelNode;
            m_isCoordinatorAggr = isCoordinatorAggr;
            m_mustHaveHashAggregate = mustHaveHashAggregate;
        }

        public RelNode getRootNode() {
            return m_voltRootNode;
        }

        @Override
        public void visit(RelNode relNode, int ordinal, RelNode parentNode) {
            // End of recursion
            if (VoltDBPRel.VOLTDB_PHYSICAL.equals(relNode.getConvention())) {
                return;
            }            // rewrite children first
            super.visit(relNode, ordinal, parentNode);

            final RelTraitSet traitSet = m_origAggrRelNode.getTraitSet().replace(RelDistributions.SINGLETON);
            RelNode voltRelNode = null;
            if (relNode instanceof LogicalAggregate) {
                LogicalAggregate aggregate = (LogicalAggregate) relNode;
                if (m_origAggrRelNode instanceof VoltDBPHashAggregate || m_mustHaveHashAggregate) {
                    voltRelNode = new VoltDBPHashAggregate(
                            aggregate.getCluster(),
                            traitSet,
                            aggregate.getInput(),
                            aggregate.indicator,
                            aggregate.getGroupSet(),
                            aggregate.getGroupSets(),
                            aggregate.getAggCallList(),
                            m_origAggrRelNode.getPostPredicate(),
                            1,
                            m_isCoordinatorAggr);
                } else if (m_origAggrRelNode instanceof VoltDBPSerialAggregate) {
                    voltRelNode = new VoltDBPSerialAggregate(
                            aggregate.getCluster(),
                            traitSet,
                            aggregate.getInput(),
                            aggregate.indicator,
                            aggregate.getGroupSet(),
                            aggregate.getGroupSets(),
                            aggregate.getAggCallList(),
                            m_origAggrRelNode.getPostPredicate(),
                            1,
                            m_isCoordinatorAggr);
                } else {
                    assert(false);
                }
            } else if (relNode instanceof LogicalProject) {
                LogicalProject project = (LogicalProject) relNode;
                voltRelNode = VoltDBPProject.create(
                        traitSet,
                        project.getInput(),
                        project.getProjects(),
                        project.getRowType());
            } else {
                // Should not be any other types. Just in case
                voltRelNode = relNode.copy(traitSet, relNode.getInputs());
            }
            if (parentNode != null) {
                // Replace the old Logical expression with the new VoltDB one
                parentNode.replaceInput(ordinal, voltRelNode);
            } else {
                // It's the root
                m_voltRootNode = voltRelNode;
            }
        }
    }

    private final boolean m_isSingleton;

    private VoltDBPAggregateExchangeTransposeRule(RelOptRuleOperand operand, boolean isSingleton, String description) {
        super(operand, description);
        m_isSingleton = isSingleton;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
//        AbstractVoltDBPAggregate aggr = call.rel(0);
//        AbstractVoltDBPExchange exchange = call.rel(1);
//
//        if (aggr.isCoordinatorAggr()) {
//            return false;
//        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AbstractVoltDBPAggregate aggr = call.rel(0);
        AbstractVoltDBPExchange exchange = call.rel(1);

//        if (aggr.isCoordinatorAggr()) {
//            return;
//        }

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
            // right here. For a distributed query it will be done when aggregate will be trasposed
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
        RelNode fragmentAggr;
        // Do we need to transform a fragment aggregate?
//        if (!needTransformation(FRAGEMENT_AGGR_FUNCTIONS, aggr)) {
            // Simply copy the original aggregate and add correct distribution trait to it.
//            RelCollation aggrCollation = aggrTraits.getTrait(RelCollationTraitDef.INSTANCE);
//            if (aggrCollation.equals(RelCollations.EMPTY)) {
//                fragmentAggr = new VoltDBPHashAggregate(
//                        aggr.getCluster(),
//                        aggrTraits.replace(exchange.getDistribution()),
//                        exchange.getInput(),
//                        aggr.indicator,
//                        aggr.getGroupSet(),
//                        aggr.getGroupSets(),
//                        aggr.getAggCallList(),
//                        aggr.getPostPredicate(),
//                        exchange.getSplitCount(),
//                        false);
//            } else {
                fragmentAggr = aggr.copy(
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

//            }
//        } else {
//            // Transform new fragment aggregate
//            // Don't forget to update aggregate traits - aggrTraits
//            fragmentAggr = transformAggregates(call, aggr, exchange.getInput());
//            // Convert new LogicalAggregates to VoltDBPAggregates
//            RewriteRelVisitor visitor = new RewriteRelVisitor(aggr, false);
//            visitor.visit(fragmentAggr, 0, null);
//            fragmentAggr = visitor.getRootNode();
//
//        }

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
//            // Convert new LogicalAggregates to VoltDBPAggregates
//            RewriteRelVisitor visitor = new RewriteRelVisitor(aggr, true, distributedExchange instanceof VoltDBPUnionExchange);
//            visitor.visit(topAggr, 0, null);
//            topAggr = visitor.getRootNode();
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
     * Borrowed from Calcite's AggregateReduceFunctionsRule.
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

        for(AggregateCall oldAggrCall : oldAggrCalls) {
            AggregateCall newAggrCall;
            if (aggrFunctionSet.contains(oldAggrCall.getAggregation().getKind())) {
                final SqlKind kind = oldAggrCall.getAggregation().getKind();
                switch (kind) {
                case COUNT:
                    // The input to the new SUM aggregate is the old COUNT aggregate
                    int newSumInput = -1;
                    for (RelDataTypeField inputField : oldAggRel.getRowType().getFieldList()) {
                        assert(inputField.getName() != null);
                        if (inputField.getName().equals(oldAggrCall.name)) {
                            newSumInput = inputField.getIndex();
                            break;
                        }
                    }
                    assert(newSumInput != -1);
                    List<Integer> newArgInputList = ImmutableList.of(newSumInput);
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

//    private RelNode transformAggregates(RelOptRuleCall ruleCall, Aggregate oldAggRel, RelNode newInput) {
//        RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
//
//        List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
//        final int groupCount = oldAggRel.getGroupCount();
//        final int indicatorCount = oldAggRel.getIndicatorCount();
//
//        final List<AggregateCall> newCalls = Lists.newArrayList();
//        final Map<AggregateCall, RexNode> aggCallMapping = Maps.newHashMap();
//
//        final List<RexNode> projList = Lists.newArrayList();
//
//        // pass through group key (+ indicators if present)
//        for (int i = 0; i < groupCount + indicatorCount; ++i) {
//          projList.add(
//              rexBuilder.makeInputRef(
//                  getFieldType(oldAggRel, i),
//                  i));
//        }
//
//        // List of input expressions. If a particular aggregate needs more, it
//        // will add an expression to the end, and we will create an extra
//        // project.
//        final RelBuilder relBuilder = ruleCall.builder();
//        relBuilder.push(newInput);
//        final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());
//
//        // create new agg function calls and rest of project list together
//        int oldCallIdx = 0;
//        for (AggregateCall oldCall : oldCalls) {
//          projList.add(
//                  transformAggregate(
//                          oldAggRel, oldCall, oldCallIdx++, newCalls, aggCallMapping, inputExprs));
//        }
//
//        final int extraArgCount =
//            inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
//        if (extraArgCount > 0) {
//          relBuilder.project(inputExprs,
//              CompositeList.of(
//                  relBuilder.peek().getRowType().getFieldNames(),
//                  Collections.<String>nCopies(extraArgCount, null)));
//        }
//        newAggregateRel(relBuilder, oldAggRel, newCalls);
//        relBuilder.project(projList, oldAggRel.getRowType().getFieldNames());
//        return relBuilder.build();
//    }

    private RexNode transformAggregate(Aggregate oldAggRel,
            AggregateCall oldCall,
            int oldCallIdx,
            List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping,
            List<RexNode> inputExprs) {
        final SqlKind kind = oldCall.getAggregation().getKind();
        switch (kind) {
        case COUNT:
            // replace original COUNT(x) with SUM(x)
            return transformCount(oldAggRel, oldCall,oldCallIdx,  newCalls, aggCallMapping,
                    inputExprs);
        case AVG:
            // replace original AVG(x) with SUM(x) / COUNT(x)
            return transformAvg(oldAggRel, oldCall, newCalls, aggCallMapping,
                    inputExprs);
        default:
            // anything else: preserve original call
            RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
            final int nGroups = oldAggRel.getGroupCount();
            List<RelDataType> oldArgTypes = SqlTypeUtil.projectTypes(
                    oldAggRel.getInput().getRowType(), oldCall.getArgList());
            return rexBuilder.addAggCall(oldCall, nGroups, oldAggRel.indicator,
                    newCalls, aggCallMapping, oldArgTypes);
        }
    }

    /**
     * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
     * into Aggregate and subclasses - but it's only needed for some
     * subclasses.
     *
     * @param relBuilder Builder of relational expressions; at the top of its
     *                   stack is its input
     * @param oldAggregate LogicalAggregate to clone.
     * @param newCalls  New list of AggregateCalls
     */
    private void newAggregateRel(RelBuilder relBuilder,
        Aggregate oldAggregate,
        List<AggregateCall> newCalls) {
      relBuilder.aggregate(
          relBuilder.groupKey(oldAggregate.getGroupSet(),
              oldAggregate.getGroupSets()),
          newCalls);
    }

    private RelDataType getFieldType(RelNode relNode, int i) {
        final RelDataTypeField inputField =
            relNode.getRowType().getFieldList().get(i);
        return inputField.getType();
      }

    private RexNode transformCount(Aggregate oldAggRel,
            AggregateCall oldCall,
            int oldCallIdx,
            List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping,
            List<RexNode> inputExprs) {
        final int nGroups = oldAggRel.getGroupCount();
        final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        RelDataType avgInputType = null;
        List<Integer> newArgList = null;
        if (oldCall.getArgList().isEmpty())  {
            // count(*)
            avgInputType = oldCall.getType();
            newArgList = new ArrayList<>();
            int sumArgInputIdx = oldAggRel.getGroupCount() + oldCallIdx;
            newArgList.add(sumArgInputIdx);
        } else {
            final int iAvgInput = oldCall.getArgList().get(0);
            avgInputType = getFieldType(oldAggRel.getInput(),
                iAvgInput);
            newArgList = oldCall.getArgList();
        }

        // The difference between SUM0 and SUM aggregate functions is that the former's
        // return type is NOT NULL matching the COUNT's return type
        final AggregateCall sumCall = AggregateCall.create(
                SqlStdOperatorTable.SUM0, oldCall.isDistinct(),
                newArgList, oldCall.filterArg,
                oldAggRel.getGroupCount(), oldAggRel.getInput(), oldCall.getType(), null);

        RexNode sumRef = rexBuilder.addAggCall(sumCall, nGroups,
                oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(avgInputType));
        RexNode retRef = rexBuilder.makeCast(oldCall.getType(), sumRef);
        return retRef;
    }

    private RexNode transformAvg(Aggregate oldAggRel,
            AggregateCall oldCall,
            List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping,
            List<RexNode> inputExprs) {
        final int nGroups = oldAggRel.getGroupCount();
        final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        final int iAvgInput = oldCall.getArgList().get(0);
        final RelDataType avgInputType = getFieldType(oldAggRel.getInput(),
                iAvgInput);
        final AggregateCall sumCall = AggregateCall.create(
                SqlStdOperatorTable.SUM, oldCall.isDistinct(),
                oldCall.getArgList(), oldCall.filterArg,
                oldAggRel.getGroupCount(), oldAggRel.getInput(), null, null);
        final AggregateCall countCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT, oldCall.isDistinct(),
                oldCall.getArgList(), oldCall.filterArg,
                oldAggRel.getGroupCount(), oldAggRel.getInput(), null, null);

        // NOTE: these references are with respect to the output
        // of newAggRel
        RexNode numeratorRef = rexBuilder.addAggCall(sumCall, nGroups,
                oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(avgInputType));
        final RexNode denominatorRef = rexBuilder.addAggCall(countCall, nGroups,
                oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(avgInputType));

        final RelDataTypeFactory typeFactory = oldAggRel.getCluster()
                .getTypeFactory();
        final RelDataType avgType = typeFactory.createTypeWithNullability(
                oldCall.getType(), numeratorRef.getType().isNullable());
        numeratorRef = rexBuilder.ensureType(avgType, numeratorRef, true);
        final RexNode divideRef = rexBuilder.makeCall(
                SqlStdOperatorTable.DIVIDE, numeratorRef, denominatorRef);
        return rexBuilder.makeCast(oldCall.getType(), divideRef);
    }

}
