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

package org.voltdb.calciteadapter.rel.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.SendPlanNode;

import com.google.common.collect.ImmutableList;

public class VoltDBPMergeExchange extends AbstractVoltDBPExchange implements VoltDBPRel {

    // Inline Offset
    final private RexNode m_offset;
    // Inline Limit
    final private RexNode m_limit;

    // Inline Serial Aggregate
    final private VoltDBPSerialAggregate m_aggregate;

    // Collation fields expressions
    final ImmutableList<RexNode> m_collationFieldExprs;

    public VoltDBPMergeExchange(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            int splitCount,
            boolean isTopExchange,
            List<RexNode> collationFieldExprs,
            RexNode offset,
            RexNode limit,
            VoltDBPSerialAggregate aggregate) {
        super(cluster, traitSet, input, splitCount, isTopExchange);
        m_collationFieldExprs = ImmutableList.copyOf(collationFieldExprs);
        m_offset = offset;
        m_limit = limit;
        m_aggregate = aggregate;
    }

    public VoltDBPMergeExchange(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            int splitCount,
            boolean isTopExchange,
            List<RexNode> collationFieldExprs) {
        this(cluster,
             traitSet,
             input,
             splitCount,
             isTopExchange,
             collationFieldExprs,
             null,
             null,
             null);
    }

    @Override
    protected VoltDBPMergeExchange copyInternal(
            RelTraitSet traitSet,
            RelNode newInput,
            boolean isTopExchange) {
        VoltDBPMergeExchange exchange = new VoltDBPMergeExchange(
                getCluster(),
                traitSet,
                newInput,
                m_splitCount,
                isTopExchange,
                m_collationFieldExprs,
                m_offset,
                m_limit,
                m_aggregate);
        return exchange;
    }

    public VoltDBPMergeExchange copyWithLimit(RexNode offset, RexNode limit) {
        return new VoltDBPMergeExchange(
                getCluster(),
                traitSet,
                getInput(),
                m_splitCount,
                isTopExchange(),
                m_collationFieldExprs,
                offset,
                limit,
                m_aggregate);
    }

    public VoltDBPMergeExchange copyWithAggregate(VoltDBPSerialAggregate aggregate) {
        return new VoltDBPMergeExchange(
                getCluster(),
                traitSet,
                getInput(),
                m_splitCount,
                isTopExchange(),
                m_collationFieldExprs,
                m_offset,
                m_limit,
                aggregate);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        MergeReceivePlanNode rpn = new MergeReceivePlanNode();
        SendPlanNode spn = new SendPlanNode();
        rpn.addAndLinkChild(spn);

        AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        spn.addAndLinkChild(child);

        // Generate its own output schema
        NodeSchema rpnOutputSchema = RexConverter.convertToVoltDBNodeSchema(/*getInput().*/getRowType());
        rpn.setPreAggregateOutputSchema(rpnOutputSchema);
        // If there is an aggregate, generate aggregate output schema
        NodeSchema aggregateOutputSchma;
        if (m_aggregate != null) {
            aggregateOutputSchma = RexConverter.convertToVoltDBNodeSchema(m_aggregate.getRowType());
            rpn.setOutputSchema(aggregateOutputSchma);
            // Inline Aggregate
            AbstractPlanNode inlineAggregatePlanNode = m_aggregate.toPlanNode(this.getRowType());
            rpn.addInlinePlanNode(inlineAggregatePlanNode);
        } else {
            rpn.setOutputSchema(rpnOutputSchema);
        }
        // Must set HaveSignificantOutputSchema after its own schema is generated
        rpn.setHaveSignificantOutputSchema(true);

        // Collation must be converted to the inline OrderByPlanNode
        RelTrait collationTrait = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        OrderByPlanNode inlineOrderByPlanNode =
                VoltDBRexUtil.collationToOrderByNode((RelCollation) collationTrait, this.m_collationFieldExprs);
        rpn.addInlinePlanNode(inlineOrderByPlanNode);

        // Inline Limit and / or Offset
        if (m_limit != null || m_offset != null) {
            LimitPlanNode inlineLimitPlanNode = VoltDBPLimit.toPlanNode(m_limit, m_offset);
            rpn.addInlinePlanNode(inlineLimitPlanNode);
        }

        return rpn;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        if (m_offset != null) {
            digest += "_offset_" + m_offset.toString();
        }
        if (m_limit != null) {
            digest += "_limit_" + m_limit.toString();
        }
        if (m_aggregate != null) {
            digest += "_aggr_" + m_aggregate.toString();
        }
        return digest;
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }
}
