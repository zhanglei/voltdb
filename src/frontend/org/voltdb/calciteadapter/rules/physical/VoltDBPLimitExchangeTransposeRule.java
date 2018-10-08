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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPLimit;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSingletonExchange;

import com.google.common.collect.ImmutableList;

/**
 * INSTANCE_1 Transform Limit with RelDistribution.ANY / Exchange rels into
 *  a) Coordinator Limit / Exchange / Fragment Limit
 *      if the original Exchange relation is a Union or Merge Exchanges
 *  b) Singleton Exchange / Limit if the original Exchange relation is a Singleton
 */
public class VoltDBPLimitExchangeTransposeRule extends RelOptRule {

    public static final VoltDBPLimitExchangeTransposeRule INSTANCE_1 =
            new VoltDBPLimitExchangeTransposeRule(operand(
                        VoltDBPLimit.class,
                        RelDistributions.ANY,
                        new RelOptRuleOperandChildren(
                                RelOptRuleOperandChildPolicy.ANY,
                                ImmutableList.of(
                                        operand(AbstractVoltDBPExchange.class, any())))),
                    "VoltDBPLimitExchangeTransposeRule_1"
                );

    private VoltDBPLimitExchangeTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    /**
     * Create a rule to match
     *          Coordinator Limit / SerialAggregate / MergeExchange
     * and push the Limit to the Fragment to be
     *          Coordinator Limit / SerialAggregate / MergeExchange / Fragment Limit
     *
     * This is possible only because the coordinator's SerialAggregate was already transposed
     * with the MergeExchange relation (It must have the desired distribution trait) and we know
     * that there is a fragment aggregate that matches the coordinator's one. The AggregateExchangeTranspose rule
     * already fired. Otherwise, the coordinator's aggregate would have RelDistribution.ANY trait
     *
     * The rule is be registered by the Aggregate / Exchange Transpose rule dynamically.
     *
     * @param trait - RelDistribution trait (HASH) that coordinator's SerialAggregate must have
     * @return
     */
    public static VoltDBPLimitExchangeTransposeRule makeRule(RelDistribution trait) {
        // Must be unique
        String ruleDsc = "VoltDBPLimitExchangeTransposeRule_" + trait.hashCode();
        assert(RelDistribution.Type.ANY != trait.getType());
        return  new VoltDBPLimitExchangeTransposeRule(operand(
                VoltDBPLimit.class,
                RelDistributions.ANY,
                new RelOptRuleOperandChildren(
                        RelOptRuleOperandChildPolicy.ANY,
                        ImmutableList.of(
                                operand(VoltDBPSerialAggregate.class,
                                        trait,
                                        new RelOptRuleOperandChildren(
                                                RelOptRuleOperandChildPolicy.ANY,
                                                ImmutableList.of(
                                                        operand(VoltDBPMergeExchange.class, any()))))))),
                ruleDsc
        );

    }
    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPLimit limitRel = call.rel(0);
        assert(limitRel != null);
        AbstractVoltDBPExchange exchangeRel;
        RelNode limitInput;
        if (call.rels.length == 2) {
            exchangeRel = call.rel(1);
            limitInput = null;
        } else {
            exchangeRel = call.rel(2);
            limitInput = call.rel(1);
        }

        RelNode result;
        if (exchangeRel instanceof VoltDBPSingletonExchange) {
            assert(call.rels.length == 2);
            result = transposeSingletonExchange((VoltDBPSingletonExchange) exchangeRel, limitRel);
        } else {
            result = transposeDistributedExchange(exchangeRel, limitRel, limitInput);
        }

        call.transformTo(result);
    }

    private RelNode transposeSingletonExchange(VoltDBPSingletonExchange origExchangeRel, VoltDBPLimit origLimitRel) {
        // Simply push the limit through the exchange
        RelTraitSet exchangeTraits = origExchangeRel.getTraitSet();
        RelTrait collationTrait = exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE);
        RelDistribution distributionTrait = origExchangeRel.getDistribution();
        // Update Limit distribution's and collation's traits
        RelTraitSet newLimitTraits = origLimitRel.getTraitSet()
                .replace(collationTrait);
        // Do not change distribution trait if this is a top exchange.
        // The trait will be updated when a limit relation will be transposed with a bottom(fragment) exchange
        if (!origExchangeRel.isTopExchange()) {
            newLimitTraits = newLimitTraits.replace(distributionTrait);
        }
        VoltDBPLimit newLimitRel = origLimitRel.copy(
                newLimitTraits,
                origExchangeRel.getInput(),
                origLimitRel.getOffset(),
                origLimitRel.getLimit(),
                origExchangeRel.getSplitCount());

        AbstractVoltDBPExchange newExchange = origExchangeRel.copy(
                exchangeTraits,
                newLimitRel,
                distributionTrait,
                origExchangeRel.isTopExchange());
        return newExchange;
    }

    private RelNode transposeDistributedExchange(
            AbstractVoltDBPExchange origExchangeRel, VoltDBPLimit origLimitRel, RelNode origLimitInputRel) {
        RelTraitSet exchangeTraits = origExchangeRel.getTraitSet();
        RelTrait collationTrait = exchangeTraits.getTrait(RelCollationTraitDef.INSTANCE);
        RelDistribution distributionTrait = origExchangeRel.getDistribution();

        // We can not push just an OFFSET through a distributed exchange
        AbstractVoltDBPExchange newExchangeRel;
        if (origLimitRel.getLimit() == null) {
            newExchangeRel = origExchangeRel;
        } else {
            // The fragment limit always has 0 offset and its limit =
            // sum of the coordinator's limit and offset
            int fragmentLimit = RexLiteral.intValue(origLimitRel.getLimit());
            if (origLimitRel.getOffset() != null) {
                fragmentLimit += RexLiteral.intValue(origLimitRel.getOffset());
            }
            RelTraitSet fragmentLimitTraits = origLimitRel.getTraitSet()
                    .replace(collationTrait)
                    .replace(distributionTrait);

            RexBuilder rexBuilder = origLimitRel.getCluster().getRexBuilder();
            RexNode fragmentLimitRex = rexBuilder.makeBigintLiteral(new BigDecimal(fragmentLimit));
            RelNode fragmentLimitRel = origLimitRel.copy(
                    fragmentLimitTraits,
                    origExchangeRel.getInput(),
                    null,
                    fragmentLimitRex,
                    origExchangeRel.getSplitCount());
            newExchangeRel = origExchangeRel.copy(
                    exchangeTraits,
                    fragmentLimitRel,
                    origExchangeRel.getDistribution(),
                    origExchangeRel.isTopExchange());
        }

        // Coordinator's limit
        RelTraitSet coordinatorLimitTraits = origLimitRel.getTraitSet()
                                                .replace(collationTrait)
                                                .replace(origExchangeRel.getDistribution());

        RelNode coordinatorLimitInputRel;
        if (origLimitInputRel == null) {
            coordinatorLimitInputRel = newExchangeRel;
        } else {
            List<RelNode> inputRels = Collections.singletonList(newExchangeRel);
            coordinatorLimitInputRel = origLimitInputRel.copy(origLimitInputRel.getTraitSet(), inputRels);
        }
        VoltDBPLimit coordinatorLimitRel = origLimitRel.copy(
                coordinatorLimitTraits,
                coordinatorLimitInputRel,
                origLimitRel.getOffset(),
                origLimitRel.getLimit(),
                origLimitRel.getSplitCount());

        return coordinatorLimitRel;
    }

}
