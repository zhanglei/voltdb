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

package org.voltdb.calciteadapter.rules.inlining;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.voltdb.calciteadapter.rel.physical.VoltDBPMergeExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;

public class VoltDBPSerialAggregateMergeExchangeMergeRule extends RelOptRule {

    public static final VoltDBPSerialAggregateMergeExchangeMergeRule INSTANCE =
            new VoltDBPSerialAggregateMergeExchangeMergeRule(
                    operand(VoltDBPSerialAggregate.class,
                            operand(VoltDBPMergeExchange.class, any()))
                    );

    /**
     * Transform  VoltDBPSerialAggregate / VoltDBPMergeExchange to VoltDBPMergeExchange with Aggregate
     */
    private VoltDBPSerialAggregateMergeExchangeMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPSerialAggregate aggregate = call.rel(0);
        VoltDBPMergeExchange exchange = call.rel(1);

        VoltDBPMergeExchange newExchange = exchange.copyWithAggregate(aggregate);
        call.transformTo(newExchange);
    }

}
