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

package org.voltdb;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.catalog.Function;

public class AggregateUserDefinedFunctionRunner extends UserDefinedFunctionRunner {

    // This function ID start value should match FUNC_VOLT_AGG_UDF_ID_START in FunctionForVoltDB.FunctionDescriptor
    private static final int FUNC_VOLT_AGG_UDF_ID_START = 2_000_000;
    private static AtomicInteger s_nextAggUDFId = new AtomicInteger(FUNC_VOLT_AGG_UDF_ID_START);

    private Method m_initMethod;
    private Method m_mergeMethod;
    private Method m_accumulateMethod;
    private Method m_finalizeMethod;

    public AggregateUserDefinedFunctionRunner(Function catalogFunction, Object funcInstance) {
        super(catalogFunction.getFunctionname(), catalogFunction.getFunctionid(), funcInstance);

        Class<?> funcClass = m_functionInstance.getClass();
        try {
            // Look for methods. We do not give detailed error message here because the checks should have been done in CreateFunctionFromClass.java
            // void init()
            m_initMethod = funcClass.getDeclaredMethod("init");
            // void merge( {class name} other)
            m_mergeMethod = funcClass.getDeclaredMethod("merge", funcClass);
            for (final Method m : funcClass.getDeclaredMethods()) {
                if (m.getName().equals("accumulate") && m_accumulateMethod == null) {
                    // The input parameters are the parameters for the aggregate function.
                    if (m.getParameterCount() == 0) {
                        continue;
                    }
                    Class<?>[] paramTypeClasses = m.getParameterTypes();
                    m_paramCount = paramTypeClasses.length;
                    m_paramTypes = new VoltType[m_paramCount];
                    m_boxUpByteArray = new boolean[m_paramCount];
                    for (int i = 0; i < m_paramCount; i++) {
                        m_paramTypes[i] = VoltType.typeFromClass(paramTypeClasses[i]);
                        m_boxUpByteArray[i] = paramTypeClasses[i] == Byte[].class;
                    }
                    m_accumulateMethod = m;
                }
                else if (m.getName().equals("finalize") && m_finalizeMethod == null) {
                    // The return type of the finalize() is the return type of the aggregate function.
                    if (m.getParameterCount() > 0) {
                        continue;
                    }
                    m_returnType = VoltType.typeFromClass(m.getReturnType());
                    m_finalizeMethod = m;
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Error loading aggregate function " + m_functionName, ex);
        }
    }

    public static boolean isUserDefinedAggregateID(int id) {
        return id >= FUNC_VOLT_AGG_UDF_ID_START;
    }

    public static int getNextFunctionId() {
        return s_nextAggUDFId.getAndIncrement();
    }
}
