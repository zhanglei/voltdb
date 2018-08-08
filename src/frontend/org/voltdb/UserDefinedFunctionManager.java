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

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Function;
import org.voltdb.utils.JavaBuiltInFunctions;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * This is the Java class that manages the UDF class instances, and also the invocation logics.
 */
public class UserDefinedFunctionManager {

    static final String ORGVOLTDB_FUNCCNAME_ERROR_FMT =
            "VoltDB does not support function classes with package names " +
            "that are prefixed with \"org.voltdb\". Please use a different " +
            "package name and retry. The class name was %s.";
    static final String UNABLETOLOAD_ERROR_FMT =
            "VoltDB was unable to load a function (%s) which was expected to be " +
            "in the catalog jarfile and will now exit.";

    ImmutableMap<Integer, ScalarUserDefinedFunctionRunner> m_udfs = ImmutableMap.<Integer, ScalarUserDefinedFunctionRunner>builder().build();

    public ScalarUserDefinedFunctionRunner getFunctionRunnerById(int functionId) {
        return m_udfs.get(functionId);
    }

    // Load all the UDFs recorded in the catalog. Instantiate and register them in the system.
    public void loadFunctions(CatalogContext catalogContext) {
        final CatalogMap<Function> catalogFunctions = catalogContext.database.getFunctions();
        // Remove obsolete tokens
        for (UserDefinedFunctionRunner runner : m_udfs.values()) {
            // The function that the current UserDefinedFunctionRunner is referring to
            // does not exist in the catalog anymore, we need to remove its token.
            if (catalogFunctions.get(runner.m_functionName) == null) {
                FunctionForVoltDB.deregisterUserDefinedFunction(runner.m_functionName);
            }
        }
        // Build new UDF runners
        ImmutableMap.Builder<Integer, ScalarUserDefinedFunctionRunner> builder =
                            ImmutableMap.<Integer, ScalarUserDefinedFunctionRunner>builder();
        for (final Function catalogFunction : catalogFunctions) {
            final String className = catalogFunction.getClassname();
            Class<?> funcClass = null;
            try {
                funcClass = catalogContext.classForProcedureOrUDF(className);
            }
            catch (final ClassNotFoundException e) {
                if (className.startsWith("org.voltdb.")) {
                    String msg = String.format(ORGVOLTDB_FUNCCNAME_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
                else {
                    String msg = String.format(UNABLETOLOAD_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }
            Object funcInstance = null;
            try {
                funcInstance = funcClass.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(String.format("Error instantiating function \"%s\"", className), e);
            }
            assert(funcInstance != null);
            builder.put(catalogFunction.getFunctionid(), new ScalarUserDefinedFunctionRunner(catalogFunction, funcInstance));
        }

        loadBuiltInJavaFunctions(builder);
        m_udfs = builder.build();
    }

    private void loadBuiltInJavaFunctions(ImmutableMap.Builder<Integer, ScalarUserDefinedFunctionRunner> builder) {
        // define the function objects
        String[] functionNames = {"format_timestamp"};
        for (String functionName : functionNames) {
            int functionID = FunctionForVoltDB.getFunctionID(functionName);
            builder.put(functionID, new ScalarUserDefinedFunctionRunner(functionName,
                    functionID, functionName, new JavaBuiltInFunctions()));
        }
    }
}
