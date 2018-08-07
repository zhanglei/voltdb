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

package org.voltdb.compiler.statements;

import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.FunctionSQL;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

/**
 * Process CREATE FUNCTION statements
 */
public abstract class CreateFunction extends StatementProcessor {
    protected static VoltLogger s_logger = new VoltLogger("UDF");
    protected static int ID_NOT_DEFINED = -1;
    private static Set<Class<?>> s_allowedDataTypes = new HashSet<>();

    static {
        s_allowedDataTypes.add(byte.class);
        s_allowedDataTypes.add(byte[].class);
        s_allowedDataTypes.add(short.class);
        s_allowedDataTypes.add(int.class);
        s_allowedDataTypes.add(long.class);
        s_allowedDataTypes.add(double.class);
        s_allowedDataTypes.add(Byte.class);
        s_allowedDataTypes.add(Byte[].class);
        s_allowedDataTypes.add(Short.class);
        s_allowedDataTypes.add(Integer.class);
        s_allowedDataTypes.add(Long.class);
        s_allowedDataTypes.add(Double.class);
        s_allowedDataTypes.add(BigDecimal.class);
        s_allowedDataTypes.add(String.class);
        s_allowedDataTypes.add(TimestampType.class);
        s_allowedDataTypes.add(GeographyPointValue.class);
        s_allowedDataTypes.add(GeographyValue.class);
    }

    protected static boolean allowDataType(Class<?> clazz) {
        return s_allowedDataTypes.contains(clazz);
    }

    protected Class<?> loadAndCheckClass(String className) throws VoltCompilerException {
        // Load the function class
        Class<?> funcClass = null;
        try {
            funcClass = Class.forName(className, true, m_classLoader);
        }
        catch (Throwable cause) {
            // We are here because either the class was not found or the class was found but
            // the initializer of the class threw an error we can't anticipate. So we will
            // wrap the error with a runtime exception that we can trap in our code.
            if (CoreUtils.isJARThrowableFatalToServer(cause)) {
                throw (Error)cause;
            }
            else {
                throw m_compiler.new VoltCompilerException(
                        String.format("Cannot load class for user-defined function: %s", className), cause);
            }
        }

        if (Modifier.isAbstract(funcClass.getModifiers())) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Cannot define a function using an abstract class %s", className));
        }
        return funcClass;
    }

    /**
     * Find out if the function is defined.  It might be defined in the
     * FunctionForVoltDB table.  It also might be in the VoltXML.
     *
     * @param functionName
     * @return
     */
    protected boolean isDefinedFunctionName(String functionName) {
        return FunctionForVoltDB.isFunctionNameDefined(functionName)
                || FunctionSQL.isFunction(functionName)
                || FunctionCustom.getFunctionId(functionName) != ID_NOT_DEFINED
                || (null != m_schema.findChild("ud_function", functionName));
    }

    public CreateFunction(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }
}
