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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.regex.Matcher;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.AggregateUserDefinedFunctionRunner;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process CREATE FUNCTION <function-name> FROM CLASS <class-name>
 */
public class CreateFunctionFromClass extends CreateFunction {

    public CreateFunctionFromClass(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db,
            DdlProceduresToLoad whichProcs) throws VoltCompilerException {

        // Matches if it is CREATE FUNCTION <name> FROM CLASS <class-name>
        Matcher statementMatcher = SQLParser.matchCreateFunctionFromClass(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        // Clean up the names
        String functionName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement).toLowerCase();
        // Class name and method name are case sensitive.
        String className = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
        String msg;

        // Check if the function is already defined
        if (isDefinedFunctionName(functionName)) {
            msg = String.format("Function \"%s\" is already defined.", functionName);
            throw m_compiler.new VoltCompilerException(msg);
        }

        // Load the function class
        Class<?> funcClass = loadAndCheckClass(className);
        if (! (funcClass instanceof Serializable)) {
            throw m_compiler.new VoltCompilerException("User-defined aggregation function classes must be serializable.");
        }

        // Get the short name of the class (no package)
        String shortName = ProcedureCompiler.deriveShortProcedureName(className);

        // void init()
        // It is OK that the init() method generates a return value, but we will ignore it and give a warning to the user.
        Method initMethod = null;
        try {
            initMethod = funcClass.getDeclaredMethod("init");
        } catch (NoSuchMethodException e) {
            throw m_compiler.new VoltCompilerException("Class " + shortName + " does not have an init() method.");
        } catch (SecurityException e) {
            throw m_compiler.new VoltCompilerException("Error examining class " + shortName, e);
        }
        if (! initMethod.getReturnType().equals(Void.TYPE)) {
            m_compiler.addWarn(String.format("%s.init() generates a return value that will be ignored.", shortName));
        }

        // void merge( {class name} other)
        // Again, any return value will be ignored. And it must have a class instance of its own type as the only parameter.
        Method mergeMethod = null;
        try {
            mergeMethod = funcClass.getDeclaredMethod("merge", funcClass);
        } catch (NoSuchMethodException e) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Class %s does not have a merge(%s) method.", shortName, shortName));
        } catch (SecurityException e) {
            throw m_compiler.new VoltCompilerException("Error examining class " + shortName, e);
        }
        if (! mergeMethod.getReturnType().equals(Void.TYPE)) {
            m_compiler.addWarn(
                    String.format("%s.merge(%s) generates a return value that will be ignored.", shortName, shortName));
        }

        Method accumulateMethod = null;
        Method finalizeMethod = null;
        Class<?> returnTypeClass = null;
        Class<?>[] paramTypeClasses = null;
        for (final Method m : funcClass.getDeclaredMethods()) {
            if (m.getName().equals("accumulate")) {
                // The input parameters are the parameters for the aggregate function.
                if (m.getParameterCount() == 0) {
                    throw m_compiler.new VoltCompilerException(shortName + ".accumulate() cannot take no parameter.");
                }
                if (! m.getReturnType().equals(Void.TYPE)) {
                    m_compiler.addWarn(
                            String.format("%s.accumulate(...) generates a return value that will be ignored.", shortName));
                }
                paramTypeClasses = m.getParameterTypes();
                for (int i = 0; i < paramTypeClasses.length; i++) {
                    if (! allowDataType(paramTypeClasses[i])) {
                        msg = String.format("%s.accumulate(...) has an unsupported parameter type %s at position %d",
                                shortName, paramTypeClasses[i].getName(), i);
                        throw m_compiler.new VoltCompilerException(msg);
                    }
                }
                accumulateMethod = m;
            }
            else if (m.getName().equals("finalize")) {
                // The return type of the finalize() is the return type of the aggregate function.
                if (m.getParameterCount() > 0) {
                    throw m_compiler.new VoltCompilerException(shortName + ".finalize() cannot take any parameter.");
                }
                returnTypeClass = m.getReturnType();
                if (! allowDataType(returnTypeClass)) {
                    throw m_compiler.new VoltCompilerException("Unsupported finalize() method return type: " + returnTypeClass.getName());
                }
                finalizeMethod = m;
            }
        }
        if (accumulateMethod == null) {
            throw m_compiler.new VoltCompilerException("Cannot find a usable accumulate() method in class " + shortName);
        }
        if (finalizeMethod == null) {
            throw m_compiler.new VoltCompilerException("Cannot find a usable finalize() method in class " + shortName);
        }

        try {
            funcClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Error instantiating function \"%s\"", className), e);
        }

        VoltType voltReturnType = VoltType.typeFromClass(returnTypeClass);
        VoltType[] voltParamTypes = new VoltType[paramTypeClasses.length];
        VoltXMLElement funcXML = new VoltXMLElement("ud_function")
                                    .withValue("name", functionName)
                                    .withValue("className", className)
                                    .withValue("returnType", String.valueOf(voltReturnType.getValue()));
        for (int i = 0; i < paramTypeClasses.length; i++) {
            VoltType voltParamType = VoltType.typeFromClass(paramTypeClasses[i]);
            VoltXMLElement paramXML = new VoltXMLElement("udf_ptype")
                                         .withValue("type", String.valueOf(voltParamType.getValue()));
            funcXML.children.add(paramXML);
            voltParamTypes[i] = voltParamType;
        }

        int functionId = AggregateUserDefinedFunctionRunner.getNextFunctionId();
        funcXML.attributes.put("functionid", String.valueOf(functionId));
        s_logger.debug(String.format("Added XML for function \"%s\"", functionName));
        m_schema.children.add(funcXML);
        return true;
    }
}
