package org.voltdb;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltdb.catalog.Function;
import org.voltdb.utils.SerializationHelper;

public class ScalarUserDefinedFunctionRunner extends UserDefinedFunctionRunner {
    Method m_functionMethod;
    final boolean[] m_boxUpByteArray;

    public ScalarUserDefinedFunctionRunner(Function catalogFunction, Object funcInstance) {
        this(catalogFunction.getFunctionname(), catalogFunction.getFunctionid(),
                catalogFunction.getMethodname(), funcInstance);
    }

    public ScalarUserDefinedFunctionRunner(String functionName, int functionId, String methodName, Object funcInstance) {
        super(functionName, functionId, funcInstance);
        initFunctionMethod(methodName);
        Class<?>[] paramTypeClasses = m_functionMethod.getParameterTypes();
        m_paramCount = paramTypeClasses.length;
        m_paramTypes = new VoltType[m_paramCount];
        m_boxUpByteArray = new boolean[m_paramCount];
        for (int i = 0; i < m_paramCount; i++) {
            m_paramTypes[i] = VoltType.typeFromClass(paramTypeClasses[i]);
            m_boxUpByteArray[i] = paramTypeClasses[i] == Byte[].class;
        }
        m_returnType = VoltType.typeFromClass(m_functionMethod.getReturnType());

        s_logger.debug(String.format("The user-defined function manager is defining function %s (ID = %s)",
                m_functionName, m_functionId));

        // We register the token again when initializing the user-defined function manager because
        // in a cluster setting the token may only be registered on the node where the CREATE FUNCTION DDL
        // is executed. We uses a static map in FunctionDescriptor to maintain the token list.
        FunctionForVoltDB.registerTokenForUDF(m_functionName, m_functionId, m_returnType, m_paramTypes);
    }

    public Object call(ByteBuffer udfBuffer) throws Throwable {
        Object[] paramsIn = new Object[m_paramCount];
        for (int i = 0; i < m_paramCount; i++) {
            paramsIn[i] = getValueFromBuffer(udfBuffer, m_paramTypes[i]);
            if (m_boxUpByteArray[i]) {
                paramsIn[i] = SerializationHelper.boxUpByteArray((byte[])paramsIn[i]);
            }
        }
        return m_functionMethod.invoke(m_functionInstance, paramsIn);
    }

    private void initFunctionMethod(String methodName) {
        m_functionMethod = null;
        for (final Method m : m_functionInstance.getClass().getDeclaredMethods()) {
            if (m.getName().equals(methodName)) {
                if (! Modifier.isPublic(m.getModifiers())) {
                    continue;
                }
                if (Modifier.isStatic(m.getModifiers())) {
                    continue;
                }
                if (m.getReturnType().equals(Void.TYPE)) {
                    continue;
                }
                m_functionMethod = m;
                break;
            }
        }
        if (m_functionMethod == null) {
            throw new RuntimeException(
                    String.format("Error loading function %s: cannot find the %s() method.",
                            m_functionName, methodName));
        }
    }
}
