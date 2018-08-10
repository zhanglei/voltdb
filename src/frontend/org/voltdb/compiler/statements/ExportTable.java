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

import java.util.List;
import java.util.regex.Matcher;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process EXPORT TABLE table-name TO TARGET connector-name
 */
public class ExportTable extends StatementProcessor {

    public ExportTable(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    // Generate a create stream statement based on the schema of the input table
    private String wrapCreateStreamStatement(
            VoltXMLElement tableXML,
            String tableName,
            String connectorName) throws VoltCompilerException {
        String partitionColumn = tableXML.attributes.get("partitioncolumn");
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STREAM ").append(tableName).append("_EXPORT_").append(connectorName);
        if (partitionColumn != null) {
            sb.append(" PARTITION ON COLUMN ").append(partitionColumn);
        }
        sb.append(" EXPORT TO TARGET ").append(connectorName).append(" (");
        VoltXMLElement columns = tableXML.findChild("columnscolumns");
        if (columns != null) {
            List<VoltXMLElement> columnList = columns.findChildren("column");
            for (VoltXMLElement col : columnList) {
                String colName = col.attributes.get("name");
                String colType = col.attributes.get("valuetype");
                sb.append(colName).append(" ").append(colType);
                if (colType.equalsIgnoreCase("VARCHAR") || colType.equalsIgnoreCase("VARBINARY")) {
                    int colSize = col.getIntAttribute("size", 0);
                    sb.append("(").append(colSize);
                    boolean inBytes = col.getBoolAttribute("bytes", false);
                    if (inBytes) {
                        sb.append(" BYTES");
                    }
                    sb.append(")");
                }
                boolean nullable = col.getBoolAttribute("nullable", false);
                if (!nullable) {
                    sb.append(" NOT NULL");
                }
                int colIndex = col.getIntAttribute("index", 0);
                if (colIndex != columnList.size() - 1) {
                    sb.append(",");
                }
            }
        } else {
            throw m_compiler.new VoltCompilerException(String.format(
                    "While configuring export, table %s has no column in the catalog.", tableName));
        }
        sb.append(");");
//        System.out.println("\n\n" + sb.toString());
        return sb.toString();
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // matches if it is EXPORT TABLE <table-name> TO TARGET <connector-name>
        // group 1 -- table name
        // group 2 -- connector name
        Matcher statementMatcher = SQLParser.matchExportTable(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        String tableName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);

        //System.out.println("\n\n" + m_schema.toString());

        VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
        String connectorName = null;
        if (tableXML != null) {
            if ((statementMatcher.group(2) != null)) {
                connectorName = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
            } else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "While configuring export, connector %s was not present in the catalog.", tableName));
            }
        } else {
            throw m_compiler.new VoltCompilerException(String.format(
                    "While configuring export, table %s was not present in the catalog.", tableName));
        }
        // Create DDL statement for export table wrapper
        ddlStatement.statement = wrapCreateStreamStatement(tableXML, tableName, connectorName);
        m_returnAfterThis = true;

        return false;
    }

}
