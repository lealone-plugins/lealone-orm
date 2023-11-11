/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm;

import java.util.List;
import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.Database;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Column.ListColumn;
import org.lealone.db.table.Column.MapColumn;
import org.lealone.db.table.Column.SetColumn;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableCodeGeneratorBase;
import org.lealone.db.table.TableSetting;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.sql.ddl.CreateService;

public class ModelCodeGenerator extends TableCodeGeneratorBase {

    public ModelCodeGenerator() {
        super("default_table_code_generator");
    }

    @Override
    public void genCode(ServerSession session, Table table, Table owner, int level) {
        String packageName = table.getPackageName();
        String tableName = table.getName();
        String className = CreateService.toClassName(tableName);
        Database db = session.getDatabase();
        Schema schema = table.getSchema();
        boolean databaseToUpper = db.getSettings().databaseToUpper;

        for (ConstraintReferential ref : table.getReferentialConstraints()) {
            Table refTable = ref.getRefTable();
            if (refTable != table && level <= 1) { // 避免递归
                genCode(session, refTable, owner, ++level);
            }
        }

        // 收集需要导入的类
        TreeSet<String> importSet = new TreeSet<>();
        importSet.add("org.lealone.plugins.orm.Model");
        importSet.add("org.lealone.plugins.orm.ModelTable");
        importSet.add("org.lealone.plugins.orm.ModelProperty");
        importSet.add("org.lealone.plugins.orm.format.JsonFormat");

        for (ConstraintReferential ref : table.getReferentialConstraints()) {
            Table refTable = ref.getRefTable();
            owner = ref.getTable();
            if (refTable == table) {
                String pn = owner.getPackageName();
                if (!packageName.equals(pn)) {
                    importSet.add(pn + "." + CreateService.toClassName(owner.getName()));
                }
                importSet.add(List.class.getName());
            } else {
                String pn = refTable.getPackageName();
                if (!packageName.equals(pn)) {
                    importSet.add(pn + "." + CreateService.toClassName(refTable.getName()));
                }
            }
        }

        StringBuilder fields = new StringBuilder();
        StringBuilder fieldNames = new StringBuilder();
        StringBuilder initFields = new StringBuilder();

        for (Column c : table.getColumns()) {
            int type = c.getType();
            String modelPropertyClassName = getModelPropertyClassName(type, importSet);
            String columnName = CamelCaseHelper.toCamelFromUnderscore(c.getName());

            fields.append("    public final ").append(modelPropertyClassName).append('<')
                    .append(className);
            if (c instanceof ListColumn) {
                fields.append(", ");
                ListColumn lc = (ListColumn) c;
                fields.append(getTypeName(lc.element, importSet));
            } else if (c instanceof SetColumn) {
                fields.append(", ");
                SetColumn sc = (SetColumn) c;
                fields.append(getTypeName(sc.element, importSet));
            } else if (c instanceof MapColumn) {
                fields.append(", ");
                MapColumn mc = (MapColumn) c;
                fields.append(getTypeName(mc.key, importSet));
                fields.append(", ");
                fields.append(getTypeName(mc.value, importSet));
            }
            fields.append("> ").append(columnName).append(";\r\n");

            // 例如: id = new PLong<>("id", this);
            initFields.append("        ").append(columnName).append(" = new ")
                    .append(modelPropertyClassName).append("<>(\"")
                    .append(databaseToUpper ? c.getName().toUpperCase() : c.getName())
                    .append("\", this");
            if (c instanceof MapColumn) {
                MapColumn mc = (MapColumn) c;
                initFields.append(", ").append(getTypeName(mc.key, importSet)).append(".class");
            }
            initFields.append(");\r\n");
            if (fieldNames.length() > 0) {
                fieldNames.append(", ");
            }
            fieldNames.append(columnName);
        }

        /////////////////////////// 以下是表关联的相关代码 ///////////////////////////

        // associate method(set, get, add) buff
        StringBuilder amBuff = new StringBuilder();
        StringBuilder adderBuff = new StringBuilder();
        StringBuilder adderInitBuff = new StringBuilder();
        StringBuilder setterBuff = new StringBuilder();
        StringBuilder setterInitBuff = new StringBuilder();

        for (ConstraintReferential ref : table.getReferentialConstraints()) {
            Table refTable = ref.getRefTable();
            owner = ref.getTable();
            String refTableClassName = CreateService.toClassName(refTable.getName());
            if (refTable == table) {
                String ownerClassName = CreateService.toClassName(owner.getName());
                // add方法，增加单个model实例
                amBuff.append("    public ").append(className).append(" add").append(ownerClassName)
                        .append("(").append(ownerClassName).append(" m) {\r\n");
                amBuff.append("        m.set").append(refTableClassName).append("(this);\r\n");
                amBuff.append("        super.addModel(m);\r\n");
                amBuff.append("        return this;\r\n");
                amBuff.append("    }\r\n");
                amBuff.append("\r\n");

                // add方法，增加多个model实例
                amBuff.append("    public ").append(className).append(" add").append(ownerClassName)
                        .append("(").append(ownerClassName).append("... mArray) {\r\n");
                amBuff.append("        for (").append(ownerClassName).append(" m : mArray)\r\n");
                amBuff.append("            add").append(ownerClassName).append("(m);\r\n");
                amBuff.append("        return this;\r\n");
                amBuff.append("    }\r\n");
                amBuff.append("\r\n");

                // get list方法
                amBuff.append("    public List<").append(ownerClassName).append("> get")
                        .append(ownerClassName).append("List() {\r\n");
                amBuff.append("        return super.getModelList(").append(ownerClassName)
                        .append(".class);\r\n");
                amBuff.append("    }\r\n");
                amBuff.append("\r\n");

                // Adder类
                IndexColumn[] refColumns = ref.getRefColumns();
                IndexColumn[] columns = ref.getColumns();
                adderBuff.append("    protected class ").append(ownerClassName)
                        .append("Adder implements AssociateAdder<").append(ownerClassName)
                        .append("> {\r\n");
                adderBuff.append("        @Override\r\n");
                adderBuff.append("        public ").append(ownerClassName).append(" getDao() {\r\n");
                adderBuff.append("            return ").append(ownerClassName).append(".dao;\r\n");
                adderBuff.append("        }\r\n");
                adderBuff.append("\r\n");
                adderBuff.append("        @Override\r\n");
                adderBuff.append("        public void add(").append(ownerClassName).append(" m) {\r\n");
                adderBuff.append("            if (");
                for (int i = 0; i < columns.length; i++) {
                    if (i != 0) {
                        adderBuff.append(" && ");
                    }
                    String columnName = CamelCaseHelper
                            .toCamelFromUnderscore(columns[i].column.getName());
                    String refColumnName = CamelCaseHelper
                            .toCamelFromUnderscore(refColumns[i].column.getName());
                    adderBuff.append("areEqual(").append(refColumnName).append(", m.").append(columnName)
                            .append(")");
                }
                adderBuff.append(") {\r\n");
                adderBuff.append("                add").append(ownerClassName).append("(m);\r\n");
                adderBuff.append("            }\r\n");
                adderBuff.append("        }\r\n");
                adderBuff.append("    }\r\n");
                adderBuff.append("\r\n");

                // new Adder()
                if (adderInitBuff.length() > 0)
                    adderInitBuff.append(", ");
                adderInitBuff.append("new ").append(ownerClassName).append("Adder()");
            } else {
                String refTableVar = CamelCaseHelper.toCamelFromUnderscore(refTable.getName());

                // 引用表字段
                fields.append("    private ").append(refTableClassName).append(" ").append(refTableVar)
                        .append(";\r\n");

                // get方法
                amBuff.append("    public ").append(refTableClassName).append(" get")
                        .append(refTableClassName).append("() {\r\n");
                amBuff.append("        return ").append(refTableVar).append(";\r\n");
                amBuff.append("    }\r\n");
                amBuff.append("\r\n");

                // set方法
                amBuff.append("    public ").append(className).append(" set").append(refTableClassName)
                        .append("(").append(refTableClassName).append(" ").append(refTableVar)
                        .append(") {\r\n");
                amBuff.append("        this.").append(refTableVar).append(" = ").append(refTableVar)
                        .append(";\r\n");

                IndexColumn[] refColumns = ref.getRefColumns();
                IndexColumn[] columns = ref.getColumns();
                for (int i = 0; i < columns.length; i++) {
                    String columnName = CamelCaseHelper
                            .toCamelFromUnderscore(columns[i].column.getName());
                    String refColumnName = CamelCaseHelper
                            .toCamelFromUnderscore(refColumns[i].column.getName());
                    amBuff.append("        this.").append(columnName).append(".set(").append(refTableVar)
                            .append(".").append(refColumnName).append(".get());\r\n");
                }
                amBuff.append("        return this;\r\n");
                amBuff.append("    }\r\n");
                amBuff.append("\r\n");

                // Setter类
                setterBuff.append("    protected class ").append(refTableClassName)
                        .append("Setter implements AssociateSetter<").append(refTableClassName)
                        .append("> {\r\n");
                setterBuff.append("        @Override\r\n");
                setterBuff.append("        public ").append(refTableClassName).append(" getDao() {\r\n");
                setterBuff.append("            return ").append(refTableClassName).append(".dao;\r\n");
                setterBuff.append("        }\r\n");
                setterBuff.append("\r\n");
                setterBuff.append("        @Override\r\n");
                setterBuff.append("        public boolean set(").append(refTableClassName)
                        .append(" m) {\r\n");
                setterBuff.append("            if (");
                for (int i = 0; i < columns.length; i++) {
                    if (i != 0) {
                        setterBuff.append(" && ");
                    }
                    String columnName = CamelCaseHelper
                            .toCamelFromUnderscore(columns[i].column.getName());
                    String refColumnName = CamelCaseHelper
                            .toCamelFromUnderscore(refColumns[i].column.getName());
                    setterBuff.append("areEqual(").append(columnName).append(", m.")
                            .append(refColumnName).append(")");
                }
                setterBuff.append(") {\r\n");
                setterBuff.append("                set").append(refTableClassName).append("(m);\r\n");
                setterBuff.append("                return true;\r\n");
                setterBuff.append("            }\r\n");
                setterBuff.append("            return false;\r\n");
                setterBuff.append("        }\r\n");
                setterBuff.append("    }\r\n");
                setterBuff.append("\r\n");

                // new Setter()
                if (setterInitBuff.length() > 0)
                    setterInitBuff.append(", ");
                setterInitBuff.append("new ").append(refTableClassName).append("Setter()");
            }
        }

        /////////////////////////// 以下是生成model类的相关代码 ///////////////////////////

        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(packageName).append(";\r\n\r\n");
        for (String p : importSet) {
            buff.append("import ").append(p).append(";\r\n");
        }
        buff.append("\r\n");
        buff.append("/**\r\n");
        buff.append(" * Model for table '").append(tableName).append("'.\r\n");
        buff.append(" *\r\n");
        buff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        buff.append(" */\r\n");
        // 例如: public class Customer extends Model<Customer> {
        buff.append("public class ").append(className).append(" extends Model<").append(className)
                .append("> {\r\n");
        buff.append("\r\n");

        // static create 方法
        // buff.append(" public static ").append(className).append(" create(String url) {\r\n");
        // buff.append(" ModelTable t = new ModelTable(url, ").append(tableFullName).append(");\r\n");
        // buff.append(" return new ").append(className).append("(t, REGULAR_MODEL);\r\n");
        // buff.append(" }\r\n");
        // buff.append("\r\n");

        // static dao字段
        String daoName = table.getParameter(TableSetting.DAO_NAME.name());
        if (daoName == null)
            daoName = "dao";
        buff.append("    public static final ").append(className).append(" ").append(daoName)
                .append(" = new ").append(className).append("(null, ROOT_DAO);\r\n");
        buff.append("\r\n");

        // 字段
        buff.append(fields);
        buff.append("\r\n");

        // 默认构造函数
        buff.append("    public ").append(className).append("() {\r\n");
        buff.append("        this(null, REGULAR_MODEL);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");

        String tableFullName = "\"" + db.getName() + "\", \"" + schema.getName() + "\", \"" + tableName
                + "\"";
        if (databaseToUpper) {
            tableFullName = tableFullName.toUpperCase();
        }
        String jsonFormatName = table.getParameter(TableSetting.JSON_FORMAT.name());
        // 内部构造函数
        buff.append("    private ").append(className).append("(ModelTable t, short modelType) {\r\n");
        buff.append("        super(t == null ? new ModelTable(").append(tableFullName)
                .append(") : t, modelType);\r\n");
        buff.append(initFields);
        if (jsonFormatName != null)
            buff.append("        super.setJsonFormat(\"").append(jsonFormatName).append("\");\r\n");
        buff.append("        super.setModelProperties(new ModelProperty[] { ").append(fieldNames)
                .append(" });\r\n");
        if (setterInitBuff.length() > 0) {
            buff.append("        super.initSetters(").append(setterInitBuff).append(");\r\n");
        }
        if (adderInitBuff.length() > 0) {
            buff.append("        super.initAdders(").append(adderInitBuff).append(");\r\n");
        }
        buff.append("    }\r\n");
        buff.append("\r\n");

        // newInstance方法
        buff.append("    @Override\r\n");
        buff.append("    protected ").append(className)
                .append(" newInstance(ModelTable t, short modelType) {\r\n");
        buff.append("        return new ").append(className).append("(t, modelType);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");

        // associate method
        buff.append(amBuff);

        // Setter类
        if (setterBuff.length() > 0) {
            buff.append(setterBuff);
        }

        // Adder类
        if (adderBuff.length() > 0) {
            buff.append(adderBuff);
        }

        // static decode方法
        buff.append("    public static ").append(className).append(" decode(String str) {\r\n");
        buff.append("        return decode(str, null);\r\n");
        buff.append("    }\r\n\r\n");
        buff.append("    public static ").append(className)
                .append(" decode(String str, JsonFormat format) {\r\n");
        buff.append("        return new ").append(className).append("().decode0(str, format);\r\n");
        buff.append("    }\r\n");
        buff.append("}\r\n");

        CreateService.writeFile(table.getCodePath(), packageName, className, buff);
    }

    private static String getModelPropertyClassName(int type, TreeSet<String> importSet) {
        String name;
        switch (type) {
        case Value.BYTES:
            name = "Bytes";
            break;
        case Value.UUID:
            name = "Uuid";
            break;
        case Value.NULL:
            throw DbException.getInternalError("type = null");
        default:
            name = DataType.getTypeClassName(type);
            int pos = name.lastIndexOf('.');
            name = name.substring(pos + 1);
        }
        name = "P" + name;
        importSet.add("org.lealone.plugins.orm.property." + name);
        return name;
    }

    private static String getTypeName(Column c, TreeSet<String> importSet) {
        String name = CreateService.getTypeName(c, importSet);
        // if (name.equals("Object"))
        // name = "?";
        return name;
    }
}
