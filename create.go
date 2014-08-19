package sqlg

type createDatabaseMaker struct {
    Format *Format
    Batch  *StatementBatch
}

func ifExistsNotExistsBlockMicrosoftCase(section Section,
    stat *Statement, format *Format, stack *CallStack) (*Statement, error) {
    sectionKind := section.GetSectionKind()
    newst := NewStatement(SS_EXEC)
    ef := NewExprFactory()
    var fnc Expr
    switch sectionKind {
    case SK_CREATE_DATABASE:
        sect := section.(*CreateDatabaseRoot)
        dbId := ef.FuncDef(ef.FuncDialectDef(
            DI_MSTSQL, "db_id({})", 1, 1))
        fnc = ef.IsNull(ef.Func(dbId, sect.DatabaseName))
    case SK_CREATE_TABLE:
        sect := section.(*CreateTableRoot)
        objectId := ef.FuncDef(ef.FuncDialectDef(
            DI_MSTSQL, "object_id({})", 1, 2))
        name := format.formatTableName(sect.Table.Name)
        fnc = ef.IsNull(ef.Func(objectId, name, "U"))
    case SK_DROP_DATABASE:
        sect := section.(*DropDatabaseRoot)
        dbId := ef.FuncDef(ef.FuncDialectDef(
            DI_MSTSQL, "db_id({})", 1, 1))
        fnc = ef.IsNotNull(ef.Func(dbId, sect.DatabaseName))
    case SK_DROP_TABLE:
        sect := section.(*DropTableRoot)
        objectId := ef.FuncDef(ef.FuncDialectDef(
            DI_MSTSQL, "object_id({})", 1, 2))
        name := format.formatTableName(sect.Table.Name)
        fnc = ef.IsNotNull(ef.Func(objectId, name, "U"))
    }
    context := NewExprBuildContext(sectionKind, SSK_EXPR1,
        stack, format, nil)
    stat2, err := fnc.GetSql(context)
    if err != nil {
        return nil, err
    }
    newst.writeString(format.getLeadingSpace())
    newst.appendStatPartsFormat("if %s begin", stat2)
    newst.writeString(format.sectionDivider)
    newst.appendStatPart(stat)
    newst.writeString(format.sectionDivider)
    newst.writeString(format.getLeadingSpace())
    newst.writeString("end")
    return newst, nil
}

func (this *createDatabaseMaker) buildCreateDatabaseSql(sect *CreateDatabaseRoot,
    stack *CallStack) error {
    if this.Format.DoIfObjectExistsNotExists() &&
        this.Format.Dialect == DI_MSTSQL {
        this.Format.incIndentLevel()
        defer this.Format.decIndentLevel()
    }
    err := sect.buildCreateDatabaseSql(this, this.Batch.Last(), stack)
    return err

}

func (this *createDatabaseMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct == false {
        var err error
        switch section.GetSectionKind() {
        case SK_CREATE_DATABASE:
            sect := section.(*CreateDatabaseRoot)
            err = this.buildCreateDatabaseSql(sect, stack)
            if err != nil {
                return err
            }
            if this.Format.DoIfObjectExistsNotExists() &&
                this.Format.Dialect == DI_MSTSQL {
                stat := this.Batch.Last()
                newstat, err := ifExistsNotExistsBlockMicrosoftCase(
                    section, stat, this.Format, stack)
                if err != nil {
                    return err
                }
                this.Batch.Replace(stat, newstat)
            }
        default:
            err = e("Unexpected section during generating "+
                "\"create database\" statement: %v", section)
        }
        return err
    }
    return nil
}

func (this *createDatabaseMaker) BuildSql(section Section,
    format *Format) error {
    this.Format = format
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    return iterateSqlParents(false, section, this.runMaker)
}

func (this *createDatabaseMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack, format *Format) *ExprBuildContext {
    context := NewExprBuildContext(sectionKind, subsectionKind, stack, format, nil)
    return context
}

type CreateDatabaseRoot struct {
    DatabaseName string
}

func NewCreateDatabaseRoot(databaseName string) *CreateDatabaseRoot {
    r := &CreateDatabaseRoot{DatabaseName: databaseName}
    return r
}

// TODO verify that primary key fields belong to table
func (this *CreateDatabaseRoot) buildCreateDatabaseSql(maker *createDatabaseMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString("create database ")
    if maker.Format.DoIfObjectExistsNotExists() {
        switch maker.Format.Dialect {
        case DI_MYSQL:
            stat.writeString("if not exists ")
        case DI_PGSQL:
            log.Warnf("%v dialect doesn't support \"IF NOT EXISTS\" option "+
                "for \"create database\" statement", maker.Format.Dialect)
        }
    }
    name := maker.Format.formatObjectName( /*this.Db.Name*/ this.DatabaseName)
    stat.writeString(name)
    return nil
}

func (this *CreateDatabaseRoot) GetSql(format *Format) (*StatementBatch, error) {
    m := &createDatabaseMaker{}
    err := m.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return m.Batch, err
}

func (this *CreateDatabaseRoot) GetSectionKind() SectionKind {
    return SK_CREATE_DATABASE
}

func (this *CreateDatabaseRoot) GetParent() Section {
    return nil
}

type createTableMaker struct {
    Format *Format
    Batch  *StatementBatch
}

func (this *createTableMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct == false {
        var err error
        switch section.GetSectionKind() {
        case SK_CREATE_TABLE:
            sect := section.(*CreateTableRoot)
            err = sect.buildCreateTableSql(this, stack)
            if err != nil {
                return err
            }
        default:
            err = e("Unexpected section during generating "+
                "\"created table\" statement: %v", section)
        }
        return err
    }
    return nil
}

func (this *createTableMaker) BuildSql(section Section,
    format *Format) error {
    f := *format
    this.Format = &f
    // SQLite create table statment doesn't support not-inline constructions
    if this.Format.Dialect == DI_SQLITE {
        this.Format.AddOptions(BO_INLINE)
    }
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    return iterateSqlParents(false, section, this.runMaker)
}

func (this *createTableMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack,
    format *Format) *ExprBuildContext {
    context := NewExprBuildContext(sectionKind, subsectionKind, stack, format, nil)
    return context
}

type CreateTableRoot struct {
    Table *TableDef
}

// TODO add "check if exists" parameter
func NewCreateTableRoot(table *TableDef) *CreateTableRoot {
    r := &CreateTableRoot{Table: table}
    return r
}

func (this *CreateTableRoot) getSqlFieldDataType(stat *Statement,
    format *Format, stack *CallStack, field *FieldDef) error {
    data := field.Data.GetStrTemplate(format.Dialect)
    if data != nil {
        switch data.ParamCount {
        case 0:
            stat.writeString(data.Template)
            return nil
        case 1:
            stat.writeString(data.Template, field.Data.Size1)
            return nil
        case 2:
            stat.writeString(data.Template, field.Data.Size1,
                field.Data.Size2)
            return nil
        }
    }
    return e("Can't produce sql statement for data \"%v\""+
        " in notation \"%v\"", field.Data, format.Dialect)
}

func (this *CreateTableRoot) getSqlFieldNullable(stat *Statement,
    format *Format, stack *CallStack,
    field *FieldDef) error {
    stat.writeString(" ")
    if field.IsNullable {
        stat.writeString("null")
    } else {
        stat.writeString("not null")
    }
    return nil
}

func (this *CreateTableRoot) getSqlFieldDefault(stat *Statement,
    sectionKind SectionKind, subsectionKind SubsectionKind,
    stack *CallStack, format *Format, field *FieldDef) error {
    if field.Default != nil {
        stat.writeString(" ")
        context := NewExprBuildContext(sectionKind, subsectionKind, stack, format, nil)
        if field.Default.Value != nil {
            stat2, err := field.Default.Value.GetSql(context)
            if err != nil {
                return err
            }
            stat.appendStatPartsFormat("default %s", stat2)
        } else {
            stat.writeString("default null")
        }
    }
    return nil
}

func (this *CreateTableRoot) getSqlFieldAttr(stat *Statement,
    format *Format, stack *CallStack,
    field *FieldDef) error {
    if format.Dialect == DI_MYSQL &&
        field.Data.Type.In(DT_AUTOINC_INT|DT_AUTOINC_INT_BIG) {
        stat.writeString(" auto_increment")
    }
    return nil
}

type BuildSqlFieldRule struct {
    DataTypes      DataType
    ShowNullable   bool
    CustomAttr1    string
    ShowPrimaryKey bool
}

func bsfr(dataTypes DataType, showNullable bool, customAttr1 string,
    showPrimaryKey bool) *BuildSqlFieldRule {
    cfd := &BuildSqlFieldRule{DataTypes: dataTypes,
        ShowNullable:   showNullable,
        CustomAttr1:    customAttr1,
        ShowPrimaryKey: showPrimaryKey}
    return cfd
}

type BuildSqlFieldVarianceRule struct {
    PrimaryKeyInline bool
    Items            []*BuildSqlFieldRule
}

func bsfvr(primaryKeyInline bool, items ...*BuildSqlFieldRule) *BuildSqlFieldVarianceRule {
    btd := &BuildSqlFieldVarianceRule{PrimaryKeyInline: primaryKeyInline, Items: items}
    return btd
}

func (this *CreateTableRoot) getBuildSqlFieldVarianceRule(
    dialect Dialect) *BuildSqlFieldVarianceRule {
    tmplt := map[Dialect]*BuildSqlFieldVarianceRule{
        DI_MSTSQL: bsfvr(false, bsfr(DT_ALL, true, "", false)),
        DI_PGSQL:  bsfvr(false, bsfr(DT_ALL, true, "", false)),
        DI_MYSQL: bsfvr(true, bsfr(DT_AUTOINC_INT|DT_AUTOINC_INT_BIG,
            true, "auto_increment", true),
            bsfr(DT_ALL, true, "", true)),
        DI_SQLITE: bsfvr(true, bsfr(DT_AUTOINC_INT|DT_AUTOINC_INT_BIG,
            false, "primary key autoincrement", false),
            bsfr(DT_ALL, true, "", true)),
    }
    if btd, ok := tmplt[dialect]; ok {
        return btd
    }
    return nil
}

func (this *CreateTableRoot) getSqlField(stat *Statement,
    format *Format, stack *CallStack, field *FieldDef) error {
    bsfvr := this.getBuildSqlFieldVarianceRule(format.Dialect)
    var bsfr *BuildSqlFieldRule
    if bsfvr != nil {
        for _, item := range bsfvr.Items {
            if field.Data.Type.In(item.DataTypes) {
                bsfr = item
                break
            }
        }
        if bsfvr != nil {
            stat.writeString(format.formatObjectName(field.Name))
            stat.writeString(" ")
            err := this.getSqlFieldDataType(stat, format, stack, field)
            if err != nil {
                return err
            }
            if bsfr.ShowNullable {
                err = this.getSqlFieldNullable(stat, format, stack, field)
                if err != nil {
                    return err
                }
            }
            err = this.getSqlFieldDefault(stat, SK_CREATE_TABLE, SSK_EXPR1,
                stack, format, field)
            if err != nil {
                return err
            }
            if bsfr.CustomAttr1 != "" {
                stat.writeString(" ")
                stat.writeString(bsfr.CustomAttr1)
            }
            if bsfvr.PrimaryKeyInline && bsfr.ShowPrimaryKey &&
                field.GetOrAdviceIsPrimaryKey() {
                stat.writeString(" primary key")
            }
        }
    }
    return nil
}

func (this *CreateTableRoot) buildCreateTableMainSql(maker *createTableMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.getLeadingSpace())
    stat.writeString("create table ")
    if maker.Format.DoIfObjectExistsNotExists() &&
        maker.Format.Dialect.In(DI_PGSQL|DI_MYSQL|DI_SQLITE) {
        stat.writeString("if not exists ")
    }
    name := maker.Format.formatTableName(this.Table.Name /*, this.Db.Name*/)
    stat.writeString("%s (", name)
    stat.writeString(maker.Format.sectionDivider)
    maker.Format.incIndentLevel()
    // add fields
    for i, field := range this.Table.Fields.Items {
        if i > 0 {
            stat.writeString(",")
            stat.writeString(maker.Format.sectionDivider)
        }
        stat.writeString(maker.Format.getLeadingSpace())
        err := this.getSqlField(stat, maker.Format, stack, field)
        if err != nil {
            maker.Format.decIndentLevel()
            return err
        }
    }
    maker.Format.decIndentLevel()
    return nil
}

func (this *CreateTableRoot) buildPrimaryKeySql(maker *createTableMaker,
    stat *Statement, stack *CallStack) error {
    maker.Format.incIndentLevel()
    bsfvr := this.getBuildSqlFieldVarianceRule(maker.Format.Dialect)
    pk := this.Table.GetOrAdvicePrimaryKey()
    if bsfvr.PrimaryKeyInline == false && len(pk.Items) > 0 {
        if maker.Format.Dialect != DI_SQLITE {
            stat.writeString(",")
            stat.writeString(maker.Format.sectionDivider)
            stat.writeString(maker.Format.getLeadingSpace())
            stat.writeString(f("constraint %s primary key (",
                maker.Format.formatObjectName(pk.Name)))
            for i, field := range pk.Items {
                if i > 0 {
                    stat.writeString(", ")
                }
                stat.writeString(f("%s",
                    maker.Format.formatObjectName(field.Name)))
            }
            stat.writeString(")")
        }
    }
    if len(pk.Items) == 0 {
        log.Warn(f("No primary key defined or "+
            "can be adviced for table \"%s\"", this.Table.Name))
    }
    stat.writeString(")")
    maker.Format.decIndentLevel()
    return nil
}

func (this *CreateTableRoot) buildIndexesSql(maker *createTableMaker,
    stat *Statement, stack *CallStack) error {
    if len(this.Table.Indexes.Items) > 0 {
        //        stat.writeString(";")
        for _, index := range this.Table.Indexes.Items {
            if len(index.Items) > 0 {
                //                stat.writeString(maker.Format.sectionDivider)
                stat.writeString(maker.Format.getLeadingSpace())
                stat.writeString(f("create index %s", maker.Format.
                    formatObjectName(index.Name)))
                stat.writeString(maker.Format.sectionDivider)
                maker.Format.incIndentLevel()
                stat.writeString(maker.Format.getLeadingSpace())
                stat.writeString(f("on %s (", maker.Format.
                    formatObjectName(this.Table.Name)))
                for i, field := range index.Items {
                    if i > 0 {
                        stat.writeString(",")
                    }
                    stat.writeString(f("%s", maker.Format.
                        formatObjectName(field.Name)))
                }
                stat.writeString(")")
                maker.Format.decIndentLevel()
            }
        }
    }
    return nil
}

func (this *CreateTableRoot) preBuildCreateTableSql(maker *createTableMaker,
    stack *CallStack) error {
    stat := maker.Batch.Last()
    // build create statement itself
    err := this.buildCreateTableMainSql(maker, stat, stack)
    if err != nil {
        return err
    }
    // add primary key
    err = this.buildPrimaryKeySql(maker, stat, stack)
    if err != nil {
        return err
    }
    // add indexes
    var stat2 *Statement
    if len(this.Table.Indexes.Items) > 0 {
        stat2 = NewStatement(SS_EXEC)
        err = this.buildIndexesSql(maker, stat2, stack)
        if err != nil {
            return err
        }
        maker.Batch.Add(stat2)
    }
    // join statement if necessary
    err = maker.Batch.Join(maker.Format)
    return err
}

func (this *CreateTableRoot) preBuildCreateTableSql2(maker *createTableMaker,
    stack *CallStack) error {
    if maker.Format.Dialect == DI_MSTSQL {
        maker.Format.incIndentLevel()
        defer maker.Format.decIndentLevel()
    }
    return this.preBuildCreateTableSql(maker, stack)
}

func (this *CreateTableRoot) buildCreateTableSql(maker *createTableMaker,
    stack *CallStack) error {
    err := this.preBuildCreateTableSql2(maker, stack)
    if err != nil {
        return err
    }
    if maker.Format.DoIfObjectExistsNotExists() &&
        maker.Format.Dialect == DI_MSTSQL && len(maker.Batch.Items) == 1 {
        stat := maker.Batch.Last()
        newstat, err := ifExistsNotExistsBlockMicrosoftCase(this,
            stat, maker.Format, stack)
        if err != nil {
            return err
        }
        maker.Batch.Replace(stat, newstat)
    }
    return nil
}

func (this *CreateTableRoot) GetSql(format *Format) (*StatementBatch, error) {
    maker := &createTableMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, nil
}

func (this *CreateTableRoot) GetSectionKind() SectionKind {
    return SK_CREATE_TABLE
}

func (this *CreateTableRoot) GetParent() Section {
    return nil
}
