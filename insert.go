package sqlg

// Generate sql statment according to general INSERT syntax:
//      insert into table1
//      [(column1, column2, ...)]
//      values (value1, value2, ...)
//      [returning expr1, expr2, ...]
//
//      or
//
//      insert into table1
//      [(column1, column2, ...)]
//      select * from table2

type insertMaker struct {
    Format           *Format
    Batch            *StatementBatch
    TargetDataSource Query
    Returning        *InsertReturning
}

func (this *insertMaker) CheckFieldAndExprCountMatch(sect *InsertValues) error {
    r := getSectionRoot(sect)
    root := r.(*InsertRoot)
    c1 := len(root.Fields)
    if c1 != 0 {
        c2 := len(sect.Exprs)
        if c1 != c2 {
            return e("Destination field count doesn't match "+
                "select column count in INSERT statement: "+
                "%d <> %d", c1, c2)
        }
    }
    return nil
}

func (this *insertMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct {
        switch section.GetSectionKind() {
        case SK_INSERT:
            sect := section.(*InsertRoot)
            this.TargetDataSource = sect.DataSource
        case SK_INSERT_VALUES:
            sect := section.(*InsertValues)
            return this.CheckFieldAndExprCountMatch(sect)
        case SK_INSERT_RETURNING:
            sect := section.(*InsertReturning)
            this.Returning = sect
        case SK_INSERT_FROM:
            sect := section.(*InsertSelect)
            query, queryBased := sect.Select.(Query)
            if queryBased {
                r := getSectionRoot(section)
                root := r.(*InsertRoot)
                if this.Format.ColumnNameAndCountValidationIsOn() {
                    c1, err := query.GetColumnCount()
                    if err != nil {
                        return err
                    }
                    c2, err := root.getInsertFieldCount()
                    if err != nil {
                        return err
                    }
                    if c1 != c2 {
                        return e("Destination field count doesn't match "+
                            "select column count in INSERT statement: "+
                            "%d <> %d", c1, c2)
                    }
                }
            } else {
                return e("Can't insert from the source, since " +
                    "source statement is not sql-ready")
            }
        }
    } else {

        var err error
        switch section.GetSectionKind() {
        case SK_INSERT:
            sect := section.(*InsertRoot)
            err = sect.buildInsertSectionSql(this, this.Batch.Last(), stack, this.Returning)
        case SK_INSERT_VALUES:
            sect := section.(*InsertValues)
            err = sect.buildValuesSectionSql(this, this.Batch.Last(), stack)
        case SK_INSERT_RETURNING:
            sect := section.(*InsertReturning)
            ef := NewExprFactory()
            getLastId := ef.FuncDef(
                ef.FuncDialectDef(DI_MYSQL, "last_insert_id()", 0, 0),
                ef.FuncDialectDef(DI_SQLITE, "last_insert_rowid()", 0, 0))
            switch this.Format.Dialect {
            case DI_PGSQL:
                stat := this.Batch.Last()
                err = sect.buildReturningSectionSql(this, stat, stack)
                stat.Type = SS_QUERY
            case DI_MYSQL, DI_SQLITE:
                sel := NewSelectRoot(ef.Func(getLastId))
                sm := NewSelectMaker()
                err := sm.BuildSql(sel, this.Format)
                if err != nil {
                    return err
                }
                this.Batch.Add(sm.Batch.Last())
            case DI_MSTSQL:
                stat := this.Batch.Last()
                stat.Type = SS_QUERY
            }
        case SK_INSERT_FROM:
            sect := section.(*InsertSelect)
            err = sect.buildSelectSectionSql(this, this.Batch.Last(), stack)
        default:
            err = e("Unexpected section during generating "+
                "\"insert\" statement: %v", section)
        }
        return err
    }
    return nil
}

func (this *insertMaker) BuildSql(section Section,
    format *Format) error {
    this.Format = format
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    err := iterateSqlParents(false, section, this.runMaker)
    if err != nil {
        return err
    }
    err = this.Batch.Join(format)
    return err
}

func (this *insertMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack,
    format *Format) *ExprBuildContext {
    al := &QueryEntries{}
    al.AddEntry(this.TargetDataSource)
    context := NewExprBuildContext(sectionKind, subsectionKind, stack, format, al)
    return context
}

// TODO
// Implement verifications:
//  1) DID number of expressions in "values" section correspond to number of fields
//  2) fields should belong to table specified
// Implement "returning option" in case of Values construction

type InsertRoot struct {
    DataSource Query
    Fields     []*TokenField
}

func NewInsertRoot(query Query, fields ...*TokenField) *InsertRoot {
    root := &InsertRoot{DataSource: query, Fields: fields}
    return root
}

func (this *InsertRoot) getInsertFieldCount() (int, error) {
    if len(this.Fields) == 0 {
        return this.DataSource.GetColumnCount()
    } else {
        return len(this.Fields), nil
    }
}

func (this *InsertRoot) Values(first Expr, last ...Expr) *InsertValues {
    exprs := []Expr{first}
    exprs = append(exprs, last...)
    iv := &InsertValues{Root: this, Exprs: exprs}
    return iv
}

func (this *InsertRoot) From(sel SqlReady) *InsertSelect {
    is := &InsertSelect{Root: this, Select: sel}
    return is
}

func (this *InsertRoot) buildInsertSectionSql(maker *insertMaker,
    stat *Statement, stack *CallStack,
    returning *InsertReturning) error {
    tableBased, _ := this.DataSource.IsTableBased()
    if tableBased == false {
        objstr, err := formatPrettyDataSource(this.DataSource,
            false, &maker.Format.Dialect)
        if err != nil {
            return err
        }
        return e("Table expected instead of %s", objstr)
    }
    stat2, err := maker.Format.formatDataSourceRef(this.DataSource)
    if err != nil {
        return err
    }
    stat.appendStatPartsFormat("insert into %s", stat2)
    // build insert columns section if exists
    if len(this.Fields) > 0 {
        stat.writeString(" (")
        for i, field := range this.Fields {
            sql := maker.Format.formatObjectName(field.Name)
            stat.writeString(sql)
            if i < len(this.Fields)-1 {
                stat.writeString(", ")
            }
        }
        stat.writeString(")")
    }
    // insert returning section if necessary
    if returning != nil && maker.Format.Dialect == DI_MSTSQL {
        err := returning.buildReturningSectionSql(maker, stat, stack)
        if err != nil {
            return err
        }
    }
    return nil
}

func (this *InsertRoot) GetSectionKind() SectionKind {
    return SK_INSERT
}

func (this *InsertRoot) GetParent() Section {
    return nil
}

type InsertValues struct {
    // parent
    Root *InsertRoot
    // data
    Exprs []Expr
}

func (this *InsertValues) Returning(first Expr, rest ...Expr) *InsertReturning {
    exprs := []Expr{first}
    exprs = append(exprs, rest...)
    ir := &InsertReturning{Values: this, Exprs: exprs}
    return ir
}

func (this *InsertValues) buildValuesSectionSql(maker *insertMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.sectionDivider)
    stat.writeString("values (")
    context := maker.GetExprBuildContext(SK_INSERT_VALUES,
        SSK_EXPR1, stack, maker.Format)
    for i, expr := range this.Exprs {
        stat2, err := expr.GetSql(context)
        if err != nil {
            return err
        }
        stat.appendStatPart(stat2)
        if i < len(this.Exprs)-1 {
            stat.writeString(", ")
        }
    }
    stat.writeString(")")
    return nil
}

func (this *InsertValues) GetSql(format *Format) (*StatementBatch, error) {
    maker := &insertMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, nil
}

func (this *InsertValues) Validate(format *Format) error {
    _, err := this.GetSql(format)
    return err
}

func (this *InsertValues) GetSectionKind() SectionKind {
    return SK_INSERT_VALUES
}

func (this *InsertValues) GetParent() Section {
    return this.Root
}

type InsertReturning struct {
    // parent
    Values *InsertValues
    // data
    Exprs []Expr
}

func (this *InsertReturning) buildReturningSectionSql(maker *insertMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.sectionDivider)
    stat.writeString(maker.Format.getLeadingSpace())
    dialect := maker.Format.Dialect
    if dialect.In(DI_PGSQL | DI_MSTSQL) {
        switch dialect {
        case DI_PGSQL:
            stat.writeString("returning ")
        case DI_MSTSQL:
            stat.writeString("output ")
        }
        context := maker.GetExprBuildContext(
            SK_INSERT_RETURNING, SSK_EXPR1, stack, maker.Format)
        for i, expr := range this.Exprs {
            stat2, err := expr.GetSql(context)
            if err != nil {
                return err
            }
            stat.appendStatPart(stat2)
            if i < len(this.Exprs)-1 {
                stat.writeString(", ")
            }
        }
    }
    return nil
}

func (this *InsertReturning) GetSql(format *Format) (*StatementBatch, error) {
    maker := &insertMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, nil
}

func (this *InsertReturning) Validate(format *Format) error {
    _, err := this.GetSql(format)
    return err
}

func (this *InsertReturning) GetSectionKind() SectionKind {
    return SK_INSERT_RETURNING
}

func (this *InsertReturning) GetParent() Section {
    return this.Values
}

type InsertSelect struct {
    // parent
    Root *InsertRoot
    // data
    // TODO change SqlReady with Subquery???
    Select SqlReady
}

func (this *InsertSelect) buildSelectSectionSql(maker *insertMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.sectionDivider)
    batch, err := this.Select.GetSql(maker.Format)
    if err != nil {
        return err
    }
    if len(batch.Items) > 1 {
        return e("Can't process multiple statements for \"from\" section: v", batch)
    }
    stat.appendStatPart(batch.Items[0])
    return nil
}

func (this *InsertSelect) GetSql(format *Format) (*StatementBatch, error) {
    maker := &insertMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, nil
}

func (this *InsertSelect) Validate(format *Format) error {
    _, err := this.GetSql(format)
    return err
}

func (this *InsertSelect) GetSectionKind() SectionKind {
    return SK_INSERT_FROM
}

func (this *InsertSelect) GetParent() Section {
    return this.Root
}
