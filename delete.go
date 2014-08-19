package sqlg

// Generate sql statement according to gereal DELETE syntax:
//  delete from table1
//  where conditions

type deleteMaker struct {
    Format  *Format
    Batch   *StatementBatch
    Queries *QueryEntries
}

func (this *deleteMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack) *ExprBuildContext {
    context := NewExprBuildContext(sectionKind, subsectionKind,
        stack, this.Format, this.Queries)
    return context
}

func (this *deleteMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct {
        switch section.GetSectionKind() {
        case SK_DELETE:
            sect := section.(*DeleteRoot)
            this.Queries.AddEntry(sect.DataSource)
        }
    } else {
        var err error
        switch section.GetSectionKind() {
        case SK_DELETE:
            sect := section.(*DeleteRoot)
            err = sect.buildSql(this, this.Batch.Last(), stack)
        case SK_DELETE_WHERE:
            sect := section.(*DeleteWhere)
            err = sect.buildWhereSectionSql(this, this.Batch.Last(), stack)
        default:
            err = e("Unexpected section during generating "+
                "\"delete\" statement: %v", section)
        }
        return err
    }
    return nil
}

func (this *deleteMaker) BuildSql(section Section,
    format *Format) error {
    this.Format = format
    this.Queries = NewQueryEntries()
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    return iterateSqlParents(false, section, this.runMaker)
}

type DeleteRoot struct {
    DataSource Query
}

func NewDeleteRoot(query Query) *DeleteRoot {
    root := &DeleteRoot{DataSource: query}
    return root
}

func (this *DeleteRoot) Where(cond Expr) *DeleteWhere {
    uw := &DeleteWhere{Root: this, Cond: cond}
    return uw
}

func (this *DeleteRoot) buildSql(maker *deleteMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString("delete from ")
    tableBased, _ := this.DataSource.IsTableBased()
    if tableBased == false {
        objstr, err := formatPrettyDataSource(this.DataSource,
            false, &maker.Format.Dialect)
        if err != nil {
            return err
        }
        return e("Table expected instead of \"%s\"", objstr)
    }
    stat2, err := maker.Format.formatDataSourceRef(this.DataSource)
    if err != nil {
        return err
    }
    stat.appendStatPart(stat2)
    return nil
}

func (this *DeleteRoot) GetSectionKind() SectionKind {
    return SK_DELETE
}

func (this *DeleteRoot) GetParent() Section {
    return nil
}

type DeleteWhere struct {
    // parent
    Root *DeleteRoot
    // data
    Cond Expr
}

func (this *DeleteWhere) buildWhereSectionSql(maker *deleteMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.sectionDivider)
    stat.writeString("where ")
    context := maker.GetExprBuildContext(SK_DELETE_WHERE, SSK_EXPR1, stack)
    stat2, err := this.Cond.GetSql(context)
    if err != nil {
        return err
    }
    stat.appendStatPart(stat2)
    return nil
}

func (this *DeleteWhere) GetSql(format *Format) (*StatementBatch, error) {
    maker := &deleteMaker{}
    err := maker.BuildSql(this, format)
    return maker.Batch, err
}

func (this *DeleteWhere) Validate(format *Format) error {
    _, err := this.GetSql(format)
    return err
}

func (this *DeleteWhere) GetSectionKind() SectionKind {
    return SK_DELETE_WHERE
}

func (this *DeleteWhere) GetParent() Section {
    return this.Root
}
