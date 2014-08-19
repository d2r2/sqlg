package sqlg

// Generate sql statement according to general UPDATE syntax:
//      update table1
//      set column1 = expr1, column2 = expr2, ...
//      [from table2]
//      [(inner|left|right) join table3 on conditions]
//      [where conditions]

// keep temporary information about specific table
// from "from" and "join" sections;
// this information used during sql script
// validate and generate process
type UpdateAnalyzeDataSource struct {
    DataSource Query
    JoinFields []*TokenField
}

// used temporary during sql script validating and generating
type updateMaker struct {
    DataSources        []*UpdateAnalyzeDataSource
    TableVisScopeIndex int
    Format             *Format
    Batch              *StatementBatch
}

// support scope visibility in section [from, join... join]
// with help of variable TableVisScopeIndex
// since it let detect mistakes with referencing to tables
// that haven't been added yet
func (this *updateMaker) GetDataSourceEntries() *QueryEntries {
    al := &QueryEntries{}
    for i, t := range this.DataSources {
        if i >= this.TableVisScopeIndex {
            al.AddEntry(t.DataSource)
        }
    }
    return al
}

func (this *updateMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct {
        switch section.GetSectionKind() {
        case SK_UPDATE:
            sect := section.(*UpdateRoot)
            err := this.AddDataSource(sect.TargetDataSource, nil)
            return err
        case SK_UPDATE_FROM_OR_JOIN:
            sect := section.(*UpdateFrom)
            var fields []*TokenField
            if sect.JoinCond != nil {
                fields = sect.JoinCond.CollectFields()
            }
            err := this.AddDataSource(sect.DataSource, fields)
            return err
            //        case SST_UPDATE_WHERE:
            //            sect := section.(*UpdateWhere)
        }
    } else {
        var err error
        switch section.GetSectionKind() {
        case SK_UPDATE:
            sect := section.(*UpdateRoot)
            err = sect.buildUpdateSectionSql(this, this.Batch.Last(), stack)
        case SK_UPDATE_FROM_OR_JOIN:
            sect := section.(*UpdateFrom)
            err = sect.buildFromSectionSql(this, this.Batch.Last(), stack)
        case SK_UPDATE_WHERE:
            sect := section.(*UpdateWhere)
            err = sect.buildWhereSectionSql(this, this.Batch.Last(), stack)
        default:
            err = e("Unexpected section during generating "+
                "\"insert\" statement: %v", section)
        }
        return err
    }
    return nil
}

func (this *updateMaker) BuildSql(section Section,
    format *Format) error {
    this.Format = format
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    return iterateSqlParents(false, section, this.runMaker)
}

func (this *updateMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack,
    format *Format) *ExprBuildContext {
    al := this.GetDataSourceEntries()
    c := NewExprBuildContext(sectionKind, subsectionKind, stack, format, al)
    return c
}

func (this *updateMaker) ResetScopeVisIndex() {
    // be aware that tables present in reverse order
    this.TableVisScopeIndex = len(this.DataSources) - 1
}

func (this *updateMaker) IncScopeVisIndex() {
    // be aware that tables present in reverse order
    this.TableVisScopeIndex--
}

func (a *updateMaker) AddDataSource(query Query,
    joinFields []*TokenField) error {
    queryAlias, aliasBased := query.(QueryAlias)
    if aliasBased {
        alias := queryAlias.GetAlias()
        tableByAlias := a.FindByAlias(alias)
        if tableByAlias != nil {
            objstr, err := formatPrettyDataSource(query, false, nil)
            if err != nil {
                return err
            }
            str := f("Can't add %s with alias \"%s\""+
                ", since other object was added with this alias",
                objstr, alias)
            return e(str)
        }
    }
    t := &UpdateAnalyzeDataSource{DataSource: query, JoinFields: joinFields}
    a.DataSources = append(a.DataSources, t)
    return nil
}

func (a *updateMaker) FindByAlias(alias string) *UpdateAnalyzeDataSource {
    for _, entry := range a.DataSources {
        queryAlias, aliasBased := entry.DataSource.(QueryAlias)
        if aliasBased {
            if alias == queryAlias.GetAlias() {
                return entry
            }
        }
    }
    return nil
}

func (a *updateMaker) FindDataSource(field *TokenField) *UpdateAnalyzeDataSource {
    for _, entry := range a.DataSources {
        tableBased, table := field.DataSource.IsTableBased()
        entryTableBased, entryTable := entry.DataSource.IsTableBased()
        if tableBased && entryTableBased {
            if table.Name == entryTable.Name {
                return entry
            }
        } else if field.DataSource == entry.DataSource {
            return entry
        }
    }
    return nil
}

type UpdateRoot struct {
    TargetDataSource Query
    Fields           []*TokenFieldSet
}

func NewUpdateRoot(query Query, fields ...*TokenFieldSet) *UpdateRoot {
    root := &UpdateRoot{TargetDataSource: query, Fields: fields}
    return root
}

func (this *UpdateRoot) Where(cond Expr) *UpdateWhere {
    uw := &UpdateWhere{Root: this, Cond: cond}
    return uw
}

func (this *UpdateRoot) buildFieldsSectionSql(maker *updateMaker,
    stat *Statement, stack *CallStack) error {
    context := maker.GetExprBuildContext(SK_UPDATE, SSK_EXPR1,
        stack, maker.Format)
    stat.writeString("set ")
    for i, field := range this.Fields {
        stat2, err := field.GetSql(context)
        if err != nil {
            return err
        }
        stat.appendStatPart(stat2)
        if i < len(this.Fields)-1 {
            stat.writeString(", ")
        }
    }
    return nil
}

func (this *UpdateRoot) buildUpdateSectionSql(maker *updateMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString("update ")
    tableBased, _ := this.TargetDataSource.IsTableBased()
    if tableBased == false {
        objstr, err := formatPrettyDataSource(this.TargetDataSource,
            false, &maker.Format.Dialect)
        if err != nil {
            return err
        }
        return e("Table expected instead of %s", objstr)
    }
    stat2, err := maker.Format.formatDataSourceRef(this.TargetDataSource)
    if err != nil {
        return err
    }
    stat.appendStatPart(stat2)
    // generate fields section sql
    stat.writeString(maker.Format.sectionDivider)
    err = this.buildFieldsSectionSql(maker, stat, stack)
    if err != nil {
        return err
    }
    return nil
}

func (this *UpdateRoot) GetSectionKind() SectionKind {
    return SK_UPDATE
}

func (this *UpdateRoot) GetParent() Section {
    return nil
}

type UpdateFrom struct {
    // parent
    Root *UpdateRoot
    From *UpdateFrom
    // data
    DataSource Query
    JoinKind   JoinKind
    JoinCond   Expr
}

func (this *UpdateFrom) InnerJoin(query Query, joinCond Expr) *UpdateFrom {
    sf := &UpdateFrom{From: this, DataSource: query,
        JoinKind: JK_INNER, JoinCond: joinCond}
    return sf
}

func (this *UpdateFrom) LeftJoin(query Query, joinCond Expr) *UpdateFrom {
    sf := &UpdateFrom{From: this, DataSource: query,
        JoinKind: JK_LEFT, JoinCond: joinCond}
    return sf
}

func (this *UpdateFrom) RightJoin(query Query, joinCond Expr) *UpdateFrom {
    sf := &UpdateFrom{From: this, DataSource: query,
        JoinKind: JK_RIGHT, JoinCond: joinCond}
    return sf
}

func (this *UpdateFrom) Where(cond Expr) *UpdateWhere {
    sw := &UpdateWhere{From: this, Cond: cond}
    return sw
}

func (this *UpdateFrom) buildFromSectionSql(maker *updateMaker,
    stat *Statement, stack *CallStack) error {
    if this.JoinCond == nil {
        stat2, err := maker.Format.formatDataSourceRef(this.DataSource)
        if err != nil {
            return err
        }
        stat.writeString(maker.Format.sectionDivider)
        stat.writeString(maker.Format.getLeadingSpace())
        stat.appendStatPartsFormat("from %s", stat2)
        // initial set index of scope visibility to first table entry
        // since it was added in reverse - last one
        maker.ResetScopeVisIndex()
    } else {
        jk := map[JoinKind]string{
            JK_INNER: "inner",
            JK_LEFT:  "left",
            JK_RIGHT: "right"}
        maker.IncScopeVisIndex()
        context := maker.GetExprBuildContext(
            SK_UPDATE_FROM_OR_JOIN, SSK_EXPR1, stack, maker.Format)
        stat2, err := maker.Format.formatDataSourceRef(this.DataSource)
        if err != nil {
            return err
        }
        stat.writeString(maker.Format.sectionDivider)
        stat.writeString(maker.Format.getLeadingSpace())
        stat.writeString(f("%s join ", jk[this.JoinKind]))
        stat.appendStatPartsFormat("%s on ", stat2)
        stat3, err := this.JoinCond.GetSql(context)
        if err != nil {
            return err
        }
        stat.appendStatPart(stat3)
    }
    return nil
}

func (this *UpdateFrom) GetSectionKind() SectionKind {
    return SK_UPDATE_FROM_OR_JOIN
}

func (this *UpdateFrom) GetParent() Section {
    if this.From != nil {
        return this.From
    } else {
        return this.Root
    }
}

type UpdateWhere struct {
    // parent
    Root *UpdateRoot
    From *UpdateFrom
    // data
    Cond Expr
}

func (this *UpdateWhere) buildWhereSectionSql(maker *updateMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.sectionDivider)
    stat.writeString("where ")
    context := maker.GetExprBuildContext(
        SK_UPDATE_WHERE, SSK_EXPR1, stack, maker.Format)
    stat2, err := this.Cond.GetSql(context)
    if err != nil {
        return err
    }
    stat.appendStatPart(stat2)
    return nil
}

func (this *UpdateWhere) GetSql(format *Format) (*StatementBatch, error) {
    maker := &updateMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, nil
}

func (this *UpdateWhere) Validate(format *Format) error {
    _, err := this.GetSql(format)
    return err
}

func (this *UpdateWhere) GetSectionKind() SectionKind {
    return SK_UPDATE_WHERE
}

func (this *UpdateWhere) GetParent() Section {
    if this.From != nil {
        return this.From
    } else {
        return this.Root
    }
}
