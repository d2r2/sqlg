package sqlg

// TODO what about Having clause
// Generate sql statement according to general SELECT syntax:
//      select expr1, expr2, ...
//      from table1
//      [(inner|left|right) join table2 on conditions]
//      [where conditions]
//      [group by expr1, expr2, ...]
//      [order by expr1, expr2, ...]

import (
	"bytes"
)

// Keep temporary information about specific table
// from "from" and "join" sections;
// this information used during sql statement
// validate and generate process.
type SelectAnalyzeDataSource struct {
	DataSource Query
	JoinFields []*TokenField
}

// Used temporary during sql statment validating and generating.
type selectMaker struct {
	// Contains tables and queries from sections "from" and "join";
	// tables added in reverse order.
	DataSources []*SelectAnalyzeDataSource
	// Index of visibility scope of sections "from" and joins.
	TableVisScopeIndex int
	Batch              *StatementBatch
	Format             *Format
}

func NewSelectMaker() *selectMaker {
	m := &selectMaker{}
	return m
}

// Support scope visibility in section [from, join... join]
// with help of variable TableVisScopeIndex
// since it let detect mistakes with referencing to tables
// that haven't been added yet.
func (this *selectMaker) GetDataSourceEntries() *QueryEntries {
	al := &QueryEntries{}
	for i, t := range this.DataSources {
		if i >= this.TableVisScopeIndex {
			al.AddEntry(t.DataSource)
		}
	}
	return al
}

func (this *selectMaker) GetExprBuildContext(sectionKind SectionKind,
	subsectionKind SubsectionKind, stack *CallStack,
	format *Format) *ExprBuildContext {
	al := this.GetDataSourceEntries()
	context := NewExprBuildContext(sectionKind, subsectionKind,
		stack, format, al)
	return context
}

func (this *selectMaker) ResetScopeVisIndex() {
	// aware that tables added to this list in reverse order
	this.TableVisScopeIndex = len(this.DataSources) - 1
}

func (this *selectMaker) IncScopeVisIndex() {
	// be aware that tables present in reverse order
	this.TableVisScopeIndex--
}

func (this *selectMaker) AddDataSource(query Query,
	joinFields []*TokenField) error {
	if query == nil {
		return e("Table or query specified in \"FROM\"/\"JOIN\" clauses is nil")
	}
	queryAlias, aliasBased := query.(QueryAlias)
	if aliasBased {
		alias := queryAlias.GetAlias()
		if queryAlias.GetSource() == nil {
			return e("Table or query with alias \"%s\" specified in \"FROM\"/\"JOIN\" "+
				"clauses is nil", alias)
		}
		queryByAlias := this.FindByAlias(alias)
		if queryByAlias != nil {
			objstr, err := formatPrettyDataSource(query, false, nil)
			if err != nil {
				return err
			}
			str := f("Can't add %s with alias \"%s\""+
				", because other object was added with this alias",
				objstr, alias)
			return e(str)
		}
	}
	t := &SelectAnalyzeDataSource{DataSource: query, JoinFields: joinFields}
	this.DataSources = append(this.DataSources, t)
	return nil
}

func (this *selectMaker) FindByAlias(alias string) *SelectAnalyzeDataSource {
	for _, entry := range this.DataSources {
		queryAlias, aliasBased := entry.DataSource.(QueryAlias)
		if aliasBased {
			if alias == queryAlias.GetAlias() {
				return entry
			}
		}
	}
	return nil
}

// TODO remove code below, since all validation should be done in Expr.GetSql
/*
func (this *selectMaker) FindTable(field *TokenField) *SelectAnalyzeTable {
    for _, entry := range m.Tables {
        tablebased, tableName := field.Table.IsTableBased()
        entryTablebased, entryTableName := entry.Table.IsTableBased()
        if tablebased && entryTablebased {
            if tableName == entryTableName {
                return entry
            }
        } else if field.Table == entry.Table {
            return entry
        }
    }
    return nil
}
*/
func (this *selectMaker) PrintTables() {
	var buf bytes.Buffer
	for i, item := range this.DataSources {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("[")
		buf.WriteString(f("DataSource: %v, ", item.DataSource))
		buf.WriteString(f("JoinFields: %v", item.JoinFields))
		buf.WriteString("]")
	}
	log.Debug(buf.String())
}

func (this *selectMaker) runMaker(direct bool,
	section Section, stack *CallStack) error {
	if direct {
		switch section.GetSectionKind() {
		case SK_SELECT:
			// TODO remove code below, since all validation should be done in Expr.GetSql
			//          sect := section.(*SelectRoot)
			//            err := sect.checkFieldsBelongToAddedTables(this)
			//            return err
		case SK_SELECT_FROM_OR_JOIN:
			sect := section.(*SelectFrom)
			var fields []*TokenField
			if sect.JoinCond != nil {
				fields = sect.JoinCond.CollectFields()
			}
			err := this.AddDataSource(sect.DataSource, fields)
			return err
			//case SST_SELECT_WHERE:
			//case SST_SELECT_GROUP_BY:
			//case SST_SELECT_ORDER_BY:
		}
	} else {
		var err error
		switch section.GetSectionKind() {
		case SK_SELECT:
			sect := section.(*SelectRoot)
			err = sect.buildSelectSectionSql(this, this.Batch.Last(), stack)
		case SK_SELECT_FROM_OR_JOIN:
			sect := section.(*SelectFrom)
			err = sect.buildFromSectionSql(this, this.Batch.Last(), stack)
		case SK_SELECT_WHERE:
			sect := section.(*SelectWhere)
			err = sect.buildWhereSectionSql(this, this.Batch.Last(), stack)
		case SK_SELECT_GROUP_BY:
			sect := section.(*SelectGroupBy)
			err = sect.buildGroupBySectionSql(this, this.Batch.Last(), stack)
		case SK_SELECT_ORDER_BY:
			sect := section.(*SelectOrderBy)
			err = sect.buildOrderBySectionSql(this, this.Batch.Last(), stack)
		default:
			err = e("Unexpected section during generating "+
				"\"select\" statement: %v", section)
		}
		return err
	}
	return nil
}

func (this *selectMaker) ColumnIsAmbiguous(name string, sel Section) (bool, error) {
	err := iterateSqlParents(true, sel, this.runMaker)
	if err != nil {
		return false, err
	}
	r := getSectionRoot(sel)
	root := r.(*SelectRoot)
	var found, ambiguous bool
	if len(root.SelExprs) > 0 {
		for _, expr := range root.SelExprs {
			column, ok := expr.(ExprNamed)
			if ok && column.GetFieldAliasOrName() == name {
				if found == false {
					found = true
				} else {
					ambiguous = true
					break
				}
			}
		}
	} else {
		for _, entry := range this.DataSources {
			ok, err := entry.DataSource.ColumnExists(name)
			if err != nil {
				return false, err
			}
			if ok {
				if found == false {
					found = true
				} else {
					ambiguous = true
					break
				}
			}
		}
	}
	return ambiguous, nil
}

func (this *selectMaker) FindColumn(name string, sel Section) (bool, error) {
	err := iterateSqlParents(true, sel, this.runMaker)
	if err != nil {
		return false, err
	}
	r := getSectionRoot(sel)
	root := r.(*SelectRoot)
	if len(root.SelExprs) > 0 {
		for _, expr := range root.SelExprs {
			column, ok := expr.(ExprNamed)
			if ok && column.GetFieldAliasOrName() == name {
				return true, nil
			}
		}
	} else {
		for _, entry := range this.DataSources {
			found, err := entry.DataSource.ColumnExists(name)
			if err != nil {
				return false, err
			}
			if found {
				return true, nil
			}
		}
	}
	return false, nil
}

func (this *selectMaker) GetColumnCount(sel Section) (int, error) {
	err := iterateSqlParents(true, sel, this.runMaker)
	if err != nil {
		return 0, err
	}
	r := getSectionRoot(sel)
	root := r.(*SelectRoot)
	if len(root.SelExprs) == 0 {
		count := 0
		for _, entry := range this.DataSources {
			c, err := entry.DataSource.GetColumnCount()
			if err != nil {
				return 0, err
			}
			count += c
		}
		return count, nil
	} else {
		return len(root.SelExprs), nil
	}
}

func (this *selectMaker) BuildSql(section Section,
	format *Format) error {
	this.Format = format
	this.Batch = NewStatementBatch()
	this.Batch.Add(NewStatement(SS_QUERY))
	return iterateSqlParents(false, section, this.runMaker)
}

func (this *selectMaker) Analyze(section Section) error {
	return iterateSqlParents(true, section, this.runMaker)
}

type SelectRoot struct {
	// expressions from section: select <expr1, expr2, ...> from
	SelExprs []Expr
}

func NewSelectRoot(selExprs ...Expr) *SelectRoot {
	s := &SelectRoot{SelExprs: selExprs}
	return s
}

func (this *SelectRoot) From(query Query) *SelectFrom {
	sf := &SelectFrom{Root: this, DataSource: query}
	return sf
}

func (this *SelectRoot) buildFieldsSectionSql(maker *selectMaker,
	stat *Statement, stack *CallStack) error {
	if len(this.SelExprs) == 0 {
		// case when no fields specify
		// so add all fields from all tables included
		for i := len(maker.DataSources) - 1; i >= 0; i-- {
			entry := maker.DataSources[i]
			query := entry.DataSource
			queryAlias, aliasBased := query.(QueryAlias)
			tableBased, table := query.IsTableBased()
			if aliasBased {
				stat.writeString("%s.*", queryAlias.GetAlias())
			} else if tableBased {
				stat.writeString("%s.*", maker.Format.formatTableName(
					table.Name /*, table.Db.Name*/))
			} else {
				return e("Can't point to the object, since no name, neither alias specified")
			}
			if i > 0 {
				stat.writeString(", ")
			}
		}
	} else {
		context := maker.GetExprBuildContext(
			SK_SELECT, SSK_EXPR1, stack, maker.Format)
		for i, expr := range this.SelExprs {
			stat2, err := expr.GetSql(context)
			if err != nil {
				return err
			}
			stat.appendStatPart(stat2)
			if i < len(this.SelExprs)-1 {
				stat.writeString(", ")
			}
		}
	}
	return nil
}

func (this *SelectRoot) buildSelectSectionSql(maker *selectMaker,
	stat *Statement, stack *CallStack) error {
	stat.writeString(maker.Format.getLeadingSpace())
	stat.writeString("select ")
	// generate fields section sql
	err := this.buildFieldsSectionSql(maker, stat, stack)
	if err != nil {
		return err
	}
	return nil
}

// TODO remove code below, since all validation should be done in Expr.GetSql
/*func (this *SelectRoot) checkFieldsBelongToAddedTables(maker *selectMaker) error {
    for _, expr := range sel.SelExprs {
        fields := expr.CollectFields()
        for _, field := range fields {
            ta := maker.FindTable(field)
            if ta == nil {
                t := field.Table
                objstr, err := formatPrettySubquery(t, true, nil)
                if err != nil {
                    return err
                }
                return e("%s from field \"%s\" haven't been added to the statement",
                    objstr, field.Name)
            }
        }
    }
    return nil
}
*/
func (this *SelectRoot) GetSectionKind() SectionKind {
	return SK_SELECT
}

func (this *SelectRoot) GetParent() Section {
	return nil
}

type SelectFrom struct {
	// parent
	Root *SelectRoot
	From *SelectFrom
	// data
	DataSource Query
	JoinKind   JoinKind
	JoinCond   Expr
}

func (this *SelectFrom) InnerJoin(query Query, joinCond Expr) *SelectFrom {
	sf := &SelectFrom{From: this, DataSource: query,
		JoinKind: JK_INNER, JoinCond: joinCond}
	return sf
}

func (this *SelectFrom) LeftJoin(query Query, joinCond Expr) *SelectFrom {
	sf := &SelectFrom{From: this, DataSource: query,
		JoinKind: JK_LEFT, JoinCond: joinCond}
	return sf
}

func (this *SelectFrom) RightJoin(query Query, joinCond Expr) *SelectFrom {
	sf := &SelectFrom{From: this, DataSource: query,
		JoinKind: JK_RIGHT, JoinCond: joinCond}
	return sf
}

func (this *SelectFrom) Where(cond Expr) *SelectWhere {
	sw := &SelectWhere{From: this, Cond: cond}
	return sw
}

func (this *SelectFrom) OrderBy(firstExpr Expr, restExprs ...Expr) *SelectOrderBy {
	fields := []Expr{firstExpr}
	fields = append(fields, restExprs...)
	so := &SelectOrderBy{From: this, Fields: fields}
	return so
}

func (this *SelectFrom) IsTableBased() (bool, *TableDef) {
	return false, nil
}

func (this *SelectFrom) GetColumnCount() (int, error) {
	maker := &selectMaker{}
	return maker.GetColumnCount(this)
}

func (this *SelectFrom) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *SelectFrom) ColumnExists(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.FindColumn(name, this)
}

func (this *SelectFrom) buildFromSectionSql(maker *selectMaker,
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
			SK_SELECT_FROM_OR_JOIN, SSK_EXPR1, stack, maker.Format)
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

func (this *SelectFrom) GetSql(format *Format) (*StatementBatch, error) {
	maker := &selectMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *SelectFrom) Validate(format *Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *SelectFrom) GetSectionKind() SectionKind {
	return SK_SELECT_FROM_OR_JOIN
}

func (this *SelectFrom) GetParent() Section {
	if this.From != nil {
		return this.From
	} else {
		return this.Root
	}
}

type SelectWhere struct {
	// parent
	From *SelectFrom
	// data
	Cond Expr
}

func (this *SelectWhere) OrderBy(firstExpr Expr, restExprs ...Expr) *SelectOrderBy {
	fields := []Expr{firstExpr}
	fields = append(fields, restExprs...)
	so := &SelectOrderBy{Where: this, Fields: fields}
	return so
}

func (this *SelectWhere) GroupBy(firstExpr Expr, restExprs ...Expr) *SelectGroupBy {
	fields := []Expr{firstExpr}
	fields = append(fields, restExprs...)
	sg := &SelectGroupBy{Where: this, Fields: fields}
	return sg
}

func (this *SelectWhere) IsTableBased() (bool, *TableDef) {
	return false, nil
}

func (this *SelectWhere) GetColumnCount() (int, error) {
	maker := &selectMaker{}
	return maker.GetColumnCount(this)
}

func (this *SelectWhere) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *SelectWhere) ColumnExists(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.FindColumn(name, this)
}

func (this *SelectWhere) buildWhereSectionSql(maker *selectMaker,
	stat *Statement, stack *CallStack) error {
	stat.writeString(maker.Format.sectionDivider)
	stat.writeString(maker.Format.getLeadingSpace())
	stat.writeString("where ")
	context := maker.GetExprBuildContext(
		SK_SELECT_WHERE, SSK_EXPR1, stack, maker.Format)
	stat2, err := this.Cond.GetSql(context)
	if err != nil {
		return err
	}
	stat.appendStatPart(stat2)
	return nil
}

func (this *SelectWhere) GetSql(format *Format) (*StatementBatch, error) {
	maker := &selectMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *SelectWhere) Validate(format *Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *SelectWhere) GetSectionKind() SectionKind {
	return SK_SELECT_WHERE
}

func (this *SelectWhere) GetParent() Section {
	return this.From
}

type SelectGroupBy struct {
	// parent
	From  *SelectFrom
	Where *SelectWhere
	// data
	Fields []Expr
}

func (this *SelectGroupBy) OrderBy(first Expr, rest ...Expr) *SelectOrderBy {
	fields := []Expr{first}
	fields = append(fields, rest...)
	so := &SelectOrderBy{GroupBy: this, Fields: fields}
	return so
}

func (this *SelectGroupBy) IsTableBased() (bool, *TableDef) {
	return false, nil
}

func (this *SelectGroupBy) GetColumnCount() (int, error) {
	maker := &selectMaker{}
	return maker.GetColumnCount(this)
}

func (this *SelectGroupBy) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *SelectGroupBy) ColumnExists(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.FindColumn(name, this)
}

func (this *SelectGroupBy) buildGroupBySectionSql(maker *selectMaker,
	stat *Statement, stack *CallStack) error {
	stat.writeString(maker.Format.sectionDivider)
	stat.writeString(maker.Format.getLeadingSpace())
	stat.writeString("group by ")
	context := maker.GetExprBuildContext(
		SK_SELECT_GROUP_BY, SSK_EXPR1, stack, maker.Format)
	for i, expr := range this.Fields {
		stat2, err := expr.GetSql(context)
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

func (this *SelectGroupBy) GetSql(format *Format) (*StatementBatch, error) {
	maker := &selectMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *SelectGroupBy) Validate(format *Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *SelectGroupBy) GetSectionKind() SectionKind {
	return SK_SELECT_GROUP_BY
}

func (this *SelectGroupBy) GetParent() Section {
	if this.Where != nil {
		return this.Where
	} else {
		return this.From
	}
}

type SelectOrderBy struct {
	// parent
	From    *SelectFrom
	Where   *SelectWhere
	GroupBy *SelectGroupBy
	// data
	Fields []Expr
}

func (this *SelectOrderBy) IsTableBased() (bool, *TableDef) {
	return false, nil
}

func (this *SelectOrderBy) GetColumnCount() (int, error) {
	maker := &selectMaker{}
	return maker.GetColumnCount(this)
}

func (this *SelectOrderBy) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *SelectOrderBy) ColumnExists(name string) (bool, error) {
	maker := &selectMaker{}
	return maker.FindColumn(name, this)
}

func (this *SelectOrderBy) buildOrderBySectionSql(maker *selectMaker,
	stat *Statement, stack *CallStack) error {
	stat.writeString(maker.Format.sectionDivider)
	stat.writeString(maker.Format.getLeadingSpace())
	stat.writeString("order by ")
	context := maker.GetExprBuildContext(
		SK_SELECT_ORDER_BY, SSK_EXPR1, stack, maker.Format)
	for i, expr := range this.Fields {
		stat2, err := expr.GetSql(context)
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

func (this *SelectOrderBy) GetSql(format *Format) (*StatementBatch, error) {
	maker := &selectMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, err
}

func (this *SelectOrderBy) Validate(format *Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *SelectOrderBy) GetSectionKind() SectionKind {
	return SK_SELECT_ORDER_BY
}

func (this *SelectOrderBy) GetParent() Section {
	if this.GroupBy != nil {
		return this.GroupBy
	} else if this.Where != nil {
		return this.Where
	} else {
		return this.From
	}
}
