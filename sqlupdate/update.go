package sqlupdate

// Generate sql statement according to the syntax:
//      update table1
//      set column1 = expr1, column2 = expr2, ...
//      [from table2]
//      [(inner|left|right) join table3 on conditions]
//      [where conditions]

// keep temporary information about specific table
// from "from" and "join" sections;
// this information used during sql script
// validate and generate process

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type UpdateAnalyzeDataSource struct {
	DataSource sqlcore.Query
	JoinFields []*sqlexp.TokenField
}

// used temporary during sql script validating and generating
type updateMaker struct {
	DataSources        []*UpdateAnalyzeDataSource
	TableVisScopeIndex int
	Format             *sqlcore.Format
	Batch              *sqlcore.StatementBatch
}

// support scope visibility in section [from, join... join]
// with help of variable TableVisScopeIndex
// since it let detect mistakes with referencing to tables
// that haven't been added yet
func (this *updateMaker) GetDataSourceEntries() *sqlexp.QueryEntries {
	al := &sqlexp.QueryEntries{}
	for i, t := range this.DataSources {
		if i >= this.TableVisScopeIndex {
			al.AddEntry(t.DataSource)
		}
	}
	return al
}

func (this *updateMaker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct {
		switch part.GetPartKind() {
		case sqlcore.SPK_UPDATE:
			sect := part.(*update)
			err := this.AddDataSource(sect.TargetDataSource, nil)
			return err
		case sqlcore.SPK_UPDATE_FROM_OR_JOIN:
			sect := part.(*from)
			var fields []*sqlexp.TokenField
			if sect.JoinCond != nil {
				fields = sect.JoinCond.CollectFields()
			}
			err := this.AddDataSource(sect.DataSource, fields)
			return err
			//        case SST_UPDATE_WHERE:
			//            sect := part.(*where)
		}
	} else {
		var err error
		switch part.GetPartKind() {
		case sqlcore.SPK_UPDATE:
			sect := part.(*update)
			err = sect.buildUpdateSectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_UPDATE_FROM_OR_JOIN:
			sect := part.(*from)
			err = sect.buildFromSectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_UPDATE_WHERE:
			sect := part.(*where)
			err = sect.buildWhereSectionSql(this, this.Batch.Last(), stack)
		default:
			err = e("Unexpected section during generating "+
				"\"insert\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *updateMaker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

func (this *updateMaker) GetExprBuildContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	al := this.GetDataSourceEntries()
	c := sqlexp.NewExprBuildContext(partKind, subPartKind, stack, format, al)
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

func (a *updateMaker) AddDataSource(query sqlcore.Query,
	joinFields []*sqlexp.TokenField) error {
	queryAlias, aliasBased := query.(sqlcore.QueryAlias)
	if aliasBased {
		alias := queryAlias.GetAlias()
		tableByAlias := a.FindByAlias(alias)
		if tableByAlias != nil {
			objstr, err := sqlexp.FormatPrettyDataSource(query, false, nil)
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
		queryAlias, aliasBased := entry.DataSource.(sqlcore.QueryAlias)
		if aliasBased {
			if alias == queryAlias.GetAlias() {
				return entry
			}
		}
	}
	return nil
}

func (a *updateMaker) FindDataSource(field *sqlexp.TokenField) *UpdateAnalyzeDataSource {
	for _, entry := range a.DataSources {
		tableBased, table := field.DataSource.IsTableBased()
		entryTableBased, entryTable := entry.DataSource.IsTableBased()
		if tableBased && entryTableBased {
			if table.GetName() == entryTable.GetName() {
				return entry
			}
		} else if field.DataSource == entry.DataSource {
			return entry
		}
	}
	return nil
}

type Update interface {
	sqlcore.SqlPart
	From(query sqlcore.Query) From
	Where(cond sqlexp.Expr) Where
}

type update struct {
	TargetDataSource sqlcore.Query
	Fields           []*sqlexp.TokenFieldAssign
}

func NewUpdate(query sqlcore.Query, fields ...*sqlexp.TokenFieldAssign) Update {
	root := &update{TargetDataSource: query, Fields: fields}
	return root
}

func (this *update) From(query sqlcore.Query) From {
	sf := &from{Root: this, DataSource: query}
	return sf
}

func (this *update) Where(cond sqlexp.Expr) Where {
	uw := &where{Root: this, Cond: cond}
	return uw
}

func (this *update) buildFieldsSectionSql(maker *updateMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	context := maker.GetExprBuildContext(sqlcore.SPK_UPDATE, sqlcore.SSPK_EXPR1,
		stack, maker.Format)
	stat.WriteString("set ")
	for i, field := range this.Fields {
		stat2, err := field.GetSql(context)
		if err != nil {
			return err
		}
		stat.AppendStatPart(stat2)
		if i < len(this.Fields)-1 {
			stat.WriteString(", ")
		}
	}
	return nil
}

func (this *update) buildUpdateSectionSql(maker *updateMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString("update ")
	tableBased, _ := this.TargetDataSource.IsTableBased()
	if tableBased == false {
		objstr, err := sqlexp.FormatPrettyDataSource(this.TargetDataSource,
			false, &maker.Format.Dialect)
		if err != nil {
			return err
		}
		return e("Table expected instead of %s", objstr)
	}
	stat2, err := maker.Format.FormatDataSourceRef(this.TargetDataSource)
	if err != nil {
		return err
	}
	stat.AppendStatPart(stat2)
	// generate fields section sql
	stat.WriteString(maker.Format.SectionDivider)
	err = this.buildFieldsSectionSql(maker, stat, stack)
	if err != nil {
		return err
	}
	return nil
}

func (this *update) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_UPDATE
}

func (this *update) GetParent() sqlcore.SqlPart {
	return nil
}
