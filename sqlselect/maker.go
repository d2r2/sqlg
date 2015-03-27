package sqlselect

import (
	"bytes"

	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

// Keep temporary information about specific table
// from "from" and "join" sections;
// this information used during sql statement
// validate and generate process.
type SelectAnalyzeDataSource struct {
	DataSource sqlcore.Query
	JoinFields []*sqlexp.TokenField
}

// Used temporary during sql statment validating and generating.
type maker struct {
	// Contains tables and queries from sections "from" and "join";
	// tables added in reverse order.
	DataSources []*SelectAnalyzeDataSource
	// Index of visibility scope of sections "from" and joins.
	TableVisScopeIndex int
	Batch              *sqlcore.StatementBatch
	Format             *sqlcore.Format
}

func NewMaker() *maker {
	m := &maker{}
	return m
}

// Support scope visibility in section [from, join... join]
// with help of variable TableVisScopeIndex
// since it let detect mistakes with referencing to tables
// that haven't been added yet.
func (this *maker) GetDataSourceEntries() *sqlexp.QueryEntries {
	al := &sqlexp.QueryEntries{}
	for i, t := range this.DataSources {
		if i >= this.TableVisScopeIndex {
			al.AddEntry(t.DataSource)
		}
	}
	return al
}

func (this *maker) GetExprBuildContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	al := this.GetDataSourceEntries()
	context := sqlexp.NewExprBuildContext(partKind, subPartKind,
		stack, format, al)
	return context
}

func (this *maker) ResetScopeVisIndex() {
	// aware that tables added to this list in reverse order
	this.TableVisScopeIndex = len(this.DataSources) - 1
}

func (this *maker) IncScopeVisIndex() {
	// be aware that tables present in reverse order
	this.TableVisScopeIndex--
}

func (this *maker) AddDataSource(query sqlcore.Query,
	joinFields []*sqlexp.TokenField) error {
	if query == nil {
		return e("Table or query specified in \"FROM\"/\"JOIN\" clauses is nil")
	}
	queryAlias, aliasBased := query.(sqlcore.QueryAlias)
	if aliasBased {
		alias := queryAlias.GetAlias()
		if queryAlias.GetSource() == nil {
			return e("Table or query with alias \"%s\" specified in \"FROM\"/\"JOIN\" "+
				"clauses is nil", alias)
		}
		queryByAlias := this.FindByAlias(alias)
		if queryByAlias != nil {
			objstr, err := sqlexp.FormatPrettyDataSource(query, false, nil)
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

func (this *maker) FindByAlias(alias string) *SelectAnalyzeDataSource {
	for _, entry := range this.DataSources {
		queryAlias, aliasBased := entry.DataSource.(sqlcore.QueryAlias)
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
func (this *maker) FindTable(field *TokenField) *SelectAnalyzeTable {
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
func (this *maker) PrintTables() {
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

func (this *maker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct {
		switch part.GetPartKind() {
		case sqlcore.SPK_SELECT:
			// TODO remove code below, since all validation should be done in Expr.GetSql
			//          sect := part.(*SelectRoot)
			//            err := sect.checkFieldsBelongToAddedTables(this)
			//            return err
		case sqlcore.SPK_SELECT_FROM_OR_JOIN:
			sect := part.(*from)
			var fields []*sqlexp.TokenField
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
		switch part.GetPartKind() {
		case sqlcore.SPK_SELECT:
			sect := part.(*sel)
			err = sect.buildSelectSectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_SELECT_FROM_OR_JOIN:
			sect := part.(*from)
			err = sect.buildFromSectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_SELECT_WHERE:
			sect := part.(*where)
			err = sect.buildWhereSectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_SELECT_GROUP_BY:
			sect := part.(*groupBy)
			err = sect.buildGroupBySectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_SELECT_ORDER_BY:
			sect := part.(*orderBy)
			err = sect.buildOrderBySectionSql(this, this.Batch.Last(), stack)
		default:
			err = e("Unexpected section during generating "+
				"\"select\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *maker) ColumnIsAmbiguous(name string, part sqlcore.SqlPart) (bool, error) {
	err := sqlcore.IterateSqlParents(true, part, this.runMaker)
	if err != nil {
		return false, err
	}
	r := sqlcore.GetSqlPartRoot(part)
	root := r.(*sel)
	var found, ambiguous bool
	if len(root.SelExprs) > 0 {
		for _, expr := range root.SelExprs {
			column, ok := expr.(sqlexp.ExprNamed)
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

func (this *maker) FindColumn(name string, part sqlcore.SqlPart) (bool, error) {
	err := sqlcore.IterateSqlParents(true, part, this.runMaker)
	if err != nil {
		return false, err
	}
	r := sqlcore.GetSqlPartRoot(part)
	root := r.(*sel)
	if len(root.SelExprs) > 0 {
		for _, expr := range root.SelExprs {
			column, ok := expr.(sqlexp.ExprNamed)
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

func (this *maker) GetColumnCount(part sqlcore.SqlPart) (int, error) {
	err := sqlcore.IterateSqlParents(true, part, this.runMaker)
	if err != nil {
		return 0, err
	}
	r := sqlcore.GetSqlPartRoot(part)
	root := r.(*sel)
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

func (this *maker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_QUERY))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

func (this *maker) Analyze(part sqlcore.SqlPart) error {
	return sqlcore.IterateSqlParents(true, part, this.runMaker)
}
