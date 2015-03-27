package sqldelete

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

// Generate sql statement according to gereal DELETE syntax:
//  delete from table1
//  where conditions

type maker struct {
	Format  *sqlcore.Format
	Batch   *sqlcore.StatementBatch
	Queries *sqlexp.QueryEntries
}

func (this *maker) GetExprBuildContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack) *sqlexp.ExprBuildContext {
	context := sqlexp.NewExprBuildContext(partKind, subPartKind,
		stack, this.Format, this.Queries)
	return context
}

func (this *maker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct {
		switch part.GetPartKind() {
		case sqlcore.SPK_DELETE:
			sect := part.(*del)
			this.Queries.AddEntry(sect.DataSource)
		}
	} else {
		var err error
		switch part.GetPartKind() {
		case sqlcore.SPK_DELETE:
			sect := part.(*del)
			err = sect.buildSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_DELETE_WHERE:
			sect := part.(*where)
			err = sect.buildWhereSectionSql(this, this.Batch.Last(), stack)
		default:
			err = e("Unexpected section during generating "+
				"\"delete\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *maker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Queries = sqlexp.NewQueryEntries()
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

type Delete interface {
	sqlcore.SqlPart
	Where(cond sqlexp.Expr) Where
}

type del struct {
	DataSource sqlcore.Query
}

func NewDelete(query sqlcore.Query) Delete {
	root := &del{DataSource: query}
	return root
}

func (this *del) Where(cond sqlexp.Expr) Where {
	uw := &where{Root: this, Cond: cond}
	return uw
}

func (this *del) buildSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString("delete from ")
	tableBased, _ := this.DataSource.IsTableBased()
	if tableBased == false {
		objstr, err := sqlexp.FormatPrettyDataSource(this.DataSource,
			false, &maker.Format.Dialect)
		if err != nil {
			return err
		}
		return e("Table expected instead of \"%s\"", objstr)
	}
	stat2, err := maker.Format.FormatDataSourceRef(this.DataSource)
	if err != nil {
		return err
	}
	stat.AppendStatPart(stat2)
	return nil
}

func (this *del) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_DELETE
}

func (this *del) GetParent() sqlcore.SqlPart {
	return nil
}
