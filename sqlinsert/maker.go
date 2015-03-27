package sqlinsert

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldef"
	"github.com/d2r2/sqlg/sqlexp"
	"github.com/d2r2/sqlg/sqlselect"
)

type maker struct {
	Format           *sqlcore.Format
	Batch            *sqlcore.StatementBatch
	TargetDataSource sqlcore.Query
	Returning        *returning
}

func (this *maker) CheckFieldAndExprCountMatch(sect *values) error {
	r := sqlcore.GetSqlPartRoot(sect)
	root := r.(*ins)
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

func (this *maker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct {
		switch part.GetPartKind() {
		case sqlcore.SPK_INSERT:
			sect := part.(*ins)
			this.TargetDataSource = sect.DataSource
		case sqlcore.SPK_INSERT_VALUES:
			sect := part.(*values)
			return this.CheckFieldAndExprCountMatch(sect)
		case sqlcore.SPK_INSERT_RETURNING:
			sect := part.(*returning)
			this.Returning = sect
		case sqlcore.SPK_INSERT_FROM:
			sect := part.(*from)
			query, queryBased := sect.From.(sqlcore.Query)
			if queryBased {
				r := sqlcore.GetSqlPartRoot(part)
				root := r.(*ins)
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
		switch part.GetPartKind() {
		case sqlcore.SPK_INSERT:
			sect := part.(*ins)
			err = sect.buildInsertSectionSql(this, this.Batch.Last(), stack, this.Returning)
		case sqlcore.SPK_INSERT_VALUES:
			sect := part.(*values)
			err = sect.buildValuesSectionSql(this, this.Batch.Last(), stack)
		case sqlcore.SPK_INSERT_RETURNING:
			sect := part.(*returning)
			ef := sqlexp.Factory()
			getLastId := ef.FuncDef(
				ef.FuncDialectDef(sqldef.DI_MYSQL, "last_insert_id()", 0, 0),
				ef.FuncDialectDef(sqldef.DI_SQLITE, "last_insert_rowid()", 0, 0))
			switch this.Format.Dialect {
			case sqldef.DI_PGSQL:
				stat := this.Batch.Last()
				err = sect.buildReturningSectionSql(this, stat, stack)
				stat.Type = sqlcore.SS_QUERY
			case sqldef.DI_MYSQL, sqldef.DI_SQLITE:
				s := sqlselect.NewSelect(ef.Func(getLastId))
				sm := sqlselect.NewMaker()
				err := sm.BuildSql(s, this.Format)
				if err != nil {
					return err
				}
				this.Batch.Add(sm.Batch.Last())
			case sqldef.DI_MSTSQL:
				stat := this.Batch.Last()
				stat.Type = sqlcore.SS_QUERY
			}
		case sqlcore.SPK_INSERT_FROM:
			sect := part.(*from)
			err = sect.buildSelectSectionSql(this, this.Batch.Last(), stack)
		default:
			err = e("Unexpected section during generating "+
				"\"insert\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *maker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	err := sqlcore.IterateSqlParents(false, part, this.runMaker)
	if err != nil {
		return err
	}
	err = this.Batch.Join(format)
	return err
}

func (this *maker) GetExprBuildContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	al := &sqlexp.QueryEntries{}
	al.AddEntry(this.TargetDataSource)
	context := sqlexp.NewExprBuildContext(partKind, subPartKind, stack, format, al)
	return context
}
