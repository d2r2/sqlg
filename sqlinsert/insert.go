package sqlinsert

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

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldef"
	"github.com/d2r2/sqlg/sqlexp"
)

// TODO
// Implement verifications:
//  1) DID number of expressions in "values" section correspond to number of fields
//  2) fields should belong to table specified
// Implement "returning option" in case of Values construction

type Insert interface {
	sqlcore.SqlPart
	Values(first sqlexp.Expr, last ...sqlexp.Expr) Values
	From(sel sqlcore.SqlReady) From
}

type ins struct {
	DataSource sqlcore.Query
	Fields     []*sqlexp.TokenField
}

func NewInsert(query sqlcore.Query, fields ...*sqlexp.TokenField) Insert {
	root := &ins{DataSource: query, Fields: fields}
	return root
}

func (this *ins) getInsertFieldCount() (int, error) {
	if len(this.Fields) == 0 {
		return this.DataSource.GetColumnCount()
	} else {
		return len(this.Fields), nil
	}
}

func (this *ins) Values(first sqlexp.Expr, last ...sqlexp.Expr) Values {
	exprs := []sqlexp.Expr{first}
	exprs = append(exprs, last...)
	iv := &values{Root: this, Exprs: exprs}
	return iv
}

func (this *ins) From(sel sqlcore.SqlReady) From {
	is := &from{Root: this, From: sel}
	return is
}

func (this *ins) buildInsertSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack,
	returning *returning) error {
	tableBased, _ := this.DataSource.IsTableBased()
	if tableBased == false {
		objstr, err := sqlexp.FormatPrettyDataSource(this.DataSource,
			false, &maker.Format.Dialect)
		if err != nil {
			return err
		}
		return e("Table expected instead of %s", objstr)
	}
	stat2, err := maker.Format.FormatDataSourceRef(this.DataSource)
	if err != nil {
		return err
	}
	stat.AppendStatPartsFormat("insert into %s", stat2)
	// build insert columns section if exists
	if len(this.Fields) > 0 {
		stat.WriteString(" (")
		for i, field := range this.Fields {
			sql := maker.Format.FormatObjectName(field.Name)
			stat.WriteString(sql)
			if i < len(this.Fields)-1 {
				stat.WriteString(", ")
			}
		}
		stat.WriteString(")")
	}
	// insert returning section if necessary
	if returning != nil && maker.Format.Dialect == sqldef.DI_MSTSQL {
		err := returning.buildReturningSectionSql(maker, stat, stack)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *ins) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_INSERT
}

func (this *ins) GetParent() sqlcore.SqlPart {
	return nil
}
