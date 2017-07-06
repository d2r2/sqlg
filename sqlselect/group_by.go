package sqlselect

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type GroupBy interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
	OrderBy(first sqlexp.Expr, rest ...sqlexp.Expr) OrderBy
}

type groupBy struct {
	// parent
	From  *from
	Where *where
	// data
	Fields []sqlexp.Expr
}

func (this *groupBy) OrderBy(first sqlexp.Expr, rest ...sqlexp.Expr) OrderBy {
	fields := []sqlexp.Expr{first}
	fields = append(fields, rest...)
	so := &orderBy{GroupBy: this, Fields: fields}
	return so
}

func (this *groupBy) IsTableBased() (bool, sqlcore.Table) {
	return false, nil
}

func (this *groupBy) GetColumnCount() (int, error) {
	maker := &maker{}
	return maker.GetColumnCount(this)
}

func (this *groupBy) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &maker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *groupBy) ColumnExists(name string) (bool, error) {
	maker := &maker{}
	return maker.FindColumn(name, this)
}

func (this *groupBy) buildGroupBySectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString(maker.Format.GetLeadingSpace())
	stat.WriteString("group by ")
	context := maker.GetExprBuildContext(
		sqlcore.SPK_SELECT_GROUP_BY, sqlcore.SSPK_EXPR1, stack, maker.Format)
	for i, expr := range this.Fields {
		stat2, err := expr.GetSql(context)
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

func (this *groupBy) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &maker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *groupBy) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *groupBy) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_SELECT_GROUP_BY
}

func (this *groupBy) GetParent() sqlcore.SqlPart {
	if this.Where != nil {
		return this.Where
	} else {
		return this.From
	}
}
