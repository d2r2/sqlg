package sqlselect

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type OrderBy interface {
	sqlcore.SqlComplete
	sqlcore.SqlPart
}

type orderBy struct {
	// parent
	From    *from
	Where   *where
	GroupBy *groupBy
	// data
	Fields []sqlexp.Expr
}

func (this *orderBy) IsTableBased() (bool, sqlcore.Table) {
	return false, nil
}

func (this *orderBy) GetColumnCount() (int, error) {
	maker := &maker{}
	return maker.GetColumnCount(this)
}

func (this *orderBy) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &maker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *orderBy) ColumnExists(name string) (bool, error) {
	maker := &maker{}
	return maker.FindColumn(name, this)
}

func (this *orderBy) buildOrderBySectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString(maker.Format.GetLeadingSpace())
	stat.WriteString("order by ")
	context := maker.GetExprBuildContext(
		sqlcore.SPK_SELECT_ORDER_BY, sqlcore.SSPK_EXPR1, stack, maker.Format)
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

func (this *orderBy) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &maker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, err
}

func (this *orderBy) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *orderBy) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_SELECT_ORDER_BY
}

func (this *orderBy) GetParent() sqlcore.SqlPart {
	if this.GroupBy != nil {
		return this.GroupBy
	} else if this.Where != nil {
		return this.Where
	} else {
		return this.From
	}
}
