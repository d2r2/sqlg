package sqlselect

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type Where interface {
	sqlcore.SqlComplete
	sqlcore.SqlPart
	OrderBy(firstExpr sqlexp.Expr, restExprs ...sqlexp.Expr) OrderBy
	GroupBy(firstExpr sqlexp.Expr, restExprs ...sqlexp.Expr) GroupBy
}

type where struct {
	// parent
	From *from
	// data
	Cond sqlexp.Expr
}

func (this *where) OrderBy(firstExpr sqlexp.Expr, restExprs ...sqlexp.Expr) OrderBy {
	fields := []sqlexp.Expr{firstExpr}
	fields = append(fields, restExprs...)
	so := &orderBy{Where: this, Fields: fields}
	return so
}

func (this *where) GroupBy(firstExpr sqlexp.Expr, restExprs ...sqlexp.Expr) GroupBy {
	fields := []sqlexp.Expr{firstExpr}
	fields = append(fields, restExprs...)
	sg := &groupBy{Where: this, Fields: fields}
	return sg
}

func (this *where) IsTableBased() (bool, sqlcore.Table) {
	return false, nil
}

func (this *where) GetColumnCount() (int, error) {
	maker := &maker{}
	return maker.GetColumnCount(this)
}

func (this *where) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &maker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *where) ColumnExists(name string) (bool, error) {
	maker := &maker{}
	return maker.FindColumn(name, this)
}

func (this *where) buildWhereSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString(maker.Format.GetLeadingSpace())
	stat.WriteString("where ")
	context := maker.GetExprBuildContext(
		sqlcore.SPK_SELECT_WHERE, sqlcore.SSPK_EXPR1, stack, maker.Format)
	stat2, err := this.Cond.GetSql(context)
	if err != nil {
		return err
	}
	stat.AppendStatPart(stat2)
	return nil
}

func (this *where) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &maker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *where) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *where) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_SELECT_WHERE
}

func (this *where) GetParent() sqlcore.SqlPart {
	return this.From
}
