package sqlinsert

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type Values interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
	Returning(first sqlexp.Expr, rest ...sqlexp.Expr) *returning
}

type values struct {
	// parent
	Root *ins
	// data
	Exprs []sqlexp.Expr
}

func (this *values) Returning(first sqlexp.Expr, rest ...sqlexp.Expr) *returning {
	exprs := []sqlexp.Expr{first}
	exprs = append(exprs, rest...)
	ir := &returning{Values: this, Exprs: exprs}
	return ir
}

func (this *values) buildValuesSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString("values (")
	context := maker.GetExprBuildContext(sqlcore.SPK_INSERT_VALUES,
		sqlcore.SSPK_EXPR1, stack, maker.Format)
	for i, expr := range this.Exprs {
		stat2, err := expr.GetSql(context)
		if err != nil {
			return err
		}
		stat.AppendStatPart(stat2)
		if i < len(this.Exprs)-1 {
			stat.WriteString(", ")
		}
	}
	stat.WriteString(")")
	return nil
}

func (this *values) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &maker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *values) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *values) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_INSERT_VALUES
}

func (this *values) GetParent() sqlcore.SqlPart {
	return this.Root
}
