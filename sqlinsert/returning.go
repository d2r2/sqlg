package sqlinsert

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldef"
	"github.com/d2r2/sqlg/sqlexp"
)

type Returning interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
}

type returning struct {
	// parent
	Values *values
	// data
	Exprs []sqlexp.Expr
}

func (this *returning) buildReturningSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString(maker.Format.GetLeadingSpace())
	dialect := maker.Format.Dialect
	if dialect.In(sqldef.DI_PGSQL | sqldef.DI_MSTSQL) {
		switch dialect {
		case sqldef.DI_PGSQL:
			stat.WriteString("returning ")
		case sqldef.DI_MSTSQL:
			stat.WriteString("output ")
		}
		context := maker.GetExprBuildContext(
			sqlcore.SPK_INSERT_RETURNING, sqlcore.SSPK_EXPR1, stack, maker.Format)
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
	}
	return nil
}

func (this *returning) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &maker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *returning) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *returning) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_INSERT_RETURNING
}

func (this *returning) GetParent() sqlcore.SqlPart {
	return this.Values
}
