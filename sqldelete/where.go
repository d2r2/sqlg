package sqldelete

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type Where interface {
	sqlcore.SqlComplete
	sqlcore.SqlPart
}

type where struct {
	// parent
	Root *del
	// data
	Cond sqlexp.Expr
}

func (this *where) buildWhereSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString("where ")
	context := maker.GetExprBuildContext(sqlcore.SPK_DELETE_WHERE, sqlcore.SSPK_EXPR1, stack)
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
	return maker.Batch, err
}

func (this *where) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *where) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_DELETE_WHERE
}

func (this *where) GetParent() sqlcore.SqlPart {
	return this.Root
}
