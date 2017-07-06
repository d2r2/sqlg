package sqlupdate

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type Where interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
}

type where struct {
	// parent
	Root *update
	From *from
	// data
	Cond sqlexp.Expr
}

func (this *where) buildWhereSectionSql(maker *updateMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	stat.WriteString("where ")
	context := maker.GetExprBuildContext(
		sqlcore.SPK_UPDATE_WHERE, sqlcore.SSPK_EXPR1, stack, maker.Format)
	stat2, err := this.Cond.GetSql(context)
	if err != nil {
		return err
	}
	stat.AppendStatPart(stat2)
	return nil
}

func (this *where) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &updateMaker{}
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
	return sqlcore.SPK_UPDATE_WHERE
}

func (this *where) GetParent() sqlcore.SqlPart {
	if this.From != nil {
		return this.From
	} else {
		return this.Root
	}
}
