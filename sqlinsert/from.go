package sqlinsert

import (
	"github.com/d2r2/sqlg/sqlcore"
)

type From interface {
	sqlcore.SqlComplete
	sqlcore.SqlPart
}

type from struct {
	// parent
	Root *ins
	// data
	// TODO change SqlComplete with Subquery???
	From sqlcore.SqlComplete
}

func (this *from) buildSelectSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.SectionDivider)
	batch, err := this.From.GetSql(maker.Format)
	if err != nil {
		return err
	}
	if len(batch.Items) > 1 {
		return e("Can't process multiple statements for \"from\" section: v", batch)
	}
	stat.AppendStatPart(batch.Items[0])
	return nil
}

func (this *from) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &maker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *from) Validate(format *sqlcore.Format) error {
	_, err := this.GetSql(format)
	return err
}

func (this *from) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_INSERT_FROM
}

func (this *from) GetParent() sqlcore.SqlPart {
	return this.Root
}
