package sqlselect

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type From interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
	InnerJoin(query sqlcore.Query, joinCond sqlexp.Expr) From
	LeftJoin(query sqlcore.Query, joinCond sqlexp.Expr) From
	RightJoin(query sqlcore.Query, joinCond sqlexp.Expr) From
	Where(cond sqlexp.Expr) Where
	OrderBy(firstExpr sqlexp.Expr, restExprs ...sqlexp.Expr) OrderBy
}

type from struct {
	// parent select
	Root *sel
	// parent from or join
	From *from
	// data
	DataSource sqlcore.Query
	JoinKind   sqlcore.JoinKind
	JoinCond   sqlexp.Expr
}

func (this *from) InnerJoin(query sqlcore.Query, joinCond sqlexp.Expr) From {
	sf := &from{From: this, DataSource: query,
		JoinKind: sqlcore.JK_INNER, JoinCond: joinCond}
	return sf
}

func (this *from) LeftJoin(query sqlcore.Query, joinCond sqlexp.Expr) From {
	sf := &from{From: this, DataSource: query,
		JoinKind: sqlcore.JK_LEFT, JoinCond: joinCond}
	return sf
}

func (this *from) RightJoin(query sqlcore.Query, joinCond sqlexp.Expr) From {
	sf := &from{From: this, DataSource: query,
		JoinKind: sqlcore.JK_RIGHT, JoinCond: joinCond}
	return sf
}

func (this *from) Where(cond sqlexp.Expr) Where {
	sw := &where{From: this, Cond: cond}
	return sw
}

func (this *from) OrderBy(firstExpr sqlexp.Expr, restExprs ...sqlexp.Expr) OrderBy {
	fields := []sqlexp.Expr{firstExpr}
	fields = append(fields, restExprs...)
	so := &orderBy{From: this, Fields: fields}
	return so
}

func (this *from) IsTableBased() (bool, sqlcore.Table) {
	return false, nil
}

func (this *from) GetColumnCount() (int, error) {
	maker := &maker{}
	return maker.GetColumnCount(this)
}

func (this *from) ColumnIsAmbiguous(name string) (bool, error) {
	maker := &maker{}
	return maker.ColumnIsAmbiguous(name, this)
}

func (this *from) ColumnExists(name string) (bool, error) {
	maker := &maker{}
	return maker.FindColumn(name, this)
}

func (this *from) buildFromSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	if this.JoinCond == nil {
		stat2, err := maker.Format.FormatDataSourceRef(this.DataSource)
		if err != nil {
			return err
		}
		stat.WriteString(maker.Format.SectionDivider)
		stat.WriteString(maker.Format.GetLeadingSpace())
		stat.AppendStatPartsFormat("from %s", stat2)
		// initial set index of scope visibility to first table entry
		// since it was added in reverse - last one
		maker.ResetScopeVisIndex()
	} else {
		jk := map[sqlcore.JoinKind]string{
			sqlcore.JK_INNER: "inner",
			sqlcore.JK_LEFT:  "left",
			sqlcore.JK_RIGHT: "right"}
		maker.IncScopeVisIndex()
		context := maker.GetExprBuildContext(
			sqlcore.SPK_SELECT_FROM_OR_JOIN, sqlcore.SSPK_EXPR1, stack, maker.Format)
		stat2, err := maker.Format.FormatDataSourceRef(this.DataSource)
		if err != nil {
			return err
		}
		stat.WriteString(maker.Format.SectionDivider)
		stat.WriteString(maker.Format.GetLeadingSpace())
		stat.WriteString(f("%s join ", jk[this.JoinKind]))
		stat.AppendStatPartsFormat("%s on ", stat2)
		stat3, err := this.JoinCond.GetSql(context)
		if err != nil {
			return err
		}
		stat.AppendStatPart(stat3)
	}
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
	return sqlcore.SPK_SELECT_FROM_OR_JOIN
}

func (this *from) GetParent() sqlcore.SqlPart {
	if this.From != nil {
		return this.From
	} else {
		return this.Root
	}
}
