package sqlselect

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlexp"
)

type Select interface {
	sqlcore.SqlPart
	From(query sqlcore.Query) From
}

type sel struct {
	// expressions from section: select <expr1, expr2, ...> from
	SelExprs []sqlexp.Expr
}

func NewSelect(selExprs ...sqlexp.Expr) Select {
	s := &sel{SelExprs: selExprs}
	return s
}

func (this *sel) From(query sqlcore.Query) From {
	sf := &from{Root: this, DataSource: query}
	return sf
}

func (this *sel) buildFieldsSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	if len(this.SelExprs) == 0 {
		// case when no fields specify
		// so add all fields from all tables included
		for i := len(maker.DataSources) - 1; i >= 0; i-- {
			entry := maker.DataSources[i]
			query := entry.DataSource
			queryAlias, aliasBased := query.(sqlcore.QueryAlias)
			tableBased, table := query.IsTableBased()
			if aliasBased {
				stat.WriteString("%s.*", queryAlias.GetAlias())
			} else if tableBased {
				stat.WriteString("%s.*", maker.Format.FormatTableName(
					table.GetName() /*, table.Db.Name*/))
			} else {
				return e("Can't point to the object, since no name, neither alias specified")
			}
			if i > 0 {
				stat.WriteString(", ")
			}
		}
	} else {
		context := maker.GetExprBuildContext(
			sqlcore.SPK_SELECT, sqlcore.SSPK_EXPR1, stack, maker.Format)
		for i, expr := range this.SelExprs {
			stat2, err := expr.GetSql(context)
			if err != nil {
				return err
			}
			stat.AppendStatPart(stat2)
			if i < len(this.SelExprs)-1 {
				stat.WriteString(", ")
			}
		}
	}
	return nil
}

func (this *sel) buildSelectSectionSql(maker *maker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.GetLeadingSpace())
	stat.WriteString("select ")
	// generate fields section sql
	err := this.buildFieldsSectionSql(maker, stat, stack)
	if err != nil {
		return err
	}
	return nil
}

// TODO remove code below, since all validation should be done in Expr.GetSql
/*func (this *sel) checkFieldsBelongToAddedTables(maker *selectMaker) error {
    for _, expr := range sel.SelExprs {
        fields := expr.CollectFields()
        for _, field := range fields {
            ta := maker.FindTable(field)
            if ta == nil {
                t := field.Table
                objstr, err := formatPrettySubquery(t, true, nil)
                if err != nil {
                    return err
                }
                return e("%s from field \"%s\" haven't been added to the statement",
                    objstr, field.Name)
            }
        }
    }
    return nil
}
*/
func (this *sel) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_SELECT
}

func (this *sel) GetParent() sqlcore.SqlPart {
	return nil
}
