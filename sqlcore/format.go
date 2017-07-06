package sqlcore

import (
	"strings"

	"github.com/d2r2/sqlg/sqldef"
)

type BuildOptions int

const (
	BO_NOOPTIONS BuildOptions = 0
	BO_INLINE                 = 1 << iota
	BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS
	BO_USE_SCHEMA_NAME
	BO_SUPPORT_MULT_STATS_IN_A_BATCH
	BO_COLUMN_NAME_AND_COUNT_VALIDATION
	BO_ODBC_MODE
)

func (this BuildOptions) String() string {
	var tmplt = map[BuildOptions]string{
		BO_NOOPTIONS:                        "BO_NOOPTIONS",
		BO_INLINE:                           "BO_INLINE",
		BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS:   "BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS",
		BO_USE_SCHEMA_NAME:                  "BO_USE_SCHEMA_NAME",
		BO_SUPPORT_MULT_STATS_IN_A_BATCH:    "BO_SUPPORT_MULT_STATS_IN_A_BATCH",
		BO_COLUMN_NAME_AND_COUNT_VALIDATION: "BO_COLUMN_NAME_AND_COUNT_VALIDATION",
	}
	return tmplt[this]
}

type Format struct {
	Dialect        sqldef.Dialect
	Options        BuildOptions
	SchemaName     *string
	DatabaseName   *string
	indentLevel    int
	SectionDivider string
	paramIndex     int
}

func NewFormat(dialect sqldef.Dialect) *Format {
	format := &Format{Dialect: dialect,
		SectionDivider: "\n"}
	format.AddOptions(BO_COLUMN_NAME_AND_COUNT_VALIDATION)
	if dialect.SupportMultipleStatementsInBatch() {
		format.AddOptions(BO_SUPPORT_MULT_STATS_IN_A_BATCH)
	}
	return format
}

func (this *Format) SkipValidation() {
	this.RemoveOptions(BO_COLUMN_NAME_AND_COUNT_VALIDATION)
}

func (this *Format) SetOptions(opts BuildOptions) {
	this.Options = opts
}

func (this *Format) AddOptions(opts BuildOptions) {
	this.Options = this.Options | opts
}

func (this *Format) RemoveOptions(opts BuildOptions) {
	this.Options = this.Options & ^opts
}

func (this *Format) Inline() bool {
	return this.Options&BO_INLINE == BO_INLINE
}

func (this *Format) OdbcMode() bool {
	return this.Options&BO_ODBC_MODE == BO_ODBC_MODE
}

func (this *Format) DoIfObjectExistsNotExists() bool {
	return this.Options&BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS ==
		BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS
}

/*
func (this *Format) UseDatabaseName() bool {
    return this.Dialect.SupportDatabases() &&
        this.Options&BO_USE_DATABASE_NAME ==
            BO_USE_DATABASE_NAME
}
*/
func (this *Format) UseSchemaName() bool {
	return this.SchemaName != nil ||
		this.Options&BO_USE_SCHEMA_NAME ==
			BO_USE_SCHEMA_NAME
}

func (this *Format) ColumnNameAndCountValidationIsOn() bool {
	return this.Options&BO_COLUMN_NAME_AND_COUNT_VALIDATION ==
		BO_COLUMN_NAME_AND_COUNT_VALIDATION
}

func (this *Format) GetSchemaName() *string {
	if this.UseSchemaName() {
		if this.SchemaName != nil {
			return this.SchemaName
		} else {
			return this.Dialect.GetDefaultSchema()
		}
	}
	return nil
}

func (this *Format) SupportMultipleStatementsInBatch() bool {
	return this.Options&BO_SUPPORT_MULT_STATS_IN_A_BATCH ==
		BO_SUPPORT_MULT_STATS_IN_A_BATCH
}

func (this *Format) GetLeadingSpace() string {
	return strings.Repeat(" ", this.indentLevel*4)
}

func (this *Format) IncIndentLevel() {
	this.indentLevel++
}

func (this *Format) DecIndentLevel() {
	this.indentLevel--
}

func (this *Format) IncParamIndex() {
	this.paramIndex++
}

func (this *Format) GetParamIndex() int {
	return this.paramIndex
}

func (this *Format) FormatObjectName(name string) string {
	switch this.Dialect {
	case sqldef.DI_MSTSQL:
		return f("[%s]", name)
	case sqldef.DI_PGSQL:
		return f("\"%s\"", name)
	case sqldef.DI_MYSQL:
		return f("`%s`", name)
	default:
		return f("%s", name)
	}
}

func (this *Format) FormatTableName(tableName string) string {
	if this.DatabaseName != nil {
		schema := this.SchemaName
		if schema == nil {
			schema = this.Dialect.GetDefaultSchema()
		}
		if schema != nil {
			return f("%s.%s.%s",
				this.FormatObjectName(*this.DatabaseName),
				*schema, this.FormatObjectName(tableName))
		} else {
			return f("%s.%s",
				this.FormatObjectName(*this.DatabaseName),
				this.FormatObjectName(tableName))
		}
	} else if this.GetSchemaName() != nil {
		return f("%s.%s", *this.GetSchemaName(),
			this.FormatObjectName(tableName))
	} else {
		return this.FormatObjectName(tableName)
	}
}

func (this *Format) FormatDataSourceRef(query Query) (
	*Statement, error) {
	stat := NewStatement(SS_UNDEF)
	queryAlias, aliasBased := query.(QueryAlias)
	tableBased, table := query.IsTableBased()
	if tableBased == false && aliasBased == false {
		return nil, e("Can't create sql statement for the object, " +
			"since it's not a table neither has alias specified")
	}
	if aliasBased {
		query = queryAlias.GetSource()
	}
	sqlValid, sqlValidOk := query.(SqlReady)
	if sqlValidOk {
		this.IncIndentLevel()
		batch, err := sqlValid.GetSql(this)
		this.DecIndentLevel()
		if err != nil {
			return nil, err
		}
		if len(batch.Items) > 1 {
			return nil, e("Can't use multiple sql statments "+
				"in \"from\" section: %v", batch)
		}
		stat = batch.Items[0]
	} else if tableBased {
		stat.WriteString(this.FormatTableName(table.GetName()))
	}
	if aliasBased {
		newst := NewStatement(SS_UNDEF)
		if sqlValidOk {
			newst.WriteString("(")
			newst.WriteString(this.SectionDivider)
		}
		newst.AppendStatPart(stat)
		if sqlValidOk {
			newst.WriteString(this.SectionDivider)
			newst.WriteString(")")
		}
		newst.WriteString(" as %s", queryAlias.GetAlias())
		stat = newst
	}
	return stat, nil
}

type CallStack struct {
	Stack []SqlPart
}

func NewCallStack() *CallStack {
	s := &CallStack{}
	return s
}

func (this *CallStack) Push(section SqlPart) {
	this.Stack = append(this.Stack, section)
}

func (this *CallStack) Pop() SqlPart {
	section := this.Stack[len(this.Stack)-1]
	this.Stack = this.Stack[:len(this.Stack)-1]
	return section
}

func (this *CallStack) Current(t SqlPartKind) bool {
	return this.Stack[len(this.Stack)-1].GetPartKind() == t
}

func (this *CallStack) First(t SqlPartKind) SqlPart {
	for _, item := range this.Stack {
		if item.GetPartKind() == t {
			return item
		}
	}
	return nil
}
