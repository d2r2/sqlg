package sqlg

import (
    "bytes"
    "strings"
)

type BuildOptions int

const (
    BO_NOOPTIONS BuildOptions = 0
    BO_INLINE                 = 1 << iota
    BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS
    BO_USE_SCHEMA_NAME
    BO_SUPPORT_MULT_STATS_IN_A_BATCH
    BO_COLUMN_NAME_AND_COUNT_VALIDATION
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
    Dialect        Dialect
    Options        BuildOptions
    SchemaName     *string
    DatabaseName   *string
    indentLevel    int
    sectionDivider string
}

func NewFormat(dialect Dialect) *Format {
    format := &Format{Dialect: dialect,
        sectionDivider: "\n"}
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

func (this *Format) getLeadingSpace() string {
    return strings.Repeat(" ", this.indentLevel*4)
}

func (this *Format) incIndentLevel() {
    this.indentLevel++
}

func (this *Format) decIndentLevel() {
    this.indentLevel--
}

func (this *Format) formatObjectName(name string) string {
    switch this.Dialect {
    case DI_MSTSQL:
        return f("[%s]", name)
    case DI_PGSQL:
        return f("\"%s\"", name)
    case DI_MYSQL:
        return f("`%s`", name)
    default:
        return f("%s", name)
    }
}

func (this *Format) formatTableName(tableName string) string {
    if this.DatabaseName != nil {
        schema := this.SchemaName
        if schema == nil {
            schema = this.Dialect.GetDefaultSchema()
        }
        if schema != nil {
            return f("%s.%s.%s",
                this.formatObjectName(*this.DatabaseName),
                *schema, this.formatObjectName(tableName))
        } else {
            return f("%s.%s",
                this.formatObjectName(*this.DatabaseName),
                this.formatObjectName(tableName))
        }
    } else if this.GetSchemaName() != nil {
        return f("%s.%s", *this.GetSchemaName(),
            this.formatObjectName(tableName))
    } else {
        return this.formatObjectName(tableName)
    }
}

func (this *Format) formatDataSourceRef(query Query) (
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
    sqlReady, sqlReadyOk := query.(SqlReady)
    if sqlReadyOk {
        this.incIndentLevel()
        batch, err := sqlReady.GetSql(this)
        this.decIndentLevel()
        if err != nil {
            return nil, err
        }
        if len(batch.Items) > 1 {
            return nil, e("Can't use multiple sql statments "+
                "in \"from\" section: %v", batch)
        }
        stat = batch.Items[0]
    } else if tableBased {
        stat.writeString(this.formatTableName(table.Name))
    }
    if aliasBased {
        newst := NewStatement(SS_UNDEF)
        if sqlReadyOk {
            newst.writeString("(")
            newst.writeString(this.sectionDivider)
        }
        newst.appendStatPart(stat)
        if sqlReadyOk {
            newst.writeString(this.sectionDivider)
            newst.writeString(")")
        }
        newst.writeString(" as %s", queryAlias.GetAlias())
        stat = newst
    }
    return stat, nil
}

func formatPrettyDataSource(query Query,
    capital bool, dialect *Dialect) (string, error) {
    var str string
    tableBased, table := query.IsTableBased()
    sqlReady, sqlReadyOk := query.(SqlReady)
    if tableBased {
        var buf bytes.Buffer
        for i, field := range table.Fields.Items {
            buf.WriteString(f("\"%s\"", field.Name))
            if i < len(table.Fields.Items)-1 {
                buf.WriteString(",")
            }
        }
        str = f("table \"%s\" with fields: %s",
            table.Name, buf.String())
    } else if sqlReadyOk && dialect != nil {
        f2 := NewFormat(*dialect)
        f2.AddOptions(BO_INLINE)
        f2.sectionDivider = " "
        batch, err := sqlReady.GetSql(f2)
        if err != nil {
            return "", err
        }
        stat := batch.Items[0]
        str = f("\"%s\"", stat.Sql())
    } else {
        str = "query"
    }
    if capital {
        return strings.ToUpper(str[:1]) + str[1:], nil
    } else {
        return str, nil
    }
}

type CallStack struct {
    Stack []Section
}

func NewCallStack() *CallStack {
    s := &CallStack{}
    return s
}

func (this *CallStack) Push(section Section) {
    this.Stack = append(this.Stack, section)
}

func (this *CallStack) Pop() Section {
    section := this.Stack[len(this.Stack)-1]
    this.Stack = this.Stack[:len(this.Stack)-1]
    return section
}

func (this *CallStack) Current(t SectionKind) bool {
    return this.Stack[len(this.Stack)-1].GetSectionKind() == t
}

func (this *CallStack) First(t SectionKind) Section {
    for _, item := range this.Stack {
        if item.GetSectionKind() == t {
            return item
        }
    }
    return nil
}
