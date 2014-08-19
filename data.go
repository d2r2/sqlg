package sqlg

type DataType int

const (
    DT_UNDEF           DataType = 0
    DT_INT_SMALL                = 1 << iota // integer:           2 byte
    DT_INT                                  // integer:           4 byte
    DT_INT_BIG                              // integer:           8 byte
    DT_NUMERIC                              // fixed-point:       variable
    DT_DECIMAL                              // fixed-point:       variable
    DT_REAL                                 // floating-point:    4 bytes; SQL-standard float(1..24)
    DT_DOUBLE                               // floating-point:    8 bytes; SQL-standard float(25..53)
    DT_FLOAT                                // floating-point:    4 or 8 bytes; SQL-standard notation
    DT_UNICODE_CHAR                         // string:            fixed-length
    DT_UNICODE_VARCHAR                      // string:            variable-length
    DT_BOOL
    DT_DATETIME
    DT_DATE
    DT_TIME
    DT_AUTOINC_INT
    DT_AUTOINC_INT_BIG
    DT_ALL = DT_INT_SMALL | DT_INT | DT_INT_BIG |
        DT_NUMERIC | DT_DECIMAL |
        DT_REAL | DT_DOUBLE | DT_FLOAT |
        DT_UNICODE_CHAR | DT_UNICODE_VARCHAR |
        DT_BOOL |
        DT_DATETIME | DT_DATE | DT_TIME |
        DT_AUTOINC_INT | DT_AUTOINC_INT_BIG
)

func (this DataType) String() string {
    fmtStr := map[DataType]string{
        DT_UNDEF:           "undefined",
        DT_INT_SMALL:       "int small",
        DT_INT:             "int",
        DT_INT_BIG:         "int big",
        DT_NUMERIC:         "numeric",
        DT_DECIMAL:         "decimal",
        DT_REAL:            "real",
        DT_DOUBLE:          "double precision",
        DT_FLOAT:           "float",
        DT_UNICODE_CHAR:    "char",
        DT_UNICODE_VARCHAR: "varchar",
        DT_BOOL:            "bool",
        DT_DATETIME:        "datetime",
        DT_DATE:            "date",
        DT_TIME:            "time",
        DT_AUTOINC_INT:     "auto increment",
        DT_AUTOINC_INT_BIG: "auto increment big",
    }
    return fmtStr[this]
}

func (this DataType) In(types DataType) bool {
    return this&types != DT_UNDEF
}

type DataDef struct {
    Type  DataType
    Size1 int
    Size2 int
}

func NewDataDef(dataType DataType, size1, size2 int) *DataDef {
    this := &DataDef{Type: dataType,
        Size1: size1, Size2: size2}
    return this
}

func (this *DataDef) String() string {
    return f("DataDef(Type: %v, Size1: %d, Size2: %d)",
        this.Type, this.Size1, this.Size2)
}

type BuildSqlDataRule struct {
    Dialects Dialect
    Template ST
}

func bsdr(dialects Dialect, template ST) *BuildSqlDataRule {
    c := &BuildSqlDataRule{Dialects: dialects, Template: template}
    return c
}

type BuildSqlDataVarianceRule struct {
    Items []*BuildSqlDataRule
}

func bsdvr(items ...*BuildSqlDataRule) *BuildSqlDataVarianceRule {
    dcl := &BuildSqlDataVarianceRule{Items: items}
    return dcl
}

type ST struct {
    Template   string
    ParamCount int
}

func (this *DataDef) GetStrTemplate(dialect Dialect) *ST {
    tmplt := map[DataType]*BuildSqlDataVarianceRule{
        DT_INT_SMALL: bsdvr(bsdr(DI_ANY, ST{"smallint", 0})),
        DT_INT:       bsdvr(bsdr(DI_ANY, ST{"int", 0})),
        DT_INT_BIG:   bsdvr(bsdr(DI_ANY, ST{"bigint", 0})),
        DT_REAL:      bsdvr(bsdr(DI_ANY, ST{"real", 0})),
        DT_DOUBLE: bsdvr(bsdr(DI_PGSQL, ST{"double precision", 0}),
            bsdr(DI_MSTSQL, ST{"float(53)", 0}),
            bsdr(DI_MYSQL|DI_SQLITE, ST{"double", 0})),
        DT_FLOAT:   bsdvr(bsdr(DI_ANY, ST{"float(%d)", 1})),
        DT_NUMERIC: bsdvr(bsdr(DI_ANY, ST{"numeric(%d,%d)", 2})),
        DT_DECIMAL: bsdvr(bsdr(DI_ANY, ST{"decimal(%d,%d)", 2})),
        DT_DATETIME: bsdvr(bsdr(DI_MSTSQL|DI_SQLITE, ST{"datetime", 0}),
            bsdr(DI_PGSQL|DI_MYSQL, ST{"timestamp", 0})),
        DT_DATE: bsdvr(bsdr(DI_ANY, ST{"date", 0})),
        DT_TIME: bsdvr(bsdr(DI_ANY, ST{"time", 0})),
        DT_AUTOINC_INT: bsdvr(bsdr(DI_MSTSQL, ST{"int identity(1,1)", 0}),
            bsdr(DI_PGSQL, ST{"serial", 0}),
            bsdr(DI_MYSQL, ST{"int", 0}),
            bsdr(DI_SQLITE, ST{"integer", 0})),
        DT_AUTOINC_INT_BIG: bsdvr(bsdr(DI_MSTSQL, ST{"bigint identity(1,1)", 0}),
            bsdr(DI_PGSQL, ST{"bigserial", 0}),
            bsdr(DI_MYSQL|DI_SQLITE, ST{"bigint", 0})),
        DT_BOOL: bsdvr(bsdr(DI_MSTSQL, ST{"bit", 0}),
            bsdr(DI_PGSQL|DI_MYSQL|DI_SQLITE, ST{"boolean", 0})),
        DT_UNICODE_CHAR: bsdvr(bsdr(DI_MSTSQL, ST{"nchar(%d)", 1}),
            bsdr(DI_PGSQL|DI_SQLITE, ST{"char(%d)", 1}),
            bsdr(DI_MYSQL, ST{"char(%d) character set utf8", 1})),
        DT_UNICODE_VARCHAR: bsdvr(bsdr(DI_MSTSQL, ST{"nvarchar(%d)", 1}),
            bsdr(DI_PGSQL|DI_SQLITE, ST{"varchar(%d)", 1}),
            bsdr(DI_MYSQL, ST{"varchar(%d) character set utf8", 1})),
    }
    if dcl, ok := tmplt[this.Type]; ok {
        for _, dc := range dcl.Items {
            if dialect.In(dc.Dialects) {
                return &dc.Template
            }
        }
    }
    return nil
}
