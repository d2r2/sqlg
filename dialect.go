package sqlg

type Dialect int

const (
    DI_UNDEF  Dialect = 0
    DI_MSTSQL         = 1 << iota
    DI_PGSQL
    DI_MYSQL
    DI_SQLITE
    DI_ANY = DI_MSTSQL | DI_PGSQL | DI_MYSQL | DI_SQLITE
)

func (this Dialect) String() string {
    fmtStr := map[Dialect]string{
        DI_MSTSQL: "Microsoft T-SQL",
        DI_PGSQL:  "PostgreSQL",
        DI_MYSQL:  "MySql",
        DI_SQLITE: "Sqlite",
    }
    return fmtStr[this]
}

func (this Dialect) SupportMultipleDatabases() bool {
    switch this {
    case DI_SQLITE:
        return false
    case DI_MSTSQL, DI_PGSQL, DI_MYSQL:
        return true
    default:
        return false
    }
}

func (this Dialect) GetDefaultSchema() *string {
    switch this {
    case DI_MSTSQL:
        schema := ""
        return &schema
    case DI_PGSQL:
        schema := "public"
        return &schema
    case DI_MYSQL:
        return nil
    default:
        return nil
    }
}

func (this Dialect) GetSystemDatabase() *string {
    switch this {
    case DI_MSTSQL:
        systemdb := "master"
        return &systemdb
    case DI_PGSQL:
        systemdb := "postgres"
        return &systemdb
    case DI_MYSQL:
        systemdb := "information_schema"
        return &systemdb
    default:
        return nil
    }
}

func (this Dialect) SupportMultipleStatementsInBatch() bool {
    switch this {
    case DI_MSTSQL, DI_PGSQL:
        return true
    default:
        return false
    }
}

func (this Dialect) In(dialects Dialect) bool {
    return this&dialects != DI_UNDEF
}
