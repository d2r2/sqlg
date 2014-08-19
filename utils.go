package sqlg

type UtilStatements struct {
}

var Utils = &UtilStatements{}

func (this *UtilStatements) GetCheckStatIfDatabaseExists(
	dialect Dialect, dbname string) (*StatementBatch, error) {
	switch dialect {
	case DI_MSTSQL:
		// select case when db_id(<dbname>) is null then 0 else 1 end
		format := NewFormat(dialect)
		format.SkipValidation()
		ef := NewExprFactory()
		fnc1 := ef.FuncDef(ef.FuncDialectDef(dialect, "db_id({})", 1, 1))
		sel := NewSelectRoot(ef.CaseThenElse(
			ef.IsNull(ef.Func(fnc1, dbname)), 0, 1))
		sm := NewSelectMaker()
		err := sm.BuildSql(sel, format)
		if err != nil {
			return nil, err
		}
		return sm.Batch, nil
	case DI_PGSQL:
		// select count(datname) from pg_catalog.pg_database where datname=<dbname>
		format := NewFormat(dialect)
		format.SkipValidation()
		schema := "pg_catalog"
		format.SchemaName = &schema
		ef := NewExprFactory()
		tbl1 := ef.Table("pg_database")
		sel := NewSelectRoot(ef.Count(ef.Field(tbl1, "datname"))).
			From(ef.TableAlias(tbl1, "a")).Where(ef.Equal(
			ef.Field(tbl1, "datname"), dbname))
		batch, err := sel.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	case DI_MYSQL:
		// select count(schema_name) from information_schema.schemata where schema_name=<dbname>
		format := NewFormat(dialect)
		format.SkipValidation()
		schema := "information_schema"
		format.SchemaName = &schema
		ef := NewExprFactory()
		tbl1 := ef.Table("schemata")
		sel := NewSelectRoot(ef.Count(ef.Field(tbl1, "schema_name"))).
			From(ef.TableAlias(tbl1, "a")).Where(ef.Equal(
			ef.Field(tbl1, "schema_name"), dbname))
		batch, err := sel.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	default:
		return nil, e("Can't create statement to find database "+
			"for dialect \"%v\"", dialect)
	}
}

func (this *UtilStatements) CheckIfDatabaseExists(dialect Dialect,
	dbName string, connInit ConnInit) (bool, error) {
	batch, err := this.GetCheckStatIfDatabaseExists(dialect, dbName)
	if err != nil {
		return false, err
	}
	dbname := dialect.GetSystemDatabase()
	db, err := connInit.Open(dialect, dbname)
	if err != nil {
		return false, err
	}
	defer db.Close()
	row, err := batch.ExecQueryRow(db)
	if err != nil {
		return false, err
	}
	var dbcount int
	if err := row.Scan(&dbcount); err != nil {
		return false, err
	}
	return dbcount != 0, nil
}

func (this *UtilStatements) CheckStatIfTableExists(
	dialect Dialect, tableName string) (*StatementBatch, error) {
	switch dialect {
	case DI_MSTSQL:
		// select case when object_id(<tablename>) is null then 0 else 1 end
		format := NewFormat(dialect)
		format.SkipValidation()
		ef := NewExprFactory()
		fnc1 := ef.FuncDef(ef.FuncDialectDef(dialect, "object_id({})", 1, 1))
		sel := NewSelectRoot(ef.CaseThenElse(
			ef.IsNull(ef.Func(fnc1, tableName)), 0, 1))
		sm := NewSelectMaker()
		err := sm.BuildSql(sel, format)
		if err != nil {
			return nil, err
		}
		return sm.Batch, nil
	case DI_PGSQL:
		// select count(a.relname) from pg_catalog.pg_class as a
		// inner join pg_catalog.pg_namespace as a on a.relnamespace = b.oid
		// where a.relname = <tablename> and b.nspname = 'public'
		format := NewFormat(dialect)
		format.SkipValidation()
		schema := "pg_catalog"
		format.SchemaName = &schema
		ef := NewExprFactory()
		tbl1 := ef.Table("pg_class")
		tbl2 := ef.Table("pg_namespace")
		schemaName := dialect.GetDefaultSchema()
		var sel SqlReady
		if schemaName == nil {
			sel = NewSelectRoot(ef.Count(ef.Field(tbl1, "relname"))).
				From(ef.TableAlias(tbl1, "a")).
				Where(ef.Equal(ef.Field(tbl1, "relname"), tableName))
		} else {
			sel = NewSelectRoot(ef.Count(ef.Field(tbl1, "relname"))).
				From(ef.TableAlias(tbl1, "a")).
				InnerJoin(ef.TableAlias(tbl2, "b"),
				ef.Equal(ef.Field(tbl1, "relnamespace"), ef.Field(tbl2, "oid"))).
				Where(ef.And(ef.Equal(ef.Field(tbl2, "nspname"), *schemaName),
				ef.Equal(ef.Field(tbl1, "relname"), tableName)))
		}
		batch, err := sel.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	case DI_MYSQL:
		// select count(table_name) from information_schema.tables
		// where table_schema = database() and table_name = <tablename>
		format := NewFormat(dialect)
		format.SkipValidation()
		schema := "information_schema"
		format.SchemaName = &schema
		ef := NewExprFactory()
		tbl1 := ef.Table("tables")
		fnc1 := ef.FuncDef(ef.FuncDialectDef(DI_MYSQL,
			"database()", 0, 0))
		sel := NewSelectRoot(ef.Count(ef.Field(tbl1, "table_name"))).
			From(ef.TableAlias(tbl1, "a")).
			Where(ef.And(
			ef.Equal(ef.Field(tbl1, "table_schema"), ef.Func(fnc1)),
			ef.Equal(ef.Field(tbl1, "table_name"), tableName)))
		batch, err := sel.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	case DI_SQLITE:
		format := NewFormat(dialect)
		format.SkipValidation()
		ef := NewExprFactory()
		tbl1 := ef.Table("sqlite_master")
		sel := NewSelectRoot(ef.Count(ef.Field(tbl1, "name"))).
			From(ef.TableAlias(tbl1, "a")).
			Where(ef.And(ef.Equal(ef.Field(tbl1, "name"), tableName),
			ef.Equal(ef.Field(tbl1, "type"), "table")))
		batch, err := sel.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	default:
		return nil, e("Can't create statement to find database "+
			"for dialect \"%v\"", dialect)
	}
}
