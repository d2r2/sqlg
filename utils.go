package sqlg

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldb"
	"github.com/d2r2/sqlg/sqldef"
	"github.com/d2r2/sqlg/sqlexp"
	"github.com/d2r2/sqlg/sqlselect"
)

type UtilStatements struct {
}

var Utils = &UtilStatements{}

func (this *UtilStatements) GetCheckStatIfDatabaseExists(
	dialect sqldef.Dialect, dbname string) (*sqlcore.StatementBatch, error) {
	switch dialect {
	case sqldef.DI_MSTSQL:
		// select case when db_id(<dbname>) is null then 0 else 1 end
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		ef := sqlexp.Factory()
		fnc1 := ef.FuncDef(ef.FuncDialectDef(dialect, "db_id({})", 1, 1))
		s := Select(ef.CaseThenElse(
			ef.IsNull(ef.Func(fnc1, dbname)), 0, 1))
		sm := sqlselect.NewMaker()
		err := sm.BuildSql(s, format)
		if err != nil {
			return nil, err
		}
		return sm.Batch, nil
	case sqldef.DI_PGSQL:
		// select count(datname) from pg_catalog.pg_database where datname=<dbname>
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		schema := "pg_catalog"
		format.SchemaName = &schema
		ef := sqlexp.Factory()
		tbl1 := sqldb.Table("pg_database")
		s := Select(ef.Count(ef.Field(tbl1, "datname"))).
			From(ef.TableAlias(tbl1, "a")).Where(ef.Equal(
			ef.Field(tbl1, "datname"), dbname))
		batch, err := s.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	case sqldef.DI_MYSQL:
		// select count(schema_name) from information_schema.schemata where schema_name=<dbname>
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		schema := "information_schema"
		format.SchemaName = &schema
		ef := sqlexp.Factory()
		tbl1 := sqldb.Table("schemata")
		s := Select(ef.Count(ef.Field(tbl1, "schema_name"))).
			From(ef.TableAlias(tbl1, "a")).Where(ef.Equal(
			ef.Field(tbl1, "schema_name"), dbname))
		batch, err := s.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	default:
		return nil, e("Can't create statement to find database "+
			"for dialect \"%v\"", dialect)
	}
}

func (this *UtilStatements) CheckIfDatabaseExists(dialect sqldef.Dialect,
	dbName string, connInit sqlcore.ConnInit) (bool, error) {
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
	dialect sqldef.Dialect, tableName string) (*sqlcore.StatementBatch, error) {
	switch dialect {
	case sqldef.DI_MSTSQL:
		// select case when object_id(<tablename>) is null then 0 else 1 end
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		ef := sqlexp.Factory()
		fnc1 := ef.FuncDef(ef.FuncDialectDef(dialect, "object_id({})", 1, 1))
		s := Select(ef.CaseThenElse(
			ef.IsNull(ef.Func(fnc1, tableName)), 0, 1))
		sm := sqlselect.NewMaker()
		err := sm.BuildSql(s, format)
		if err != nil {
			return nil, err
		}
		return sm.Batch, nil
	case sqldef.DI_PGSQL:
		// select count(a.relname) from pg_catalog.pg_class as a
		// inner join pg_catalog.pg_namespace as a on a.relnamespace = b.oid
		// where a.relname = <tablename> and b.nspname = 'public'
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		schema := "pg_catalog"
		format.SchemaName = &schema
		ef := sqlexp.Factory()
		tbl1 := sqldb.Table("pg_class")
		tbl2 := sqldb.Table("pg_namespace")
		schemaName := dialect.GetDefaultSchema()
		var s sqlcore.SqlComplete
		if schemaName == nil {
			s = Select(ef.Count(ef.Field(tbl1, "relname"))).
				From(ef.TableAlias(tbl1, "a")).
				Where(ef.Equal(ef.Field(tbl1, "relname"), tableName))
		} else {
			s = Select(ef.Count(ef.Field(tbl1, "relname"))).
				From(ef.TableAlias(tbl1, "a")).
				InnerJoin(ef.TableAlias(tbl2, "b"),
				ef.Equal(ef.Field(tbl1, "relnamespace"), ef.Field(tbl2, "oid"))).
				Where(ef.And(ef.Equal(ef.Field(tbl2, "nspname"), *schemaName),
				ef.Equal(ef.Field(tbl1, "relname"), tableName)))
		}
		batch, err := s.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	case sqldef.DI_MYSQL:
		// select count(table_name) from information_schema.tables
		// where table_schema = database() and table_name = <tablename>
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		schema := "information_schema"
		format.SchemaName = &schema
		ef := sqlexp.Factory()
		tbl1 := sqldb.Table("tables")
		fnc1 := ef.FuncDef(ef.FuncDialectDef(sqldef.DI_MYSQL,
			"database()", 0, 0))
		s := Select(ef.Count(ef.Field(tbl1, "table_name"))).
			From(ef.TableAlias(tbl1, "a")).
			Where(ef.And(
			ef.Equal(ef.Field(tbl1, "table_schema"), ef.Func(fnc1)),
			ef.Equal(ef.Field(tbl1, "table_name"), tableName)))
		batch, err := s.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	case sqldef.DI_SQLITE:
		format := sqlcore.NewFormat(dialect)
		format.SkipValidation()
		ef := sqlexp.Factory()
		tbl1 := sqldb.Table("sqlite_master")
		s := Select(ef.Count(ef.Field(tbl1, "name"))).
			From(ef.TableAlias(tbl1, "a")).
			Where(ef.And(ef.Equal(ef.Field(tbl1, "name"), tableName),
			ef.Equal(ef.Field(tbl1, "type"), "table")))
		batch, err := s.GetSql(format)
		if err != nil {
			return nil, err
		}
		return batch, nil
	default:
		return nil, e("Can't create statement to find database "+
			"for dialect \"%v\"", dialect)
	}
}
