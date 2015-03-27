package sqlg

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqlcreate"
	"github.com/d2r2/sqlg/sqldb"
	"github.com/d2r2/sqlg/sqldelete"
	"github.com/d2r2/sqlg/sqldrop"
	"github.com/d2r2/sqlg/sqlexp"
	"github.com/d2r2/sqlg/sqlinsert"
	"github.com/d2r2/sqlg/sqlselect"
	"github.com/d2r2/sqlg/sqlupdate"
)

func Select(fields ...sqlexp.Expr) sqlselect.Select {
	s := sqlselect.NewSelect(fields...)
	return s
}

func Insert(table sqlcore.Query, fields ...*sqlexp.TokenField) sqlinsert.Insert {
	ins := sqlinsert.NewInsert(table, fields...)
	return ins
}

func Update(table sqlcore.Query, first *sqlexp.TokenFieldAssign,
	rest ...*sqlexp.TokenFieldAssign) sqlupdate.Update {
	fields := []*sqlexp.TokenFieldAssign{first}
	fields = append(fields, rest...)
	upd := sqlupdate.NewUpdate(table, fields...)
	return upd
}

func Delete(table sqlcore.Query) sqldelete.Delete {
	del := sqldelete.NewDelete(table)
	return del
}

func CreateDatabase(databaseName string) sqlcreate.CreateDatabase {
	create := sqlcreate.NewCreateDatabase(databaseName)
	return create
}

func CreateTable(table *sqldb.TableDef) sqlcreate.CreateTable {
	create := sqlcreate.NewCreateTable(table)
	return create
}

func DropDatabase(databaseName string) sqldrop.DropDatabase {
	drop := sqldrop.NewDropDatabase(databaseName)
	return drop
}

func DropTable(table *sqldb.TableDef) sqldrop.DropTable {
	r := sqldrop.NewDropTable(table)
	return r
}
