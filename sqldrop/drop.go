package sqldrop

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldb"
	"github.com/d2r2/sqlg/sqldef"
	"github.com/d2r2/sqlg/sqlexp"
)

type dropDatabaseMaker struct {
	Format *sqlcore.Format
	Batch  *sqlcore.StatementBatch
}

func ifExistsNotExistsBlockMicrosoftCase(part sqlcore.SqlPart,
	stat *sqlcore.Statement, format *sqlcore.Format, stack *sqlcore.CallStack) (*sqlcore.Statement, error) {
	partKind := part.GetPartKind()
	newst := sqlcore.NewStatement(sqlcore.SS_EXEC)
	ef := sqlexp.Factory()
	var fnc sqlexp.Expr
	switch partKind {
	case sqlcore.SPK_DROP_DATABASE:
		sect := part.(*dropDatabase)
		dbId := ef.FuncDef(ef.FuncDialectDef(
			sqldef.DI_MSTSQL, "db_id({})", 1, 1))
		fnc = ef.IsNotNull(ef.Func(dbId, sect.DatabaseName))
	case sqlcore.SPK_DROP_TABLE:
		sect := part.(*dropTable)
		objectId := ef.FuncDef(ef.FuncDialectDef(
			sqldef.DI_MSTSQL, "object_id({})", 1, 2))
		name := format.FormatTableName(sect.Table.Name)
		fnc = ef.IsNotNull(ef.Func(objectId, name, "U"))
	}
	context := sqlexp.NewExprBuildContext(partKind, sqlcore.SSPK_EXPR1,
		stack, format, nil)
	stat2, err := fnc.GetSql(context)
	if err != nil {
		return nil, err
	}
	newst.WriteString(format.GetLeadingSpace())
	newst.AppendStatPartsFormat("if %s begin", stat2)
	newst.WriteString(format.SectionDivider)
	newst.AppendStatPart(stat)
	newst.WriteString(format.SectionDivider)
	newst.WriteString(format.GetLeadingSpace())
	newst.WriteString("end")
	return newst, nil
}

func (this *dropDatabaseMaker) buildDropDatabase(sect *dropDatabase,
	stack *sqlcore.CallStack) error {
	if this.Format.DoIfObjectExistsNotExists() &&
		this.Format.Dialect == sqldef.DI_MSTSQL {
		this.Format.IncIndentLevel()
		defer this.Format.DecIndentLevel()
	}
	err := sect.buildDropDatabaseSql(this, this.Batch.Last(), stack)
	return err
}

func (this *dropDatabaseMaker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct == false {
		var err error
		switch part.GetPartKind() {
		case sqlcore.SPK_DROP_DATABASE:
			sect := part.(*dropDatabase)
			err = this.buildDropDatabase(sect, stack)
			if err != nil {
				return err
			}
			if this.Format.DoIfObjectExistsNotExists() &&
				this.Format.Dialect == sqldef.DI_MSTSQL {
				stat := this.Batch.Last()
				newstat, err := ifExistsNotExistsBlockMicrosoftCase(
					part, stat, this.Format, stack)
				if err != nil {
					return err
				}
				this.Batch.Replace(stat, newstat)
			}
		default:
			err = e("Unexpected section during generating "+
				"\"drop database\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *dropDatabaseMaker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

func (this *dropDatabaseMaker) GetExprBuildContext(sectionKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	context := sqlexp.NewExprBuildContext(sectionKind, subPartKind, stack, format, nil)
	return context
}

type DropDatabase interface {
	sqlcore.SqlComplete
	sqlcore.SqlPart
}

type dropDatabase struct {
	DatabaseName string
}

func NewDropDatabase(databaseName string) DropDatabase {
	r := &dropDatabase{DatabaseName: databaseName}
	return r
}

func (this *dropDatabase) buildDropDatabaseSql(maker *dropDatabaseMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString("drop database ")
	if maker.Format.DoIfObjectExistsNotExists() &&
		maker.Format.Dialect.In(sqldef.DI_PGSQL|sqldef.DI_MYSQL) {
		stat.WriteString("if exists ")
	}
	name := maker.Format.FormatObjectName(this.DatabaseName)
	stat.WriteString(name)
	return nil
}

func (this *dropDatabase) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &dropDatabaseMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, err
}

func (this *dropDatabase) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_DROP_DATABASE
}

func (this *dropDatabase) GetParent() sqlcore.SqlPart {
	return nil
}

type dropTableMaker struct {
	Format *sqlcore.Format
	Batch  *sqlcore.StatementBatch
}

func (this *dropTableMaker) buildDropTable(sect *dropTable,
	stack *sqlcore.CallStack) error {
	if this.Format.DoIfObjectExistsNotExists() &&
		this.Format.Dialect == sqldef.DI_MSTSQL {
		this.Format.IncIndentLevel()
		defer this.Format.DecIndentLevel()
	}
	err := sect.buildDropTableSql(this, this.Batch.Last(), stack)
	return err
}

func (this *dropTableMaker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct == false {
		switch part.GetPartKind() {
		case sqlcore.SPK_DROP_TABLE:
			sect := part.(*dropTable)
			err := this.buildDropTable(sect, stack)
			if err != nil {
				return err
			}
			if this.Format.DoIfObjectExistsNotExists() &&
				this.Format.Dialect == sqldef.DI_MSTSQL {
				stat := this.Batch.Last()
				newstat, err := ifExistsNotExistsBlockMicrosoftCase(
					part, stat, this.Format, stack)
				if err != nil {
					return err
				}
				this.Batch.Replace(stat, newstat)
			}
			return nil
		default:
			return e("Unexpected section during generating "+
				"\"drop table\" statement: %v", part)
		}
	}
	return nil
}

func (this *dropTableMaker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

func (this *dropTableMaker) GetExprBuildContext(sectionKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	context := sqlexp.NewExprBuildContext(sectionKind, subPartKind, stack, format, nil)
	return context
}

type DropTable interface {
	sqlcore.SqlComplete
	sqlcore.SqlPart
}

type dropTable struct {
	Table *sqldb.TableDef
}

// TODO add "check if exists" parameter
func NewDropTable(table *sqldb.TableDef) DropTable {
	r := &dropTable{Table: table}
	return r
}

func (this *dropTable) buildDropTableSql(maker *dropTableMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.GetLeadingSpace())
	stat.WriteString("drop table ")
	if maker.Format.DoIfObjectExistsNotExists() &&
		maker.Format.Dialect.In(sqldef.DI_PGSQL|sqldef.DI_MYSQL|sqldef.DI_SQLITE) {
		stat.WriteString("if exists ")
	}
	name := maker.Format.FormatTableName(this.Table.Name)
	stat.WriteString(name)
	return nil
}

func (this *dropTable) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &dropTableMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, err
}

func (this *dropTable) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_DROP_TABLE
}

func (this *dropTable) GetParent() sqlcore.SqlPart {
	return nil
}
