package sqlcreate

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldb"
	"github.com/d2r2/sqlg/sqldef"
	//	"github.com/d2r2/sqlg/sqldrop"
	"github.com/d2r2/sqlg/sqlexp"
)

type createDatabaseMaker struct {
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
	case sqlcore.SPK_CREATE_DATABASE:
		sect := part.(*createDatabase)
		dbId := ef.FuncDef(ef.FuncDialectDef(
			sqldef.DI_MSTSQL, "db_id({})", 1, 1))
		fnc = ef.IsNull(ef.Func(dbId, sect.DatabaseName))
	case sqlcore.SPK_CREATE_TABLE:
		sect := part.(*createTable)
		objectId := ef.FuncDef(ef.FuncDialectDef(
			sqldef.DI_MSTSQL, "object_id({})", 1, 2))
		name := format.FormatTableName(sect.Table.Name)
		fnc = ef.IsNull(ef.Func(objectId, name, "U"))
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

func (this *createDatabaseMaker) buildCreateDatabaseSql(sect *createDatabase,
	stack *sqlcore.CallStack) error {
	if this.Format.DoIfObjectExistsNotExists() &&
		this.Format.Dialect == sqldef.DI_MSTSQL {
		this.Format.IncIndentLevel()
		defer this.Format.DecIndentLevel()
	}
	err := sect.buildCreateDatabaseSql(this, this.Batch.Last(), stack)
	return err

}

func (this *createDatabaseMaker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct == false {
		var err error
		switch part.GetPartKind() {
		case sqlcore.SPK_CREATE_DATABASE:
			sect := part.(*createDatabase)
			err = this.buildCreateDatabaseSql(sect, stack)
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
				"\"create database\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *createDatabaseMaker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	this.Format = format
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

func (this *createDatabaseMaker) GetExprBuildContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	context := sqlexp.NewExprBuildContext(partKind, subPartKind, stack, format, nil)
	return context
}

type CreateDatabase interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
}

type createDatabase struct {
	DatabaseName string
}

func NewCreateDatabase(databaseName string) CreateDatabase {
	r := &createDatabase{DatabaseName: databaseName}
	return r
}

// TODO verify that primary key fields belong to table
func (this *createDatabase) buildCreateDatabaseSql(maker *createDatabaseMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString("create database ")
	if maker.Format.DoIfObjectExistsNotExists() {
		switch maker.Format.Dialect {
		case sqldef.DI_MYSQL:
			stat.WriteString("if not exists ")
		case sqldef.DI_PGSQL:
			log.Warnf("%v dialect doesn't support \"IF NOT EXISTS\" option "+
				"for \"create database\" statement", maker.Format.Dialect)
		}
	}
	name := maker.Format.FormatObjectName( /*this.Db.Name*/ this.DatabaseName)
	stat.WriteString(name)
	return nil
}

func (this *createDatabase) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	m := &createDatabaseMaker{}
	err := m.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return m.Batch, err
}

func (this *createDatabase) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_CREATE_DATABASE
}

func (this *createDatabase) GetParent() sqlcore.SqlPart {
	return nil
}

type createTableMaker struct {
	Format *sqlcore.Format
	Batch  *sqlcore.StatementBatch
}

func (this *createTableMaker) runMaker(direct bool,
	part sqlcore.SqlPart, stack *sqlcore.CallStack) error {
	if direct == false {
		var err error
		switch part.GetPartKind() {
		case sqlcore.SPK_CREATE_TABLE:
			sect := part.(*createTable)
			err = sect.buildCreateTableSql(this, stack)
			if err != nil {
				return err
			}
		default:
			err = e("Unexpected section during generating "+
				"\"created table\" statement: %v", part)
		}
		return err
	}
	return nil
}

func (this *createTableMaker) BuildSql(part sqlcore.SqlPart,
	format *sqlcore.Format) error {
	f := *format
	this.Format = &f
	// SQLite create table statment doesn't support not-inline constructions
	if this.Format.Dialect == sqldef.DI_SQLITE {
		this.Format.AddOptions(sqlcore.BO_INLINE)
	}
	this.Batch = sqlcore.NewStatementBatch()
	this.Batch.Add(sqlcore.NewStatement(sqlcore.SS_EXEC))
	return sqlcore.IterateSqlParents(false, part, this.runMaker)
}

func (this *createTableMaker) GetExprBuildContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack,
	format *sqlcore.Format) *sqlexp.ExprBuildContext {
	context := sqlexp.NewExprBuildContext(partKind, subPartKind, stack, format, nil)
	return context
}

type CreateTable interface {
	sqlcore.SqlReady
	sqlcore.SqlPart
}

type createTable struct {
	Table *sqldb.TableDef
}

// TODO add "check if exists" parameter
func NewCreateTable(table *sqldb.TableDef) CreateTable {
	r := &createTable{Table: table}
	return r
}

func (this *createTable) getSqlFieldDataType(stat *sqlcore.Statement,
	format *sqlcore.Format, stack *sqlcore.CallStack, field *sqldb.FieldDef) error {
	data := field.Data.GetStrTemplate(format.Dialect)
	if data != nil {
		switch data.ParamCount {
		case 0:
			stat.WriteString(data.Template)
			return nil
		case 1:
			stat.WriteString(data.Template, field.Data.Size1)
			return nil
		case 2:
			stat.WriteString(data.Template, field.Data.Size1,
				field.Data.Size2)
			return nil
		}
	}
	return e("Can't produce sql statement for data \"%v\""+
		" in notation \"%v\"", field.Data, format.Dialect)
}

func (this *createTable) getSqlFieldNullable(stat *sqlcore.Statement,
	format *sqlcore.Format, stack *sqlcore.CallStack,
	field *sqldb.FieldDef) error {
	stat.WriteString(" ")
	if field.IsNullable {
		stat.WriteString("null")
	} else {
		stat.WriteString("not null")
	}
	return nil
}

func (this *createTable) getSqlFieldDefault(stat *sqlcore.Statement,
	partKind sqlcore.SqlPartKind, subPartKind sqlcore.SqlSubPartKind,
	stack *sqlcore.CallStack, format *sqlcore.Format, field *sqldb.FieldDef) error {
	if field.Default != nil {
		stat.WriteString(" ")
		context := sqlexp.NewExprBuildContext(partKind, subPartKind, stack, format, nil)
		if field.Default.Value != nil {
			stat2, err := field.Default.Value.GetSql(context)
			if err != nil {
				return err
			}
			stat.AppendStatPartsFormat("default %s", stat2)
		} else {
			stat.WriteString("default null")
		}
	}
	return nil
}

func (this *createTable) getSqlFieldAttr(stat *sqlcore.Statement,
	format *sqlcore.Format, stack *sqlcore.CallStack,
	field *sqldb.FieldDef) error {
	if format.Dialect == sqldef.DI_MYSQL &&
		field.Data.Type.In(sqldef.DT_AUTOINC_INT|sqldef.DT_AUTOINC_INT_BIG) {
		stat.WriteString(" auto_increment")
	}
	return nil
}

type BuildSqlFieldRule struct {
	DataTypes      sqldef.DataType
	ShowNullable   bool
	CustomAttr1    string
	ShowPrimaryKey bool
}

func bsfr(dataTypes sqldef.DataType, showNullable bool, customAttr1 string,
	showPrimaryKey bool) *BuildSqlFieldRule {
	cfd := &BuildSqlFieldRule{DataTypes: dataTypes,
		ShowNullable:   showNullable,
		CustomAttr1:    customAttr1,
		ShowPrimaryKey: showPrimaryKey}
	return cfd
}

type BuildSqlFieldVarianceRule struct {
	PrimaryKeyInline bool
	Items            []*BuildSqlFieldRule
}

func bsfvr(primaryKeyInline bool, items ...*BuildSqlFieldRule) *BuildSqlFieldVarianceRule {
	btd := &BuildSqlFieldVarianceRule{PrimaryKeyInline: primaryKeyInline, Items: items}
	return btd
}

func (this *createTable) getBuildSqlFieldVarianceRule(
	dialect sqldef.Dialect) *BuildSqlFieldVarianceRule {
	tmplt := map[sqldef.Dialect]*BuildSqlFieldVarianceRule{
		sqldef.DI_MSTSQL: bsfvr(false, bsfr(sqldef.DT_ALL, true, "", false)),
		sqldef.DI_PGSQL:  bsfvr(false, bsfr(sqldef.DT_ALL, true, "", false)),
		sqldef.DI_MYSQL: bsfvr(true, bsfr(sqldef.DT_AUTOINC_INT|sqldef.DT_AUTOINC_INT_BIG,
			true, "auto_increment", true),
			bsfr(sqldef.DT_ALL, true, "", true)),
		sqldef.DI_SQLITE: bsfvr(true, bsfr(sqldef.DT_AUTOINC_INT|sqldef.DT_AUTOINC_INT_BIG,
			false, "primary key autoincrement", false),
			bsfr(sqldef.DT_ALL, true, "", true)),
	}
	if btd, ok := tmplt[dialect]; ok {
		return btd
	}
	return nil
}

func (this *createTable) getSqlField(stat *sqlcore.Statement,
	format *sqlcore.Format, stack *sqlcore.CallStack, field *sqldb.FieldDef) error {
	bsfvr := this.getBuildSqlFieldVarianceRule(format.Dialect)
	var bsfr *BuildSqlFieldRule
	if bsfvr != nil {
		for _, item := range bsfvr.Items {
			if field.Data.Type.In(item.DataTypes) {
				bsfr = item
				break
			}
		}
		if bsfvr != nil {
			stat.WriteString(format.FormatObjectName(field.Name))
			stat.WriteString(" ")
			err := this.getSqlFieldDataType(stat, format, stack, field)
			if err != nil {
				return err
			}
			if bsfr.ShowNullable {
				err = this.getSqlFieldNullable(stat, format, stack, field)
				if err != nil {
					return err
				}
			}
			err = this.getSqlFieldDefault(stat, sqlcore.SPK_CREATE_TABLE, sqlcore.SSPK_EXPR1,
				stack, format, field)
			if err != nil {
				return err
			}
			if bsfr.CustomAttr1 != "" {
				stat.WriteString(" ")
				stat.WriteString(bsfr.CustomAttr1)
			}
			if bsfvr.PrimaryKeyInline && bsfr.ShowPrimaryKey &&
				field.GetOrAdviceIsPrimaryKey() {
				stat.WriteString(" primary key")
			}
		}
	}
	return nil
}

func (this *createTable) buildCreateTableMainSql(maker *createTableMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	stat.WriteString(maker.Format.GetLeadingSpace())
	stat.WriteString("create table ")
	if maker.Format.DoIfObjectExistsNotExists() &&
		maker.Format.Dialect.In(sqldef.DI_PGSQL|sqldef.DI_MYSQL|sqldef.DI_SQLITE) {
		stat.WriteString("if not exists ")
	}
	name := maker.Format.FormatTableName(this.Table.Name /*, this.Db.Name*/)
	stat.WriteString("%s (", name)
	stat.WriteString(maker.Format.SectionDivider)
	maker.Format.IncIndentLevel()
	// add fields
	for i, field := range this.Table.Fields.Items {
		if i > 0 {
			stat.WriteString(",")
			stat.WriteString(maker.Format.SectionDivider)
		}
		stat.WriteString(maker.Format.GetLeadingSpace())
		err := this.getSqlField(stat, maker.Format, stack, field)
		if err != nil {
			maker.Format.DecIndentLevel()
			return err
		}
	}
	maker.Format.DecIndentLevel()
	return nil
}

func (this *createTable) buildPrimaryKeySql(maker *createTableMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	maker.Format.IncIndentLevel()
	bsfvr := this.getBuildSqlFieldVarianceRule(maker.Format.Dialect)
	pk := this.Table.GetOrAdvicePrimaryKey()
	if bsfvr.PrimaryKeyInline == false && len(pk.Items) > 0 {
		if maker.Format.Dialect != sqldef.DI_SQLITE {
			stat.WriteString(",")
			stat.WriteString(maker.Format.SectionDivider)
			stat.WriteString(maker.Format.GetLeadingSpace())
			stat.WriteString(f("constraint %s primary key (",
				maker.Format.FormatObjectName(pk.Name)))
			for i, field := range pk.Items {
				if i > 0 {
					stat.WriteString(", ")
				}
				stat.WriteString(f("%s",
					maker.Format.FormatObjectName(field.Name)))
			}
			stat.WriteString(")")
		}
	}
	if len(pk.Items) == 0 {
		log.Warn(f("No primary key defined or "+
			"can be adviced for table \"%s\"", this.Table.Name))
	}
	stat.WriteString(")")
	maker.Format.DecIndentLevel()
	return nil
}

func (this *createTable) buildIndexesSql(maker *createTableMaker,
	stat *sqlcore.Statement, stack *sqlcore.CallStack) error {
	if len(this.Table.Indexes.Items) > 0 {
		//        stat.WriteString(";")
		for _, index := range this.Table.Indexes.Items {
			if len(index.Items) > 0 {
				//                stat.WriteString(maker.Format.sectionDivider)
				stat.WriteString(maker.Format.GetLeadingSpace())
				stat.WriteString(f("create index %s", maker.Format.
					FormatObjectName(index.Name)))
				stat.WriteString(maker.Format.SectionDivider)
				maker.Format.IncIndentLevel()
				stat.WriteString(maker.Format.GetLeadingSpace())
				stat.WriteString(f("on %s (", maker.Format.
					FormatObjectName(this.Table.Name)))
				for i, field := range index.Items {
					if i > 0 {
						stat.WriteString(",")
					}
					stat.WriteString(f("%s", maker.Format.
						FormatObjectName(field.Name)))
				}
				stat.WriteString(")")
				maker.Format.DecIndentLevel()
			}
		}
	}
	return nil
}

func (this *createTable) preBuildCreateTableSql(maker *createTableMaker,
	stack *sqlcore.CallStack) error {
	stat := maker.Batch.Last()
	// build create statement itself
	err := this.buildCreateTableMainSql(maker, stat, stack)
	if err != nil {
		return err
	}
	// add primary key
	err = this.buildPrimaryKeySql(maker, stat, stack)
	if err != nil {
		return err
	}
	// add indexes
	var stat2 *sqlcore.Statement
	if len(this.Table.Indexes.Items) > 0 {
		stat2 = sqlcore.NewStatement(sqlcore.SS_EXEC)
		err = this.buildIndexesSql(maker, stat2, stack)
		if err != nil {
			return err
		}
		maker.Batch.Add(stat2)
	}
	// join statement if necessary
	err = maker.Batch.Join(maker.Format)
	return err
}

func (this *createTable) preBuildCreateTableSql2(maker *createTableMaker,
	stack *sqlcore.CallStack) error {
	if maker.Format.Dialect == sqldef.DI_MSTSQL {
		maker.Format.IncIndentLevel()
		defer maker.Format.DecIndentLevel()
	}
	return this.preBuildCreateTableSql(maker, stack)
}

func (this *createTable) buildCreateTableSql(maker *createTableMaker,
	stack *sqlcore.CallStack) error {
	err := this.preBuildCreateTableSql2(maker, stack)
	if err != nil {
		return err
	}
	if maker.Format.DoIfObjectExistsNotExists() &&
		maker.Format.Dialect == sqldef.DI_MSTSQL && len(maker.Batch.Items) == 1 {
		stat := maker.Batch.Last()
		newstat, err := ifExistsNotExistsBlockMicrosoftCase(this,
			stat, maker.Format, stack)
		if err != nil {
			return err
		}
		maker.Batch.Replace(stat, newstat)
	}
	return nil
}

func (this *createTable) GetSql(format *sqlcore.Format) (*sqlcore.StatementBatch, error) {
	maker := &createTableMaker{}
	err := maker.BuildSql(this, format)
	if err != nil {
		return nil, err
	}
	return maker.Batch, nil
}

func (this *createTable) GetPartKind() sqlcore.SqlPartKind {
	return sqlcore.SPK_CREATE_TABLE
}

func (this *createTable) GetParent() sqlcore.SqlPart {
	return nil
}
