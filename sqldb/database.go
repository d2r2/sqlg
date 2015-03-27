package sqldb

import (
	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldef"
	"github.com/d2r2/sqlg/sqlexp"
)

type DefaultDef struct {
	On    bool
	Value sqlexp.Expr
}

type FieldDef struct {
	Name         string
	Data         *sqldef.DataDef
	IsNullable   bool
	Default      *DefaultDef
	IsPrimaryKey bool
}

func NewFieldDef(name string, data *sqldef.DataDef) *FieldDef {
	this := &FieldDef{Name: name, Data: data,
		IsNullable: true, IsPrimaryKey: false}
	return this
}

func (this *FieldDef) GetName() string {
	return this.Name
}

func (this *FieldDef) DefaultValue(value interface{}) *FieldDef {
	var expr sqlexp.Expr
	switch value.(type) {
	case sqlexp.Expr:
		expr = value.(sqlexp.Expr)
	case nil:
		expr = nil
	default:
		expr = &sqlexp.TokenValue{Value: value}
	}
	this.Default = &DefaultDef{On: true, Value: expr}
	return this
}

func (this *FieldDef) NotNull() *FieldDef {
	this.IsNullable = false
	return this
}

func (this *FieldDef) Null() *FieldDef {
	this.IsNullable = true
	return this
}

func (this *FieldDef) PrimaryKey() *FieldDef {
	this.IsPrimaryKey = true
	return this
}

func (this *FieldDef) GetOrAdviceIsPrimaryKey() bool {
	return this.IsPrimaryKey ||
		this.Data.Type.In(sqldef.DT_AUTOINC_INT|sqldef.DT_AUTOINC_INT_BIG)
}

type FieldsDef struct {
	Items []*FieldDef
}

func (this *FieldsDef) AddField(name string, fieldType sqldef.DataType,
	size1 int, size2 int) *FieldDef {
	d := sqldef.NewDataDef(fieldType, size1, size2)
	f := NewFieldDef(name, d)
	this.Items = append(this.Items, f)
	return f
}

func (this *FieldsDef) AddUnicodeFixed(name string,
	size int) *FieldDef {
	return this.AddField(name, sqldef.DT_UNICODE_CHAR, size, 0)
}

func (this *FieldsDef) AddUnicodeVariable(name string,
	size int) *FieldDef {
	return this.AddField(name, sqldef.DT_UNICODE_VARCHAR, size, 0)
}

func (this *FieldsDef) AddInt(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_INT, 0, 0)
}

func (this *FieldsDef) AddAutoinc(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_AUTOINC_INT, 0, 0).NotNull()
}

func (this *FieldsDef) AddFloat(name string,
	precision int) *FieldDef {
	return this.AddField(name, sqldef.DT_FLOAT, precision, 0)
}

func (this *FieldsDef) AddReal(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_REAL, 0, 0)
}

func (this *FieldsDef) AddDouble(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_DOUBLE, 0, 0)
}

func (this *FieldsDef) AddNumeric(name string,
	precision, scale int) *FieldDef {
	return this.AddField(name, sqldef.DT_NUMERIC, precision, scale)
}

func (this *FieldsDef) AddDecimal(name string,
	precision, scale int) *FieldDef {
	return this.AddField(name, sqldef.DT_DECIMAL, precision, scale)
}

/*  Don't work correctly with TIME data time in UnixOBDC.
    Don't allow select data producing error message:
    unsupported column type 92
*/
func (this *FieldsDef) AddTime(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_TIME, 0, 0)
}

func (this *FieldsDef) AddDate(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_DATE, 0, 0)
}

func (this *FieldsDef) AddDateTime(name string) *FieldDef {
	return this.AddField(name, sqldef.DT_DATETIME, 0, 0)
}

func (this *FieldsDef) Find(name string) *FieldDef {
	for _, field := range this.Items {
		if field.Name == name {
			return field
		}
	}
	return nil
}

type FieldCollection struct {
	Name  string
	Items []*FieldDef
}

func (this *FieldCollection) AddField(field *FieldDef) {
	this.Items = append(this.Items, field)
}

type IndexesDef struct {
	Items []*FieldCollection
}

func (id *IndexesDef) AddIndex(name string,
	fields ...*FieldDef) *FieldCollection {
	fc := &FieldCollection{Name: name, Items: fields}
	id.Items = append(id.Items, fc)
	return fc
}

// TODO implement Indexes
type TableDef struct {
	Name           string
	Fields         FieldsDef
	PrimaryKeyName string
	Indexes        IndexesDef
}

func Table(name string) *TableDef {
	t := &TableDef{Name: name}
	return t
}

func (this *TableDef) IsTableBased() (bool, sqlcore.Table) {
	return true, this
}

func (this *TableDef) GetColumnCount() (int, error) {
	return len(this.Fields.Items), nil
}

func (this *TableDef) GetFields() []sqlcore.Field {
	var fields []sqlcore.Field
	for _, f := range this.Fields.Items {
		fields = append(fields, f)
	}
	return fields
}

func (this *TableDef) ColumnIsAmbiguous(name string) (bool, error) {
	found := false
	for _, field := range this.Fields.Items {
		if found {
			if field.Name == name {
				return true, nil
			}
		} else if field.Name == name {
			found = true
		}
	}
	return false, nil
}

func (this *TableDef) ColumnExists(name string) (bool, error) {
	for _, field := range this.Fields.Items {
		if field.Name == name {
			return true, nil
		}
	}
	return false, nil
}

func (this *TableDef) GetName() string {
	return this.Name
}

func (this *TableDef) GetOrAdvicePrimaryKey() *FieldCollection {
	fc := &FieldCollection{}
	// if primary key defined
	var prKeys = []*FieldDef{}
	for _, field := range this.Fields.Items {
		if field.IsPrimaryKey {
			prKeys = append(prKeys, field)
		}
	}
	if len(prKeys) != 0 {
		fc.Items = prKeys
		if this.PrimaryKeyName == "" {
			fc.Name = f("PK_%s", this.Name)
		} else {
			fc.Name = this.PrimaryKeyName
		}
	} else {
		for _, field := range this.Fields.Items {
			if field.GetOrAdviceIsPrimaryKey() {
				fc.AddField(field)
			}
		}
		fc.Name = f("PK_%s", this.Name)
	}
	return fc
}

/*
type TablesDef struct {
    //    Db    *DatabaseDef
    Items []*TableDef
}

func (this *TablesDef) Add(name string) *TableDef {
    t := &TableDef{ Db: this.Db,  Name: name}
    this.Items = append(this.Items, t)
    return t
}

func (this *TablesDef) Find(name string) *TableDef {
    for _, item := range this.Items {
        if item.Name == name {
            return item
        }
    }
    return nil
}


type DatabaseDef struct {
    Name   string
    Tables TablesDef
}

func NewDatabase(name string) *DatabaseDef {
    db := &DatabaseDef{Name: name}
    db.Tables.Db = db
    return db
}

func (this *DatabaseDef) Select(fields ...sqlexp.Expr) *SelectRoot {
    sel := NewSelectRoot(fields...)
    return sel
}

func (this *DatabaseDef) Insert(table Query, fields ...*TokenField) *InsertRoot {
    ins := NewInsertRoot(table, fields...)
    return ins
}

func (this *DatabaseDef) Update(table Query, firstField *TokenFieldSet,
    restFields ...*TokenFieldSet) *UpdateRoot {
    fields := []*TokenFieldSet{firstField}
    fields = append(fields, restFields...)
    upd := NewUpdateRoot(this, table, fields...)
    return upd
}

func (this *DatabaseDef) Delete(table Query) *DeleteRoot {
    del := NewDeleteRoot(table)
    return del
}

func (this *DatabaseDef) CreateDatabase() *CreateDatabaseRoot {
    create := NewCreateDatabaseRoot(this)
    return create
}

func (this *DatabaseDef) CreateTable(table *TableDef) *CreateTableRoot {
    create := NewCreateTableRoot(table, this)
    return create
}

func (this *DatabaseDef) DropDatabase() *DropDatabaseRoot {
    drop := NewDropDatabaseRoot(this)
    return drop
}

func (this *DatabaseDef) DropTable(table *TableDef) *DropTableRoot {
    r := NewDropTableRoot(table, this)
    return r
}

func (this *DatabaseDef) sqlexp.ExprFactory() *sqlexp.ExprFactory {
    return Newsqlexp.ExprFactory()
}
*/
