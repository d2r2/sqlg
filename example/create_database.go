package main

import (
	_ "code.google.com/p/odbc"
	"database/sql"
	//    _ "github.com/mattn/go-sqlite3"
	"fmt"
	"github.com/d2r2/sqlg"
	_ "github.com/mxk/go-sqlite/sqlite3"
	"log"
	"time"
)

type DbConnSpec struct {
	Server   string
	Port     int
	Database string
	User     string
	Password string
}

func getPostgreSqlConnSettings(conn *DbConnSpec) (string, string) {
	driverName := "odbc"
	dataSource := fmt.Sprintf("driver=postgresql;server=%s;database=%s;"+
		"sslmode=disable;uid=%s", conn.Server, conn.Database,
		conn.User)
	return driverName, dataSource
}

func getMySqlConnSettings(conn *DbConnSpec) (string, string) {
	driverName := "odbc"
	dataSource := fmt.Sprintf("driver=mysql;server=%s;database=%s;"+
		"sslmode=disable;uid=%s", conn.Server, conn.Database,
		conn.User)
	return driverName, dataSource
}

func getMicrosoftSqlConnSettings(conn *DbConnSpec) (string, string) {
	driverName := "odbc"
	dataSource := fmt.Sprintf("driver=freetds;server=%s;port=%d;"+
		"database=%s;uid=%s", conn.Server, conn.Port,
		conn.Database, conn.User)
	if conn.Password != "" {
		dataSource += fmt.Sprintf(";pwd=%s", conn.Password)
	}
	return driverName, dataSource
}

func getSqliteConnSettings(conn *DbConnSpec) (string, string) {
	driverName := "sqlite3"
	dataSource := fmt.Sprintf("%s", conn.Database)
	return driverName, dataSource
}

func openConnection(dialect sqlg.Dialect, dbname *string) (*sql.DB, error) {
	var driverName, dataSource string
	switch dialect {
	case sqlg.DI_PGSQL:
		driverName, dataSource = getPostgreSqlConnSettings(&DbConnSpec{
			Server: "127.0.0.1", Database: *dbname, User: "ddyakov"})
	case sqlg.DI_MSTSQL:
		driverName, dataSource = getMicrosoftSqlConnSettings(&DbConnSpec{
			Server: "wakobpfin11", Port: 1433, Database: *dbname,
			User: "sa", Password: "Quinta-45"})
	case sqlg.DI_MYSQL:
		driverName, dataSource = getMySqlConnSettings(&DbConnSpec{
			Server: "127.0.0.1", Database: *dbname, User: "root"})
	case sqlg.DI_SQLITE:
		driverName, dataSource = getSqliteConnSettings(&DbConnSpec{
			Database: "./foo.db"})
	default:
		return nil, fmt.Errorf("Can't find connection specification for %v", dialect)
	}
	conn, err := sql.Open(driverName, dataSource)
	return conn, err
}

func createDatabase(db *sql.DB, dbName string, format *sqlg.Format) error {
	batch, err := sqlg.Predefined.CheckIfDatabaseExists(
		format.Dialect, dbName)
	if err != nil {
		return err
	}
	row, err := batch.ExecQueryRow(db)
	if err != nil {
		return err
	}
	var dbcount int
	err = row.Scan(&dbcount)
	if err != nil {
		return err
	}
	if dbcount == 0 {
		ef := sqlg.NewExprFactory()
		batch, err = ef.CreateDatabase(dbName).GetSql(format)
		if err != nil {
			return err
		}
		_, err := batch.Exec(db)
		if err != nil {
			return err
		}
		err = nil
	}
	return err
}

func constructTables() map[string]*sqlg.TableDef {
	tables := make(map[string]*sqlg.TableDef)
	ef := sqlg.NewExprFactory()
	// build table
	name := "Customers"
	custs := ef.Table(name)
	/*fldId := */ custs.Fields.AddAutoinc("Id")
	custs.Fields.AddUnicodeVariable("FirstName", 50).NotNull()
	custs.Fields.AddUnicodeVariable("LastName", 50).NotNull()
	custs.Fields.AddDate("BirthDate").NotNull().
		DefaultValue(time.Date(1974, 10, 15, 0, 0, 0, 0, time.UTC))
	custs.Fields.AddDateTime("ReferenceDate").DefaultValue(ef.CurrentDateTime())
	custs.Indexes.AddIndex("IX_1", custs.Fields.Find("LastName"))
	//    cust.PrimaryKey.Name = "PK_1"
	//    custs.PrimaryKey.AddField(fldId)
	tables[name] = custs
	name = "Products"
	prods := ef.Table(name)
	/*fldId = */ prods.Fields.AddAutoinc("Id")
	prods.Fields.AddUnicodeVariable("Descr", 100).NotNull()
	//    prods.Fields.AddNumeric("Price", false, 18, 2)
	prods.Fields.AddReal("Price").NotNull().DefaultValue(0)
	prods.Fields.AddDateTime("ReferenceDate").DefaultValue(ef.CurrentDateTime())
	//    prods.PrimaryKey.AddField(fldId)
	tables[name] = prods
	name = "Orders"
	ords := ef.Table(name)
	/*fldId = */ ords.Fields.AddAutoinc("Id")
	ords.Fields.AddDate("OrderDate").NotNull()
	ords.Fields.AddInt("CustId").NotNull()
	ords.Fields.AddUnicodeVariable("Descr", 100)
	//    ords.PrimaryKey.AddField(fldId)
	tables[name] = ords
	name = "OrderDetails"
	dtls := ef.Table("OrderDetails")
	/*fldId := */ dtls.Fields.AddAutoinc("Id")
	dtls.Fields.AddInt("OrderId").NotNull()
	dtls.Fields.AddUnicodeVariable("ItemName", 50).NotNull()
	dtls.Fields.AddInt("Quantity").NotNull()
	//    dtls.PrimaryKey.AddField(fldId)
	tables[name] = dtls
	return tables
}

/*
func getTables(dbDef *sqlg.DatabaseDef) (*sqlg.TableDef,
    *sqlg.TableDef, *sqlg.TableDef, *sqlg.TableDef, error) {
    custs, err := dbDef.GetTable("Customers")
    if err != nil {
        return nil, nil, nil, nil, err
    }
    prods, err := dbDef.GetTable("Products")
    if err != nil {
        return nil, nil, nil, nil, err
    }
    ords, err := dbDef.GetTable("Orders")
    if err != nil {
        return nil, nil, nil, nil, err
    }
    dtls, err := dbDef.GetTable("OrderDetails")
    if err != nil {
        return nil, nil, nil, nil, err
    }
    return custs, prods, ords, dtls, nil
}
*/
func dropTables(db *sql.DB, format *sqlg.Format,
	tables ...*sqlg.TableDef) error {
	ef := sqlg.NewExprFactory()
	for _, t := range tables {
		batch, err := ef.DropTable(t).GetSql(format)
		if err != nil {
			return err
		}
		_, err = batch.Exec(db)
		if err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

func createTables(db *sql.DB, format *sqlg.Format,
	tables ...*sqlg.TableDef) error {
	ef := sqlg.NewExprFactory()
	for _, t := range tables {
		batch, err := ef.CreateTable(t).GetSql(format)
		if err != nil {
			return err
		}
		_, err = batch.Exec(db)
		if err != nil {
			return err
		}
	}
	return nil
}

func insertCustomers(db *sql.DB, format *sqlg.Format,
	custs *sqlg.TableDef, firstName, lastName string) (int, error) {
	ef := sqlg.NewExprFactory()
	// fill Customers
	i1 := ef.Insert(custs,
		ef.Field(custs, "FirstName"),
		ef.Field(custs, "LastName") /*,ef.Field(custs, "ReferenceDate")*/).
		Values(ef.Value(firstName),
		ef.Value(lastName) /*ef.CurrentDateTime()*/).
		Returning(ef.Field(custs, "Id"))
	batch, err := i1.GetSql(format)
	row, err := batch.ExecQueryRow(db)
	if err != nil {
		return 0, err
	}
	var id int
	err = row.Scan(&id)
	if err != nil {
		return 0, err
	}
	fmt.Println(fmt.Sprintf("Customer is = %d", id))
	return id, nil
}

func insertProducts(db *sql.DB, format *sqlg.Format,
	prods *sqlg.TableDef, descr string, price float32) (int, error) {
	ef := sqlg.NewExprFactory()
	// fill Products
	i1 := ef.Insert(prods,
		ef.Field(prods, "Descr"), ef.Field(prods, "Price")).
		Values(ef.Value(descr), ef.Value(price)).
		Returning(ef.Field(prods, "Id"))
	batch, err := i1.GetSql(format)
	row, err := batch.ExecQueryRow(db)
	if err != nil {
		return 0, err
	}
	var id int
	err = row.Scan(&id)
	if err != nil {
		return 0, err
	}
	fmt.Println(fmt.Sprintf("Product is = %d", id))
	return id, nil
}

type CustInfo struct {
	FirstName string
	LastName  string
}

type ProductInfo struct {
	Descr string
	Price float32
}

func insertData(db *sql.DB, format *sqlg.Format,
	custs, prods, ords, dtls *sqlg.TableDef) error {
	var custInfo = []CustInfo{
		{FirstName: "Елена", LastName: "Макарова"},
		{FirstName: "John", LastName: "Doe"},
		{FirstName: "Жанар", LastName: "Уалиева"},
		{FirstName: "Karen", LastName: "Wolfe"},
		{FirstName: "Jacob", LastName: "Pratt"},
		{FirstName: "Yang", LastName: "Wang"},
		{FirstName: "Pedro", LastName: "Afonso"},
		{FirstName: "Elizabeth", LastName: "Brown"},
	}
	for _, item := range custInfo {
		_, err := insertCustomers(db, format, custs,
			item.FirstName, item.LastName)
		if err != nil {
			return err
		}
	}
	var prodInfo = []ProductInfo{
		{"Tofu", 23.25},
		{"Boston Crab Meat", 18.4},
		{Descr: "Jack's New England Clam Chowder", Price: 9.65},
		{Descr: "Chocolade", Price: 12.75},
		{Descr: "Filo Mix", Price: 7},
		{Descr: "Tourtiere", Price: 7.45},
		{Descr: "Ravioli Angelo", Price: 19.5},
	}
	for _, item := range prodInfo {
		_, err := insertProducts(db, format, prods, item.Descr, item.Price)
		if err != nil {
			return err
		}
	}
	return nil
}

func createAndFillDatabase(dialect sqlg.Dialect) error {
	var db *sql.DB
	var err error
	format := sqlg.NewFormat(dialect)
	format.AddOptions( /*SBO_USE_DATABASE_NAME|*/
		sqlg.BO_USE_SCHEMA_NAME | sqlg.BO_DO_IF_OBJECT_EXISTS_NOT_EXISTS)
	dbName := "Test123"
	tables := constructTables()
	if dialect.SupportMultipleDatabases() {
		dbname := dialect.GetSystemDatabase()
		db, err = openConnection(dialect, dbname)
		err = createTables(db, dbName, format)
		db.Close()
		if err != nil {
			return err
		}
		db, err = openConnection(dialect, &dbName)
	} else {
		db, err = openConnection(dialect, nil)
	}
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return err
	}
	custs, prods, ords, dtls, err := getTables(dbDef)
	if err != nil {
		return err
	}
	/*sql, args, err := dbDef.CreateDatabase().GetSql(format)
	  log.Info(sql)
	  log.Info(args)
	  _, err = db.Exec(sql, args...)
	  if err != nil {
	      t.Fatal(err)
	  }*/
	err = dropTables(db, format, custs, prods, ords, dtls)
	if err != nil {
		return err
	}
	err = createTables(db, format, custs, prods, ords, dtls)
	if err != nil {
		return err
	}
	err = insertData(db, format, custs, prods, ords, dtls)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	//var dialect Dialect = DI_MSTSQL
	var dialect sqlg.Dialect = sqlg.DI_PGSQL
	//var dialect Dialect = DI_MYSQL
	//var dialect Dialect = DI_SQLITE
	err := createAndFillDatabase(dialect)
	if err != nil {
		log.Fatal(err)
	}
}
