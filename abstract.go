package sqlg

import (
	"database/sql"
)

// TODO put description of each interface
// TODO to each structure implement description if interface used in

type SqlReady interface {
	GetSql(format *Format) (sql *StatementBatch, err error)
}

type Query interface {
	IsTableBased() (ok bool, table *TableDef)
	GetColumnCount() (count int, err error)
	ColumnIsAmbiguous(name string) (ok bool, err error)
	ColumnExists(name string) (ok bool, err error)
}

type QueryAlias interface {
	Query
	GetSource() Query
	GetAlias() string
}

type Expr interface {
	GetSql(context *ExprBuildContext) (*Statement, error)
	// TODO change return type to []ExprField?
	CollectFields() []*TokenField
	CheckContext(sectionType SectionKind,
		subsectionType SubsectionKind,
		stack *CallStack) bool
}

type ExprNamed interface {
	GetFieldAliasOrName() string
}

type JoinKind int

const (
	JK_INNER JoinKind = iota
	JK_LEFT
	JK_RIGHT
)

type ConnInit interface {
	Open(dialect Dialect, dbName *string) (*sql.DB, error)
}
