package sqlcore

import (
	"database/sql"

	"github.com/d2r2/sqlg/sqldef"
)

// TODO put description of each interface
// TODO to each structure implement description if interface used in

type Table interface {
	GetName() string
	GetFields() []Field
}

type Field interface {
	GetName() string
}

type Query interface {
	IsTableBased() (ok bool, table Table)
	GetColumnCount() (count int, err error)
	ColumnIsAmbiguous(name string) (ok bool, err error)
	ColumnExists(name string) (ok bool, err error)
}

type QueryAlias interface {
	Query
	GetSource() Query
	GetAlias() string
}

type SqlComplete interface {
	GetSql(format *Format) (sql *StatementBatch, err error)
}

type JoinKind int

const (
	JK_INNER JoinKind = iota
	JK_LEFT
	JK_RIGHT
)

type ConnInit interface {
	Open(dialect sqldef.Dialect, dbName *string) (*sql.DB, error)
}
