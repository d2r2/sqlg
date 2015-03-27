package sqlcore

type SqlPartKind int

const (
	SPK_UNDEF SqlPartKind = 0
	// select statement sections
	SPK_SELECT = 1 << iota
	SPK_SELECT_FROM_OR_JOIN
	SPK_SELECT_WHERE
	SPK_SELECT_GROUP_BY
	SPK_SELECT_ORDER_BY
	// insert statement sections
	SPK_INSERT
	SPK_INSERT_VALUES
	SPK_INSERT_RETURNING
	SPK_INSERT_FROM
	// update statement sections
	SPK_UPDATE
	SPK_UPDATE_FROM_OR_JOIN
	SPK_UPDATE_WHERE
	// delete statement sections
	SPK_DELETE
	SPK_DELETE_WHERE
	// create table sections
	SPK_CREATE_TABLE
	// create database sections
	SPK_CREATE_DATABASE
	// drop table sections
	SPK_DROP_TABLE
	// drop database sections
	SPK_DROP_DATABASE
	// any
	SPK_ANY = SPK_SELECT | SPK_SELECT_FROM_OR_JOIN |
		SPK_SELECT_GROUP_BY | SPK_SELECT_ORDER_BY |
		SPK_SELECT_WHERE |
		SPK_INSERT | SPK_INSERT_FROM |
		SPK_INSERT_RETURNING | SPK_INSERT_VALUES |
		SPK_UPDATE | SPK_UPDATE_FROM_OR_JOIN |
		SPK_UPDATE_WHERE |
		SPK_DELETE | SPK_DELETE_WHERE |
		SPK_CREATE_DATABASE | SPK_CREATE_TABLE |
		SPK_DROP_DATABASE | SPK_DROP_TABLE
)

func (this SqlPartKind) String() string {
	strs := map[SqlPartKind]string{
		SPK_UNDEF:               "unknown sql section",
		SPK_SELECT:              "SELECT <...>",
		SPK_SELECT_FROM_OR_JOIN: "select FROM JOIN on <...>",
		SPK_SELECT_WHERE:        "select from WHERE <...>",
		SPK_SELECT_GROUP_BY:     "select from GROUP BY <...>",
		SPK_SELECT_ORDER_BY:     "select from ORDER BY <....",
		SPK_INSERT:              "INSERT INTO <...>",
		SPK_INSERT_VALUES:       "insert into VALUES <....",
		SPK_INSERT_RETURNING:    "insert into values RETURNING <...>",
		SPK_INSERT_FROM:         "insert into FROM <....",
		SPK_UPDATE:              "UPDATE <...>",
		SPK_UPDATE_FROM_OR_JOIN: "update FROM JOIN on <...>",
		SPK_UPDATE_WHERE:        "update WHERE [...]",
		SPK_DELETE:              "DELETE [...]",
		SPK_DELETE_WHERE:        "delete WHERE [...]",
		SPK_CREATE_DATABASE:     "CREATE DATABASE [...]",
		SPK_CREATE_TABLE:        "CREATE TABLE [...]",
		SPK_DROP_DATABASE:       "DROP DATABASE [...]",
		SPK_DROP_TABLE:          "DROP TABLE [...]",
	}
	return strs[this]
}

func (this SqlPartKind) In(sections SqlPartKind) bool {
	return this&sections != SPK_UNDEF
}

type SqlSubPartKind int

const (
	SSPK_UNDEF SqlSubPartKind = 0
	SSPK_EXPR1                = 1 << iota
	SSPK_EXPR2
	SSPK_EXPR3
	SSPK_ANY = SSPK_EXPR1 | SSPK_EXPR2 | SSPK_EXPR3
)

// Concept of SqlPart imply that each sql statment could be splited
// by peaces. For instance, complex select statement might looks like:
// select -> from -> join -> where -> groupBy -> orderBy
// So, in this scheme, each part correspond to SqlPart interface.
// Moreover, section has a reference to parent, so using this link
// we can run from tail to head generating text representation of statement
type SqlPart interface {
	GetPartKind() SqlPartKind
	GetParent() SqlPart
}

func GetSqlPartRoot(section SqlPart) SqlPart {
	p := section.GetParent()
	if p != nil {
		return GetSqlPartRoot(p)
	} else {
		return section
	}
}

type ProcessSqlPart func(direct bool, section SqlPart,
	stack *CallStack) error

func IterateSqlParents(onlyForward bool, section SqlPart,
	process ProcessSqlPart) error {
	stack := NewCallStack()
	err := iterSqlParent(onlyForward, section, process, stack)
	return err
}

func iterSqlParent(onlyForward bool, section SqlPart,
	process ProcessSqlPart, stack *CallStack) error {
	// push stack and pop at the exit
	stack.Push(section)
	defer stack.Pop()
	// direct order call
	err := process(true, section, stack)
	if err != nil {
		return err
	}
	// parent iterate call
	parent := section.GetParent()
	if parent != nil {
		err = iterSqlParent(onlyForward, parent, process, stack)
		if err != nil {
			return err
		}
	}
	// reverse order call
	if onlyForward == false {
		err = process(false, section, stack)
	}
	return err
}
