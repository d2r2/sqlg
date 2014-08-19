package sqlg

type SectionKind int

const (
    SK_UNDEF SectionKind = 0
    // select statement sections
    SK_SELECT = 1 << iota
    SK_SELECT_FROM_OR_JOIN
    SK_SELECT_WHERE
    SK_SELECT_GROUP_BY
    SK_SELECT_ORDER_BY
    // insert statement sections
    SK_INSERT
    SK_INSERT_VALUES
    SK_INSERT_RETURNING
    SK_INSERT_FROM
    // update statement sections
    SK_UPDATE
    SK_UPDATE_FROM_OR_JOIN
    SK_UPDATE_WHERE
    // delete statement sections
    SK_DELETE
    SK_DELETE_WHERE
    // create table sections
    SK_CREATE_TABLE
    // create database sections
    SK_CREATE_DATABASE
    // drop table sections
    SK_DROP_TABLE
    // drop database sections
    SK_DROP_DATABASE
    // any
    SK_ANY = SK_SELECT | SK_SELECT_FROM_OR_JOIN |
        SK_SELECT_GROUP_BY | SK_SELECT_ORDER_BY |
        SK_SELECT_WHERE |
        SK_INSERT | SK_INSERT_FROM |
        SK_INSERT_RETURNING | SK_INSERT_VALUES |
        SK_UPDATE | SK_UPDATE_FROM_OR_JOIN |
        SK_UPDATE_WHERE |
        SK_DELETE | SK_DELETE_WHERE |
        SK_CREATE_DATABASE | SK_CREATE_TABLE |
        SK_DROP_DATABASE | SK_DROP_TABLE
)

func (this SectionKind) String() string {
    strs := map[SectionKind]string{
        SK_UNDEF:               "unknown sql section",
        SK_SELECT:              "SELECT <...>",
        SK_SELECT_FROM_OR_JOIN: "select FROM JOIN on <...>",
        SK_SELECT_WHERE:        "select from WHERE <...>",
        SK_SELECT_GROUP_BY:     "select from GROUP BY <...>",
        SK_SELECT_ORDER_BY:     "select from ORDER BY <....",
        SK_INSERT:              "INSERT INTO <...>",
        SK_INSERT_VALUES:       "insert into VALUES <....",
        SK_INSERT_RETURNING:    "insert into values RETURNING <...>",
        SK_INSERT_FROM:         "insert into FROM <....",
        SK_UPDATE:              "UPDATE <...>",
        SK_UPDATE_FROM_OR_JOIN: "update FROM JOIN on <...>",
        SK_UPDATE_WHERE:        "update WHERE [...]",
        SK_DELETE:              "DELETE [...]",
        SK_DELETE_WHERE:        "delete WHERE [...]",
        SK_CREATE_DATABASE:     "CREATE DATABASE [...]",
        SK_CREATE_TABLE:        "CREATE TABLE [...]",
        SK_DROP_DATABASE:       "DROP DATABASE [...]",
        SK_DROP_TABLE:          "DROP TABLE [...]",
    }
    return strs[this]
}

func (this SectionKind) In(sections SectionKind) bool {
    return this&sections != SK_UNDEF
}

type SubsectionKind int

const (
    SSK_UNDEF SubsectionKind = 0
    SSK_EXPR1                = 1 << iota
    SSK_EXPR2
    SSK_EXPR3
    SSK_ANY = SSK_EXPR1 | SSK_EXPR2 | SSK_EXPR3
)

// Concept of Section imply that each sql statment could be splited
// by peaces. For instance, complex select statement might looks like:
// select -> from -> join -> where -> groupBy -> orderBy
// So, in this scheme, each part correspond to Section interface.
// Moreover, section has a reference to parent, so using this link
// we can run from tail to head generating text representation of statement
type Section interface {
    GetSectionKind() SectionKind
    GetParent() Section
}

func getSectionRoot(section Section) Section {
    p := section.GetParent()
    if p != nil {
        return getSectionRoot(p)
    } else {
        return section
    }
}

type ProcessSection func(direct bool, section Section,
    stack *CallStack) error

func iterateSqlParents(onlyForward bool, section Section,
    process ProcessSection) error {
    stack := NewCallStack()
    err := iterSqlParent(onlyForward, section, process, stack)
    return err
}

func iterSqlParent(onlyForward bool, section Section,
    process ProcessSection, stack *CallStack) error {
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
