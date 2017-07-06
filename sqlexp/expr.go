package sqlexp

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldef"
)

type Expr interface {
	GetSql(context *ExprBuildContext) (*sqlcore.Statement, error)
	// TODO change return type to []ExprField?
	CollectFields() []*TokenField
	CheckContext(partKind sqlcore.SqlPartKind,
		subpartKind sqlcore.SqlSubPartKind,
		stack *sqlcore.CallStack) bool
}

type ExprNamed interface {
	GetFieldAliasOrName() string
}

type SqlFunc int

const (
	// sql operators
	SF_UNDEF      SqlFunc = 0
	SF_CUSTOMFUNC         = 1 << iota
	// logycal operations
	SF_EQUAL       //  expr1 = expr2
	SF_NOT_EQ      //  expr1 <> expr2
	SF_LESS        //  expr1 < expr2
	SF_LESS_EQ     //  expr1 <= expr2
	SF_GREAT       //  expr1 > expr2
	SF_GREAT_EQ    //  expr1 >= expr2
	SF_LIKE        //  expr1 like expr2
	SF_IN          //  expr0 in (expr1, expr2, ... exprN)
	SF_NOT_IN      //  expr0 not in (expr1, expr2, ... exprN)
	SF_BEETWEN     //  expr0 between (expr1, expr2)
	SF_AND         //  expr1 and expr2
	SF_OR          //  expr1 or expr2
	SF_IS_NULL     //  expr1 is null
	SF_IS_NOT_NULL //  expr1 is not null
	// ariphmetic operations
	SF_ADD  //  addition
	SF_SUBT //  subtraction
	SF_MULT //  multiplication
	SF_DIV  //  division
	// special logycal operation
	SF_CASE_THEN_ELSE //  case when expr0 then expr1 else expr2 end
	SF_COALESCE       // strict analog of sql coalesce function
	// aggregate functions
	SF_AGR_SUM   //  sum(op1)
	SF_AGR_MIN   //  min(op1)
	SF_AGR_MAX   //  max(op1)
	SF_AGR_AVG   //  avg(op1)
	SF_AGR_COUNT //  count(op1)
	// group by asc, desc hints
	SF_ORD_ASC  //  field1 asc
	SF_ORD_DESC //  field1 desc
	// sql specific: date functions
	SF_CURDATE
	SF_CURTIME
	SF_CURDATETIME
	// sql specific: string functions
	SF_TRIMSPACE
	SF_LTRIMSPACE
	SF_RTRIMSPACE
)

func (sf SqlFunc) String() string {
	fnc := map[SqlFunc]string{
		SF_UNDEF:    "undefined function",
		SF_EQUAL:    "op1 = op2",
		SF_NOT_EQ:   "op1 <> op2",
		SF_LESS:     "op1 < op2",
		SF_LESS_EQ:  "op1 <= op2",
		SF_GREAT:    "op1 > op2",
		SF_GREAT_EQ: "op1 >= op2",
		SF_LIKE:     "op1 like op2",
		SF_IN:       "op in (op1, op2, ... opn)",
		SF_NOT_IN:   "not in (op1, op2, ... opn)",
		SF_BEETWEN:  "op1 between (op1, op2)",
		SF_AND:      "op1 and op2",
		SF_OR:       "op1 or op2",
		// aggregate functions
		SF_AGR_SUM:   "sum(op1)",
		SF_AGR_MIN:   "min(op1)",
		SF_AGR_MAX:   "max(op1)",
		SF_AGR_AVG:   "avg(op1)",
		SF_AGR_COUNT: "count(op1)",
		// group by asc, desc hints
		SF_ORD_ASC:  "expr1 asc",
		SF_ORD_DESC: "expr1 desc",
		// sql specific: date functions
		SF_CURDATE:     "get_current_date()",
		SF_CURTIME:     "get_current_time()",
		SF_CURDATETIME: "get_current_datetime()",
		// sql specific: string functions
		SF_TRIMSPACE:  "trim()",
		SF_LTRIMSPACE: "trim_left()",
		SF_RTRIMSPACE: "trim_right()",
	}
	return fnc[sf]
}

func (sf SqlFunc) In(funcs SqlFunc) bool {
	return sf&funcs != SF_UNDEF
}

type QueryEntries struct {
	Queries []sqlcore.Query
}

func NewQueryEntries() *QueryEntries {
	this := &QueryEntries{}
	return this
}

func (this *QueryEntries) AddEntry(query sqlcore.Query) {
	this.Queries = append(this.Queries, query)
}

func (this *QueryEntries) FindEntry(query sqlcore.Query) (sqlcore.Query, bool) {
	var entryFound sqlcore.Query
	ambiguous := false
	for _, entry := range this.Queries {
		queryTableBased, queryTable := query.IsTableBased()
		entryTableBased, entryTable := entry.IsTableBased()
		entryAlias, entryAliasBased := entry.(sqlcore.QueryAlias)
		if queryTableBased && entryTableBased &&
			queryTable.GetName() == entryTable.GetName() ||
			entryAliasBased && entryAlias.GetSource() == query ||
			entry == query {
			if entryFound == nil {
				entryFound = entry
			} else {
				ambiguous = true
				break
			}
		}
	}
	return entryFound, ambiguous
}

type ExprBuildContext struct {
	SqlPartKind    sqlcore.SqlPartKind
	SqlSubPartKind sqlcore.SqlSubPartKind
	Stack          *sqlcore.CallStack
	Format         *sqlcore.Format
	DataSources    *QueryEntries
}

func NewExprBuildContext(
	sectionKind sqlcore.SqlPartKind,
	subsectionKind sqlcore.SqlSubPartKind,
	stack *sqlcore.CallStack,
	format *sqlcore.Format,
	entries *QueryEntries) *ExprBuildContext {
	c := &ExprBuildContext{SqlPartKind: sectionKind,
		SqlSubPartKind: subsectionKind,
		Stack:          stack,
		Format:         format,
		DataSources:    entries}
	return c
}

func FormatPrettyDataSource(query sqlcore.Query,
	capital bool, dialect *sqldef.Dialect) (string, error) {
	var str string
	tableBased, table := query.IsTableBased()
	sqlValid, sqlValidOk := query.(sqlcore.SqlReady)
	if tableBased {
		var buf bytes.Buffer
		for i, field := range table.GetFields() {
			buf.WriteString(f("\"%s\"", field.GetName()))
			if i < len(table.GetFields())-1 {
				buf.WriteString(",")
			}
		}
		str = f("table \"%s\" with fields: %s",
			table.GetName(), buf.String())
	} else if sqlValidOk && dialect != nil {
		f2 := sqlcore.NewFormat(*dialect)
		f2.AddOptions(sqlcore.BO_INLINE)
		f2.SectionDivider = " "
		batch, err := sqlValid.GetSql(f2)
		if err != nil {
			return "", err
		}
		stat := batch.Items[0]
		str = f("\"%s\"", stat.Sql())
	} else {
		str = "query"
	}
	if capital {
		return strings.ToUpper(str[:1]) + str[1:], nil
	} else {
		return str, nil
	}
}

type TokenField struct {
	DataSource sqlcore.Query
	Name       string
}

func (this *TokenField) FindEntryAndValidate(context *ExprBuildContext) (sqlcore.Query, error) {
	objstr, err := FormatPrettyDataSource(this.DataSource,
		false, &context.Format.Dialect)
	if err != nil {
		return nil, err
	}
	entry, entryAmb := context.DataSources.FindEntry(this.DataSource)
	if entry == nil {
		str := f("Column \"%s\" is associated with %s, "+
			"which haven't been added to the statement", this.Name, objstr)
		return nil, e(str)
	}
	if entryAmb {
		return nil, e("Reference to %s is ambiguous in column \"%s\"",
			objstr, this.Name)
	}
	// perform validation of column name, if option is specified
	if context.Format.ColumnNameAndCountValidationIsOn() {
		colAmb, err := entry.ColumnIsAmbiguous(this.Name)
		if err != nil {
			return nil, err
		}
		if colAmb {
			return nil, e("Reference to column \"%s\" is ambiguous in %s",
				this.Name, objstr)
		}
		exists, err := entry.ColumnExists(this.Name)
		if err != nil {
			return nil, err
		}
		if exists == false {
			return nil, e("Can't find column \"%s\" in %s", this.Name, objstr)
		}
	}
	tableBased, _ := entry.IsTableBased()
	_, aliasBased := entry.(sqlcore.QueryAlias)
	if tableBased == false && aliasBased == false {
		objstr, err := FormatPrettyDataSource(entry,
			false, &context.Format.Dialect)
		if err != nil {
			return nil, err
		}
		return nil, e("Column \"%s\" reference to object that is not a table "+
			"and neither has alias specified: %s", this.Name, objstr)
	}
	return entry, nil
}

func (this *TokenField) GetSql(context *ExprBuildContext) (*sqlcore.Statement, error) {
	entry, err := this.FindEntryAndValidate(context)
	if err != nil {
		return nil, err
	}
	stat := sqlcore.NewStatement(sqlcore.SS_UNDEF)
	if context.SqlPartKind == sqlcore.SPK_INSERT_RETURNING {
		dialect := context.Format.Dialect
		switch dialect {
		case sqldef.DI_PGSQL:
			stat.WriteString(f("%s",
				context.Format.FormatObjectName(this.Name)))
		case sqldef.DI_MSTSQL:
			stat.WriteString(f("inserted.%s",
				context.Format.FormatObjectName(this.Name)))
		default:
			return nil, e("Can't provide field specification "+
				"for returning section in notation \"%v\"", dialect)
		}
	} else {
		tableBased, table := entry.IsTableBased()
		queryAlias, aliasBased := entry.(sqlcore.QueryAlias)
		if aliasBased {
			stat.WriteString(f("%s.%s", queryAlias.GetAlias(),
				context.Format.FormatObjectName(this.Name)))
		} else if tableBased {
			stat.WriteString(f("%s.%s",
				context.Format.FormatTableName(table.GetName()),
				context.Format.FormatObjectName(this.Name)))
		}
	}
	return stat, nil
}

func (this *TokenField) CollectFields() []*TokenField {
	return []*TokenField{this}
}

/*
func (this *TokenField) ContextFlagsAny() SqlPart {
    return SP_DELETE_WHERE | SP_INSERT_RETURNING | SP_SELECT_FIELDS |
        SP_SELECT_GROUP_BY | SP_SELECT_JOIN_COND | SP_SELECT_ORDER_BY |
        SP_SELECT_WHERE | SP_UPDATE_FIELDS | SP_UPDATE_JOIN_COND |
        SP_UPDATE_WHERE
}

func (this *TokenField) ContextFlagsEach() SqlPart {
    return SP_UNDEF
}
*/
func (this *TokenField) CheckContext(sectionKind sqlcore.SqlPartKind,
	subsectionKind sqlcore.SqlSubPartKind,
	stack *sqlcore.CallStack) bool {
	// TODO
	return true
}

func (this *TokenField) GetFieldAliasOrName() string {
	return this.Name
}

type TokenValue struct {
	Value interface{}
}

func doubleQuote(value string) string {
	var buf bytes.Buffer
	for _, ch := range value {
		buf.WriteRune(ch)
		if ch == '\'' {
			buf.WriteRune(ch)
		}
	}
	return buf.String()
}

func (this *TokenValue) formatValue(context *ExprBuildContext,
	stat *sqlcore.Statement, value interface{}) {
	if context.Format.Inline() {
		stat.WriteString("%v", value)
	} else if !context.Format.OdbcMode() && context.Format.Dialect == sqldef.DI_PGSQL {
		context.Format.IncParamIndex()
		stat.WriteString(f("$%d", context.Format.GetParamIndex()))
		stat.AppendArg(value)
	} else {
		stat.WriteString("?")
		stat.AppendArg(value)
	}
}

func (this *TokenValue) formatTimeDuration(context *ExprBuildContext,
	stat *sqlcore.Statement) {
	const DURATION_FORMAT = "15:04:05.0000000"
	d := this.Value.(time.Duration)
	t := time.Time{}
	t = t.Add(d)
	if context.Format.Inline() {
		this.formatValue(context, stat, f("'%s'", t.Format(DURATION_FORMAT)))
	} else {
		this.formatValue(context, stat, f("%s", t.Format(DURATION_FORMAT)))
	}
}

func (this *TokenValue) formatTime(context *ExprBuildContext,
	stat *sqlcore.Statement) {
	const TIME_FORMAT = "2006-01-02T15:04:05.000"
	t := this.Value.(time.Time)
	if context.Format.Inline() {
		this.formatValue(context, stat, f("'%s'", t.Format(TIME_FORMAT)))
	} else {
		this.formatValue(context, stat, f("%s", t.Format(TIME_FORMAT)))
	}
}

func (this *TokenValue) GetSql(context *ExprBuildContext) (*sqlcore.Statement, error) {
	stat := sqlcore.NewStatement(sqlcore.SS_UNDEF)
	if context.Format.Inline() {
		switch this.Value.(type) {
		case string:
			switch context.Format.Dialect {
			case sqldef.DI_MSTSQL:
				stat.WriteString("N'%s'", doubleQuote(this.Value.(string)))
			default:
				stat.WriteString("'%s'", doubleQuote(this.Value.(string)))
			}
		case time.Time:
			switch context.Format.Dialect {
			case sqldef.DI_SQLITE:
				const TIME_FORMAT = "2006-01-02T15:04:05.000"
				tm := this.Value.(time.Time)
				stat.WriteString("'%s'", tm.Format(TIME_FORMAT))
			default:
				tm := this.Value.(time.Time)
				stat.WriteString("'%v'", tm)
			}
			//            this.formatTime(context, stat)
		case time.Duration:
			this.formatTimeDuration(context, stat)
		default:
			stat.WriteString("%v", this.Value)
		}
	} else {
		switch this.Value.(type) {
		// Found during test case, that Microsoft SQL doesn't let insert
		// time.Duration to the time(7) field, reporting that it's
		// impossible to insert int value to time(7) field.
		// So, convert here time.Duration to string representation.
		// TODO Perhaphs SQlite should be added here as well.
		case time.Duration:
			this.formatTimeDuration(context, stat)
			//        case time.Time:
			//            this.formatTime(context, stat)
		default:
			this.formatValue(context, stat, this.Value)
		}
	}
	return stat, nil
}

func (this *TokenValue) CollectFields() []*TokenField {
	return []*TokenField{}
}

func (this *TokenValue) CheckContext(sectionKind sqlcore.SqlPartKind,
	subsectionKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack) bool {
	return true
}

type CustomDialectFuncDef struct {
	Dialect sqldef.Dialect
	Func    *FuncTemplate
}

type CustomFuncDef struct {
	Funcs []*CustomDialectFuncDef
}

func NewCustomDialectFunc(dialect sqldef.Dialect, template string,
	min int, max int) *CustomDialectFuncDef {
	fnc := &CustomDialectFuncDef{Dialect: dialect,
		Func: &FuncTemplate{Template: template, ParamMin: min, ParamMax: max}}
	return fnc
}

func NewCustomFunc(funcs ...*CustomDialectFuncDef) *CustomFuncDef {
	this := &CustomFuncDef{Funcs: funcs}
	return this
}

type BuildSqlFunc struct {
	Dialects       sqldef.Dialect
	SqlPartKind    sqlcore.SqlPartKind
	SqlSubPartKind sqlcore.SqlSubPartKind
	Template       FuncTemplate
}

func bsf(dialects sqldef.Dialect, sectionKind sqlcore.SqlPartKind,
	subsectionKind sqlcore.SqlSubPartKind, template FuncTemplate) BuildSqlFunc {
	fc := BuildSqlFunc{Dialects: dialects, SqlPartKind: sectionKind,
		SqlSubPartKind: subsectionKind, Template: template}
	return fc
}

type BuildSqlFuncList struct {
	Items []BuildSqlFunc
}

func bsfl(items ...BuildSqlFunc) BuildSqlFuncList {
	this := BuildSqlFuncList{Items: items}
	return this
}

type FuncTemplate struct {
	// {} stands for all provided arguments
	// {0} {1}...{N} for specific one
	Template string
	ParamMin int
	ParamMax int
}

func ft(template string, min, max int) FuncTemplate {
	this := FuncTemplate{Template: template, ParamMin: min, ParamMax: max}
	return this
}

func (ft *FuncTemplate) GetSql(context *ExprBuildContext,
	args ...Expr) (*sqlcore.Statement, error) {
	stat := sqlcore.NewStatement(sqlcore.SS_UNDEF)
	startBrPos := -1
	endBrPos := -1
	for i, ch := range ft.Template {
		if ch == '{' {
			startBrPos = i
			continue
		} else if ch == '}' {
			endBrPos = i
			// get index
			if startBrPos >= 0 {
				strIndex := ft.Template[startBrPos+1 : endBrPos]
				if strIndex != "" {
					k, err := strconv.Atoi(strIndex)
					if err != nil {
						return nil, e("Invalid index {%s} in "+
							"expression template \"%s\"", strIndex, ft.Template)
					}
					if k >= len(args) {
						return nil, e("Index {%k} exceed argument counts for "+
							"expression template \"%s\"", k, ft.Template)
					}
					stat2, err := args[k].GetSql(context)
					if err != nil {
						return nil, err
					}
					stat.AppendStatPart(stat2)
				} else {
					c := len(args)
					if ft.ParamMin > ft.ParamMax {
						return nil, e("Minimum argument count %d can't exceed "+
							"maximum %d in expression template \"%s\"", ft.ParamMin,
							ft.ParamMax, ft.Template)
					}
					if ft.ParamMin > c {
						return nil, e("Declared minimum argument count %d exceed "+
							"provided argument count in expression template \"%s\"",
							ft.ParamMin, ft.Template)
					}
					if ft.ParamMax < c {
						c = ft.ParamMax
					}
					for j := 0; j < c; j++ {
						stat2, err := args[j].GetSql(context)
						if err != nil {
							return nil, err
						}
						stat.AppendStatPart(stat2)
						if j < len(args)-1 {
							stat.WriteString(",")
						}
					}
				}
				startBrPos = -1
				endBrPos = -1
			} else {
				return nil, e("Closing } found without opening {"+
					" in expression template \"%s\"", ft.Template)
			}
		} else if startBrPos == -1 {
			if ch == '?' {
				return nil, e("Can't use %c character in expression "+
					"template \"%s\"", ch, ft.Template)
			}
			stat.WriteRune(ch)
		}
	}
	return stat, nil
}

type TokenFunc struct {
	Func       SqlFunc
	CustomFunc *CustomFuncDef
	Args       []Expr
}

func (this *TokenFunc) getFuncTemplate(dialect sqldef.Dialect) *FuncTemplate {
	// general templates which not depend on sql dialect
	fnc := map[SqlFunc]BuildSqlFuncList{
		SF_AGR_AVG:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("avg({0})", 1, 1))),
		SF_AGR_COUNT:   bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("count({0})", 1, 1))),
		SF_AGR_MAX:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("max({0})", 1, 1))),
		SF_AGR_MIN:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("min({0})", 1, 1))),
		SF_AGR_SUM:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("sum({0})", 1, 1))),
		SF_AND:         bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} and {1}", 2, 2))),
		SF_BEETWEN:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} between({1}, {2})", 3, 3))),
		SF_EQUAL:       bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} = {1}", 2, 2))),
		SF_LESS:        bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} < {1}", 2, 2))),
		SF_LESS_EQ:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} <= {1}", 2, 2))),
		SF_LIKE:        bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} like {1}", 2, 2))),
		SF_GREAT:       bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} > {1}", 2, 2))),
		SF_GREAT_EQ:    bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} >= {1}", 2, 2))),
		SF_NOT_EQ:      bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} <> {1}", 2, 2))),
		SF_OR:          bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} or {1}", 2, 2))),
		SF_IN:          bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} in ({1})", 2, 2))),
		SF_NOT_IN:      bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} not in ({1})", 2, 2))),
		SF_IS_NULL:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} is null", 1, 1))),
		SF_IS_NOT_NULL: bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} is not null", 1, 1))),
		SF_CASE_THEN_ELSE: bsfl(
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("case when {0} then {1} else {2} end case", 3, 3)),
			bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("case when {0} then {1} else {2} end", 3, 3))),
		SF_COALESCE: bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("coalesce({})", 1, -1))),
		SF_ORD_ASC:  bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} asc", 1, 1))),
		SF_ORD_DESC: bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0} desc", 1, 1))),
		SF_ADD:      bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0}+{1}", 2, 2))),
		SF_SUBT:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0}-{1}", 2, 2))),
		SF_MULT:     bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0}*{1}", 2, 2))),
		SF_DIV:      bsfl(bsf(sqldef.DI_ANY, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("{0}/{1}", 2, 2))),
		// string functions
		SF_TRIMSPACE: bsfl(
			bsf(sqldef.DI_MSTSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("ltrim(rtrim({0}))", 1, 1)),
			bsf(sqldef.DI_PGSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("trim(both from {0})", 1, 1)),
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("___({0})", 1, 1)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("___({0})", 1, 1))),
		SF_RTRIMSPACE: bsfl(
			bsf(sqldef.DI_MSTSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("rtrim({0}))", 1, 1)),
			bsf(sqldef.DI_PGSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("trim(trailing from {0})", 1, 1)),
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("___({0})", 1, 1)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("___({0})", 1, 1))),
		SF_LTRIMSPACE: bsfl(
			bsf(sqldef.DI_MSTSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("ltrim(rtrim({0}))", 1, 1)),
			bsf(sqldef.DI_PGSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("trim(leading from {0})", 1, 1)),
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("___({0})", 1, 1)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("___({0})", 1, 1))),
		// date/time functions
		SF_CURDATE: bsfl(
			bsf(sqldef.DI_MSTSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("cust(getdate() as date)", 0, 0)),
			bsf(sqldef.DI_PGSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("current_date", 0, 0)),
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("curdate()", 0, 0)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_CREATE_TABLE, sqlcore.SSPK_ANY, ft("current_date", 0, 0)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("date('now')", 0, 0))),
		SF_CURDATETIME: bsfl(
			bsf(sqldef.DI_MSTSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("getdate()", 0, 0)),
			bsf(sqldef.DI_PGSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("current_timestamp", 0, 0)),
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("now()", 0, 0)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_CREATE_TABLE, sqlcore.SSPK_ANY, ft("current_timestamp", 0, 0)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("datetime('now')", 0, 0))),
		SF_CURTIME: bsfl(
			bsf(sqldef.DI_MSTSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("cust(getdate() as time)", 0, 0)),
			bsf(sqldef.DI_PGSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("current_time", 0, 0)),
			bsf(sqldef.DI_MYSQL, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("curtime()", 0, 0)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_CREATE_TABLE, sqlcore.SSPK_ANY, ft("current_time", 0, 0)),
			bsf(sqldef.DI_SQLITE, sqlcore.SPK_ANY, sqlcore.SSPK_ANY, ft("time('now')", 0, 0))),
	}
	/*    // Microsoft T-SQL specific templates
	      fncMicrosoftSql := map[FuncContext]FuncTemplate{
	          // date functions
	          SF_CURDATE:     FT{"cast(getdate() as date)", 0},
	          SF_CURDATETIME: FT{"getdate()", 0},
	          SF_CURTIME:     FT{"cast(getdate() as time)", 0},
	      }
	      // PostgreSQL specific templates
	      fncPostgreSql := map[SqlFunc]FT{
	          // date functions
	          SF_CURDATE:     FT{"current_date", 0},
	          SF_CURDATETIME: FT{"current_timestamp", 0},
	          SF_CURTIME:     FT{"current_time", 0},
	      }
	      // MySql specific templates
	      fncMySql := map[SqlFunc]FT{
	          // date functions
	          SF_CURDATE:     FT{"curdate()", 0},
	          SF_CURDATETIME: FT{"now()", 0},
	          SF_CURTIME:     FT{"curtime()", 0},
	      }
	      // Sqllite specific templates
	      fncSqlite := map[SqlFunc]FT{
	          // date functions
	          SF_CURDATE:     FT{"date('now')", 0},
	          SF_CURDATETIME: FT{"datetime('now')", 0},
	          SF_CURTIME:     FT{"time('now')", 0},
	      }*/
	if fcl, ok := fnc[this.Func]; ok {
		for _, fc := range fcl.Items {
			if dialect.In(fc.Dialects) {
				return &fc.Template
			}
		}
	}
	return nil
}

func (this *TokenFunc) GetSql(context *ExprBuildContext) (*sqlcore.Statement, error) {
	dialect := context.Format.Dialect
	if this.Func == SF_CUSTOMFUNC {
		for _, fnc := range this.CustomFunc.Funcs {
			if dialect.In(fnc.Dialect) {
				stat, err := fnc.Func.GetSql(context, this.Args...)
				if err != nil {
					return nil, err
				}
				return stat, nil
			}
		}
		return nil, e("Custom function is undefined for dialect \"%v\"", dialect)
	} else {
		fnc := this.getFuncTemplate(dialect)
		if fnc != nil {
			/*        if this.FlagsAny != SP_UNDEF &&
			          context.Flags&t.FlagsAny == SP_UNDEF ||
			          this.FlagsEach != SP_UNDEF &&
			              context.Flags&t.FlagsEach != this.FlagsEach {
			          return nil, e("Functon \"%s\" can't be used in \"%v\" "+
			              "without \"%v\"", this.Func, context.Flags, this.FlagsEach)
			      }*/
			stat, err := fnc.GetSql(context, this.Args...)
			if err != nil {
				return nil, err
			}
			return stat, nil
		}
	}
	return nil, e("Unknown how to process expression \"%v\" "+
		"in dialect \"%v\"", this.Func, dialect)
}

func (this *TokenFunc) CollectFields() []*TokenField {
	var fields []*TokenField
	for _, item := range this.Args {
		f := item.CollectFields()
		fields = append(fields, f...)
	}
	return fields
}

func (this *TokenFunc) CheckContext(sectionKind sqlcore.SqlPartKind,
	subsectionKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack) bool {
	switch this.Func {
	case SF_AGR_AVG, SF_AGR_COUNT, SF_AGR_MAX, SF_AGR_MIN, SF_AGR_SUM:
		if sectionKind != sqlcore.SPK_SELECT_GROUP_BY &&
			stack.First(sqlcore.SPK_SELECT_GROUP_BY) != nil {
			return true
		} else {
			return false
		}
	}
	return true
}

type TokenFieldAlias struct {
	Expr  Expr
	Alias string
}

func (this *TokenFieldAlias) GetSql(context *ExprBuildContext) (*sqlcore.Statement, error) {
	stat, err := this.Expr.GetSql(context)
	if err != nil {
		return nil, err
	}
	newst := sqlcore.NewStatement(sqlcore.SS_UNDEF)
	newst.WriteString("%s as %s", stat.Sql(), this.Alias)
	newst.AppendArgs(stat.Args)
	return newst, nil
}

func (this *TokenFieldAlias) CollectFields() []*TokenField {
	return this.Expr.CollectFields()
}

/*
func (this *TokenFieldAlias) ContextFlagsAny() SqlPart {
    return SP_UNDEF
}

func (this *TokenFieldAlias) ContextFlagsEach() SqlPart {
    return SP_UNDEF
}
*/
func (this *TokenFieldAlias) CheckContext(sectionKind sqlcore.SqlPartKind,
	subsectionKind sqlcore.SqlSubPartKind, stack *sqlcore.CallStack) bool {
	// TODO
	return true
}

func (this *TokenFieldAlias) GetFieldAliasOrName() string {
	return this.Alias
}

type TokenTableAlias struct {
	DataSource sqlcore.Query
	Alias      string
}

func (this *TokenTableAlias) IsTableBased() (bool, sqlcore.Table) {
	return this.DataSource.IsTableBased()
}

func (this *TokenTableAlias) GetColumnCount() (int, error) {
	return this.DataSource.GetColumnCount()
}

func (this *TokenTableAlias) ColumnIsAmbiguous(name string) (bool, error) {
	return this.DataSource.ColumnIsAmbiguous(name)
}

func (this *TokenTableAlias) ColumnExists(name string) (bool, error) {
	return this.DataSource.ColumnExists(name)
}

func (this *TokenTableAlias) GetSource() sqlcore.Query {
	return this.DataSource
}

func (this *TokenTableAlias) GetAlias() string {
	return this.Alias
}

type TokenFieldAssign struct {
	Field *TokenField
	Value Expr
}

func (this *TokenFieldAssign) GetSql(context *ExprBuildContext) (*sqlcore.Statement, error) {
	stat := sqlcore.NewStatement(sqlcore.SS_UNDEF)
	stat.WriteString("%s = ",
		context.Format.FormatObjectName(this.Field.Name))
	stat2, err := this.Value.GetSql(context)
	if err != nil {
		return nil, err
	}
	stat.AppendStatPart(stat2)
	return stat, nil
}

type TokenError struct {
	Error error
}

func NewTokenError(err error) *TokenError {
	this := &TokenError{Error: err}
	return this
}

func (this *TokenError) GetSql(context *ExprBuildContext) (*sqlcore.Statement, error) {
	return nil, this.Error
}

func (this *TokenError) CollectFields() []*TokenField {
	return nil
}

func (this *TokenError) CheckContext(partKind sqlcore.SqlPartKind,
	subPartKind sqlcore.SqlSubPartKind,
	stack *sqlcore.CallStack) bool {
	return false
}
