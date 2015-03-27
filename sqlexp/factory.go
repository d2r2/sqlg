package sqlexp

import (
	"time"

	"github.com/d2r2/sqlg/sqlcore"
	"github.com/d2r2/sqlg/sqldef"
)

type ExprFactory struct {
}

func Factory() *ExprFactory {
	fry := &ExprFactory{}
	return fry
}

func (this *ExprFactory) Field(query sqlcore.Query, fieldName string) *TokenField {
	exp := &TokenField{DataSource: query, Name: fieldName}
	return exp
}

func (this *ExprFactory) Value(value interface{}) *TokenValue {
	exp := &TokenValue{Value: value}
	return exp
}

// shortage and full equivalent of function Value(...)
func (this *ExprFactory) V(value interface{}) *TokenValue {
	exp := &TokenValue{Value: value}
	return exp
}

func (this *ExprFactory) Assign(field *TokenField, value Expr) *TokenFieldAssign {
	exp := &TokenFieldAssign{Field: field, Value: value}
	return exp
}

func (this *ExprFactory) makeFunc(function SqlFunc,
	args ...Expr) *TokenFunc {
	exp := &TokenFunc{Args: args, Func: function}
	return exp
}

func (this *ExprFactory) convertToExpr(expr interface{}) Expr {
	switch expr.(type) {
	case Expr:
		return expr.(Expr)
	case string, int, int32, int64, uint, uint32, uint64, float32, float64, bool:
		return this.Value(expr)
	case time.Duration, time.Time:
		return this.Value(expr)
	default:
		return NewTokenError(e(
			"Don't know how to convert the value to sql expression: %s", expr))
	}
}

// sql operator: =
func (this *ExprFactory) Equal(l Expr, r interface{}) *TokenFunc {
	r2 := this.convertToExpr(r)
	return this.makeFunc(SF_EQUAL, l, r2)
}

// sql operator: <>
func (this *ExprFactory) NotEqual(l Expr, r interface{}) *TokenFunc {
	r2 := this.convertToExpr(r)
	return this.makeFunc(SF_NOT_EQ, l, r2)
}

// sql operator: AND
func (this *ExprFactory) And(l, r Expr) *TokenFunc {
	return this.makeFunc(SF_AND, l, r)
}

// sql operator: OR
func (this *ExprFactory) Or(l, r Expr) *TokenFunc {
	return this.makeFunc(SF_OR, l, r)
}

// sql operator: <
func (this *ExprFactory) Less(l Expr, r interface{}) *TokenFunc {
	r2 := this.convertToExpr(r)
	return this.makeFunc(SF_LESS, l, r2)
}

// sql operator: >
func (this *ExprFactory) Greater(l Expr, r interface{}) *TokenFunc {
	r2 := this.convertToExpr(r)
	return this.makeFunc(SF_GREAT, l, r2)
}

// sql operator: <=
func (this *ExprFactory) LessEq(l Expr, r interface{}) *TokenFunc {
	r2 := this.convertToExpr(r)
	return this.makeFunc(SF_LESS_EQ, l, r2)
}

// sql operator: >=
func (this *ExprFactory) GreaterEq(l Expr, r interface{}) *TokenFunc {
	r2 := this.convertToExpr(r)
	return this.makeFunc(SF_GREAT_EQ, l, r2)
}

// TODO this function should be on top of expr calls
// TODO some verification should be implemented
// order by: asc
func (this *ExprFactory) SortAsc(expr Expr) *TokenFunc {
	return this.makeFunc(SF_ORD_ASC, expr)
}

// TODO this function should be on top of expr calls
// TODO some verification should be implemented
// order by: desc
func (this *ExprFactory) SortDesc(expr Expr) *TokenFunc {
	return this.makeFunc(SF_ORD_DESC, expr)
}

// aggregate function: sum()
func (this *ExprFactory) Sum(expr Expr) *TokenFunc {
	return this.makeFunc(SF_AGR_SUM, expr)
}

// agregate function: count()
func (this *ExprFactory) Count(expr Expr) *TokenFunc {
	return this.makeFunc(SF_AGR_COUNT, expr)
}

// agregate function: min()
func (this *ExprFactory) Min(expr Expr) *TokenFunc {
	return this.makeFunc(SF_AGR_MIN, expr)
}

// agregate function: max()
func (this *ExprFactory) Max(expr Expr) *TokenFunc {
	return this.makeFunc(SF_AGR_MAX, expr)
}

// agregate function: avg()
func (this *ExprFactory) Average(expr Expr) *TokenFunc {
	return this.makeFunc(SF_AGR_AVG, expr)
}

func (this *ExprFactory) IsNull(expr Expr) *TokenFunc {
	return this.makeFunc(SF_IS_NULL, expr)
}

func (this *ExprFactory) IsNotNull(expr Expr) *TokenFunc {
	return this.makeFunc(SF_IS_NOT_NULL, expr)
}

func (this *ExprFactory) Add(expr1 Expr, expr2 interface{}) *TokenFunc {
	r2 := this.convertToExpr(expr2)
	return this.makeFunc(SF_ADD, expr1, r2)
}

func (this *ExprFactory) Subt(expr1 Expr, expr2 interface{}) *TokenFunc {
	r2 := this.convertToExpr(expr2)
	return this.makeFunc(SF_SUBT, expr1, r2)
}

func (this *ExprFactory) Mult(expr1 Expr, expr2 interface{}) *TokenFunc {
	r2 := this.convertToExpr(expr2)
	return this.makeFunc(SF_MULT, expr1, r2)
}

func (this *ExprFactory) Div(expr1 Expr, expr2 interface{}) *TokenFunc {
	r2 := this.convertToExpr(expr2)
	return this.makeFunc(SF_DIV, expr1, r2)
}

func (this *ExprFactory) CaseThenElse(expr1 Expr, expr2, expr3 interface{}) *TokenFunc {
	r2 := this.convertToExpr(expr2)
	r3 := this.convertToExpr(expr3)
	return this.makeFunc(SF_CASE_THEN_ELSE, expr1, r2, r3)
}

func (this *ExprFactory) Coalesce(first Expr, rest ...interface{}) *TokenFunc {
	args := []Expr{first}
	for _, v := range rest {
		args = append(args, this.convertToExpr(v))
	}
	return this.makeFunc(SF_COALESCE, args...)
}

// create alias for expression
func (this *ExprFactory) FieldAlias(expr Expr,
	alias string) *TokenFieldAlias {
	efa := &TokenFieldAlias{Expr: expr, Alias: alias}
	return efa
}

func (this *ExprFactory) TableAlias(query sqlcore.Query,
	alias string) *TokenTableAlias {
	eta := &TokenTableAlias{DataSource: query, Alias: alias}
	return eta
}

// sql custom function

func (this *ExprFactory) FuncDialectDef(dialect sqldef.Dialect, template string,
	minParams, maxParams int) *CustomDialectFuncDef {
	fnc := NewCustomDialectFunc(dialect, template, minParams,
		maxParams)
	return fnc
}

func (this *ExprFactory) FuncDef(funcs ...*CustomDialectFuncDef) *CustomFuncDef {
	fnc := NewCustomFunc(funcs...)
	return fnc
}

func (this *ExprFactory) Func(fnc *CustomFuncDef, args ...interface{}) *TokenFunc {
	// convert slice of interface{} to Expr
	args2 := make([]Expr, len(args))
	for i, item := range args {
		args2[i] = this.convertToExpr(item)
	}
	exp := &TokenFunc{Args: args2, Func: SF_CUSTOMFUNC, CustomFunc: fnc}
	return exp
}

// =========================================
//      sql dialect specific functions
// =========================================

func (this *ExprFactory) TrimSpace(expr Expr) *TokenFunc {
	return this.makeFunc(SF_TRIMSPACE, expr)
}

func (this *ExprFactory) TrimSpaceRight(expr Expr) *TokenFunc {
	return this.makeFunc(SF_RTRIMSPACE, expr)
}

func (this *ExprFactory) TrimSpaceLeft(expr Expr) *TokenFunc {
	return this.makeFunc(SF_LTRIMSPACE, expr)
}

func (this *ExprFactory) CurrentDate() *TokenFunc {
	return this.makeFunc(SF_CURDATE)
}

func (this *ExprFactory) CurrentTime() *TokenFunc {
	return this.makeFunc(SF_CURTIME)
}

func (this *ExprFactory) CurrentDateTime() *TokenFunc {
	return this.makeFunc(SF_CURDATETIME)
}

// new functions
