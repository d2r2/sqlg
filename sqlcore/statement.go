package sqlcore

import (
	"bytes"
	"database/sql"
)

type StatementType int

const (
	SS_UNDEF StatementType = iota
	SS_EXEC
	SS_QUERY
)

func (this StatementType) String() string {
	switch this {
	case SS_UNDEF:
		return "Undefined"
	case SS_EXEC:
		return "Exec"
	case SS_QUERY:
		return "Query"
	}
	return f("%d", this)
}

type Statement struct {
	sql  bytes.Buffer
	Type StatementType
	Args []interface{}
}

func (this *Statement) Sql() string {
	return this.sql.String()
}

func (this *Statement) String() string {
	str := f("Type: %v; Sql: %s; Args: %v", this.Type,
		this.sql.String(), this.Args)
	return str
}

func (this *Statement) WriteString(sql string, params ...interface{}) {
	if len(params) > 0 {
		sql = f(sql, params...)
	}
	this.sql.WriteString(sql)
}

func (this *Statement) WriteRune(ch rune) {
	this.sql.WriteRune(ch)
}

func (this *Statement) AppendArgs(args []interface{}) {
	this.Args = append(this.Args, args...)
}

func (this *Statement) AppendArg(arg interface{}) {
	this.Args = append(this.Args, arg)
}

func (this *Statement) AppendStatPart(stat *Statement) {
	this.WriteString(stat.Sql())
	this.AppendArgs(stat.Args)
}

func (this *Statement) AppendStatPartsFormat(format string, stats ...*Statement) {
	sqls := make([]interface{}, len(stats))
	count := 0
	for i, stat := range stats {
		sqls[i] = stat.Sql()
		count += len(stat.Args)
	}
	args := make([]interface{}, count)
	j := 0
	for _, stat := range stats {
		for _, arg := range stat.Args {
			args[j] = arg
			j++
		}
	}
	sql := f(format, sqls...)
	this.WriteString(sql)
	this.AppendArgs(args)
}

func NewStatement(statType StatementType) *Statement {
	stat := &Statement{Type: statType}
	return stat
}

type StatementBatch struct {
	Items []*Statement
}

func NewStatementBatch() *StatementBatch {
	batch := &StatementBatch{}
	return batch
}

func (this *StatementBatch) Add(stat *Statement) {
	this.Items = append(this.Items, stat)
}

func (this *StatementBatch) Remove(stat *Statement) {
	var foundIndex int = -1
	for i, item := range this.Items {
		if item == stat {
			foundIndex = i
			break
		}
	}
	if foundIndex != -1 {
		this.Items = append(this.Items[:foundIndex], this.Items[foundIndex+1:]...)
	}
}

func (this *StatementBatch) Replace(stat, newstat *Statement) {
	for index, item := range this.Items {
		if item == stat {
			this.Items[index] = newstat
			break
		}
	}
}

func (this *StatementBatch) Last() *Statement {
	if len(this.Items) > 0 {
		return this.Items[len(this.Items)-1]
	} else {
		return nil
	}
}

func (this *StatementBatch) Join(format *Format) error {
	// join statement if necessary
	if format.SupportMultipleStatementsInBatch() &&
		len(this.Items) > 1 {
		firstStat := this.Items[0]
		lastStat := this.Items[len(this.Items)-1]
		for i, item := range this.Items {
			if firstStat != item {
				firstStat.WriteString(";")
				firstStat.WriteString(format.SectionDivider)
				firstStat.AppendStatPart(item)
			}
			if i == len(this.Items)-1 {
				firstStat.Type = lastStat.Type
			} else {
				if item.Type != SS_EXEC {
					return e("Can't join statement, since it's return record set: \"%s\"",
						item.Sql())
				}
			}
		}
		this.Items = nil
		this.Add(firstStat)
	}
	return nil
}

func (this *StatementBatch) Exec(db *sql.DB) (sql.Result, error) {
	log.Debug(this)
	var res sql.Result
	for _, stat := range this.Items {
		if stat.Type != SS_EXEC {
			return nil, e("Statement is not \"exec\" type: %v", stat)
		}
		res2, err := db.Exec(stat.Sql(), stat.Args...)
		if err != nil {
			log.Error(err)
			log.Error(stat)
			return nil, err
		}
		res = res2
	}
	return res, nil
}

func (this *StatementBatch) ExecQueryRow(db *sql.DB) (*sql.Row, error) {
	log.Debug(this)
	for i, stat := range this.Items {
		if i < len(this.Items)-1 {
			if stat.Type != SS_EXEC {
				return nil, e("Statement is not \"exec\" type: %v", stat)
			}
			_, err := db.Exec(stat.Sql(), stat.Args...)
			if err != nil {
				log.Error(err)
				log.Error(stat)
				return nil, err
			}
		} else {
			if stat.Type != SS_QUERY {
				return nil, e("Statement is not \"query\" type: %v", stat)
			}
			row := db.QueryRow(stat.Sql(), stat.Args...)
			return row, nil
		}
	}
	return nil, nil
}

func (this *StatementBatch) Query(db *sql.DB) (*sql.Rows, error) {
	log.Debug(this)
	if len(this.Items) > 1 {
		return nil, e("Can't query multiple statments: %v", this)
	}
	stat := this.Items[0]
	rows, err := db.Query(stat.Sql(), stat.Args...)
	if err != nil {
		log.Error(err)
		log.Error(stat)
		return nil, err
	}
	return rows, nil
}
