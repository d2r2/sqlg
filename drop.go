package sqlg

type dropDatabaseMaker struct {
    Format *Format
    Batch  *StatementBatch
}

func (this *dropDatabaseMaker) buildDropDatabase(sect *DropDatabaseRoot,
    stack *CallStack) error {
    if this.Format.DoIfObjectExistsNotExists() &&
        this.Format.Dialect == DI_MSTSQL {
        this.Format.incIndentLevel()
        defer this.Format.decIndentLevel()
    }
    err := sect.buildDropDatabaseSql(this, this.Batch.Last(), stack)
    return err
}

func (this *dropDatabaseMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct == false {
        var err error
        switch section.GetSectionKind() {
        case SK_DROP_DATABASE:
            sect := section.(*DropDatabaseRoot)
            err = this.buildDropDatabase(sect, stack)
            if err != nil {
                return err
            }
            if this.Format.DoIfObjectExistsNotExists() &&
                this.Format.Dialect == DI_MSTSQL {
                stat := this.Batch.Last()
                newstat, err := ifExistsNotExistsBlockMicrosoftCase(
                    section, stat, this.Format, stack)
                if err != nil {
                    return err
                }
                this.Batch.Replace(stat, newstat)
            }
        default:
            err = e("Unexpected section during generating "+
                "\"drop database\" statement: %v", section)
        }
        return err
    }
    return nil
}

func (this *dropDatabaseMaker) BuildSql(section Section,
    format *Format) error {
    this.Format = format
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    return iterateSqlParents(false, section, this.runMaker)
}

func (this *dropDatabaseMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack,
    format *Format) *ExprBuildContext {
    context := NewExprBuildContext(sectionKind, subsectionKind, stack, format, nil)
    return context
}

type DropDatabaseRoot struct {
    DatabaseName string
}

func NewDropDatabaseRoot(databaseName string) *DropDatabaseRoot {
    r := &DropDatabaseRoot{DatabaseName: databaseName}
    return r
}

func (this *DropDatabaseRoot) buildDropDatabaseSql(maker *dropDatabaseMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString("drop database ")
    if maker.Format.DoIfObjectExistsNotExists() &&
        maker.Format.Dialect.In(DI_PGSQL|DI_MYSQL) {
        stat.writeString("if exists ")
    }
    name := maker.Format.formatObjectName(this.DatabaseName)
    stat.writeString(name)
    return nil
}

func (this *DropDatabaseRoot) GetSql(format *Format) (*StatementBatch, error) {
    maker := &dropDatabaseMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, err
}

func (this *DropDatabaseRoot) GetSectionKind() SectionKind {
    return SK_DROP_DATABASE
}

func (this *DropDatabaseRoot) GetParent() Section {
    return nil
}

type dropTableMaker struct {
    Format *Format
    Batch  *StatementBatch
}

func (this *dropTableMaker) buildDropTable(sect *DropTableRoot,
    stack *CallStack) error {
    if this.Format.DoIfObjectExistsNotExists() &&
        this.Format.Dialect == DI_MSTSQL {
        this.Format.incIndentLevel()
        defer this.Format.decIndentLevel()
    }
    err := sect.buildDropTableSql(this, this.Batch.Last(), stack)
    return err
}

func (this *dropTableMaker) runMaker(direct bool,
    section Section, stack *CallStack) error {
    if direct == false {
        switch section.GetSectionKind() {
        case SK_DROP_TABLE:
            sect := section.(*DropTableRoot)
            err := this.buildDropTable(sect, stack)
            if err != nil {
                return err
            }
            if this.Format.DoIfObjectExistsNotExists() &&
                this.Format.Dialect == DI_MSTSQL {
                stat := this.Batch.Last()
                newstat, err := ifExistsNotExistsBlockMicrosoftCase(
                    section, stat, this.Format, stack)
                if err != nil {
                    return err
                }
                this.Batch.Replace(stat, newstat)
            }
            return nil
        default:
            return e("Unexpected section during generating "+
                "\"drop table\" statement: %v", section)
        }
    }
    return nil
}

func (this *dropTableMaker) BuildSql(section Section,
    format *Format) error {
    this.Format = format
    this.Batch = NewStatementBatch()
    this.Batch.Add(NewStatement(SS_EXEC))
    return iterateSqlParents(false, section, this.runMaker)
}

func (this *dropTableMaker) GetExprBuildContext(sectionKind SectionKind,
    subsectionKind SubsectionKind, stack *CallStack,
    format *Format) *ExprBuildContext {
    context := NewExprBuildContext(sectionKind, subsectionKind, stack, format, nil)
    return context
}

type DropTableRoot struct {
    Table *TableDef
}

// TODO add "check if exists" parameter
func NewDropTableRoot(table *TableDef) *DropTableRoot {
    r := &DropTableRoot{Table: table}
    return r
}

func (this *DropTableRoot) buildDropTableSql(maker *dropTableMaker,
    stat *Statement, stack *CallStack) error {
    stat.writeString(maker.Format.getLeadingSpace())
    stat.writeString("drop table ")
    if maker.Format.DoIfObjectExistsNotExists() &&
        maker.Format.Dialect.In(DI_PGSQL|DI_MYSQL|DI_SQLITE) {
        stat.writeString("if exists ")
    }
    name := maker.Format.formatTableName(this.Table.Name)
    stat.writeString(name)
    return nil
}

func (this *DropTableRoot) GetSql(format *Format) (*StatementBatch, error) {
    maker := &dropTableMaker{}
    err := maker.BuildSql(this, format)
    if err != nil {
        return nil, err
    }
    return maker.Batch, err
}

func (this *DropTableRoot) GetSectionKind() SectionKind {
    return SK_DROP_TABLE
}

func (this *DropTableRoot) GetParent() Section {
    return nil
}
