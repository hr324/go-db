package sqlmini

type stmtKind int

const (
	sNil stmtKind = iota
	sCreateTable
	sCreateIndex
	sInsert
	sDelete
	sSelect
	sBegin
	sCommit
	sRollback
)

type whereKind int

const (
	wNone whereKind = iota
	wPkEq
	wPkRange
	wFieldEq
	wFieldRange
)

type stmt struct {
	k stmtKind

	tbl   string
	idx   string
	field string

	pk   string
	json string

	where  whereKind
	wField string
	loInc  bool
	hiInc  bool
	lo     string
	hi     string
}

type parser struct {
	lex *lexer
	cur token
}

func newParser(s string) *parser {
	p := &parser{lex: newLexer(s)}
	p.next()
	return p
}

func (p *parser) next() { p.cur = p.lex.scan() }

func (p *parser) accept(k tokKind) bool {
	if p.cur.k == k {
		p.next()
		return true
	}
	return false
}

func (p *parser) expect(k tokKind) {
	if p.cur.k != k {
		panic("parse")
	}
	p.next()
}

func (p *parser) parseIdent() string {
	if p.cur.k == tIdent {
		s := p.cur.s
		p.next()
		return s
	}
	panic("ident")
}

func (p *parser) parseString() string {
	if p.cur.k == tString {
		s := p.cur.s
		p.next()
		return s
	}
	panic("string")
}

func (p *parser) parse() (stmts []stmt) {
	for {
		for p.cur.k == tSemicolon {
			p.next()
		}
		switch p.cur.k {
		case tEOF:
			return
		case tCreate:
			stmts = append(stmts, p.parseCreate())
		case tInsert:
			stmts = append(stmts, p.parseInsert())
		case tDelete:
			stmts = append(stmts, p.parseDelete())
		case tSelect:
			stmts = append(stmts, p.parseSelect())
		case tBegin:
			p.next()
			stmts = append(stmts, stmt{k: sBegin})
			if p.cur.k == tSemicolon {
				p.next()
			}
		case tCommit:
			p.next()
			stmts = append(stmts, stmt{k: sCommit})
			if p.cur.k == tSemicolon {
				p.next()
			}
		case tRollback:
			p.next()
			stmts = append(stmts, stmt{k: sRollback})
			if p.cur.k == tSemicolon {
				p.next()
			}
		default:
			panic("stmt")
		}
	}
}

func (p *parser) parseCreate() stmt {
	p.expect(tCreate)
	switch p.cur.k {
	case tTable:
		p.next()
		name := p.parseIdent()
		if p.cur.k == tSemicolon {
			p.next()
		}
		return stmt{k: sCreateTable, tbl: name}
	case tIndex:
		p.next()
		idx := p.parseIdent()
		p.expect(tOn)
		tbl := p.parseIdent()
		p.expect(tLParen)
		f := p.parseIdent()
		p.expect(tRParen)
		if p.cur.k == tSemicolon {
			p.next()
		}
		return stmt{k: sCreateIndex, tbl: tbl, idx: idx, field: f}
	default:
		panic("create")
	}
}

func (p *parser) parseInsert() stmt {
	p.expect(tInsert)
	p.expect(tInto)
	tbl := p.parseIdent()
	p.expect(tValues)
	p.expect(tLParen)
	pk := p.parseString()
	p.expect(tComma)
	p.expect(tJSON)
	j := p.parseString()
	p.expect(tRParen)
	if p.cur.k == tSemicolon {
		p.next()
	}
	return stmt{k: sInsert, tbl: tbl, pk: pk, json: j}
}

func (p *parser) parseDelete() stmt {
	p.expect(tDelete)
	p.expect(tFrom)
	tbl := p.parseIdent()
	p.expect(tWhere)
	p.expect(tIdent)
	p.expect(tEq)
	pk := p.parseString()
	if p.cur.k == tSemicolon {
		p.next()
	}
	return stmt{k: sDelete, tbl: tbl, pk: pk, where: wPkEq}
}

func (p *parser) parseSelect() stmt {
	p.expect(tSelect)
	if p.cur.k == tStar {
		p.next()
	}
	p.expect(tFrom)
	tbl := p.parseIdent()
	if !p.accept(tWhere) {
		if p.cur.k == tSemicolon {
			p.next()
		}
		return stmt{k: sSelect, tbl: tbl, where: wNone}
	}
	lhs := p.parseIdent()
	switch lhs {
	case "pk":
		if p.accept(tEq) {
			pk := p.parseString()
			if p.cur.k == tSemicolon {
				p.next()
			}
			return stmt{k: sSelect, tbl: tbl, where: wPkEq, pk: pk}
		}
		op1 := p.cur.k
		p.next()
		lo := p.parseString()
		p.expect(tAnd)
		_ = p.parseIdent()
		op2 := p.cur.k
		p.next()
		hi := p.parseString()
		loInc := (op1 == tGe)
		hiInc := (op2 == tLe)
		if p.cur.k == tSemicolon {
			p.next()
		}
		return stmt{k: sSelect, tbl: tbl, where: wPkRange, lo: lo, hi: hi, loInc: loInc, hiInc: hiInc}
	default:
		field := lhs
		if p.accept(tEq) {
			val := p.parseString()
			if p.cur.k == tSemicolon {
				p.next()
			}
			return stmt{k: sSelect, tbl: tbl, where: wFieldEq, wField: field, lo: val, hi: val, loInc: true, hiInc: true}
		}
		op1 := p.cur.k
		p.next()
		lo := p.parseString()
		p.expect(tAnd)
		_ = p.parseIdent()
		op2 := p.cur.k
		p.next()
		hi := p.parseString()
		loInc := (op1 == tGe)
		hiInc := (op2 == tLe)
		if p.cur.k == tSemicolon {
			p.next()
		}
		return stmt{k: sSelect, tbl: tbl, where: wFieldRange, wField: field, lo: lo, hi: hi, loInc: loInc, hiInc: hiInc}
	}
}
