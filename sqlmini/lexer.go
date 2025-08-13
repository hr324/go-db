package sqlmini

import (
	"strconv"
	"strings"
	"unicode"
)

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tString
	tNumber
	tLParen
	tRParen
	tComma
	tSemicolon
	tEq
	tLt
	tGt
	tLe
	tGe
	tAnd
	tJSON
	tCreate
	tTable
	tIndex
	tOn
	tInsert
	tInto
	tValues
	tDelete
	tFrom
	tWhere
	tSelect
	tStar
	tBegin
	tCommit
	tRollback
)

type token struct {
	k tokKind
	s string
}

type lexer struct {
	src []rune
	i   int
}

func newLexer(s string) *lexer { return &lexer{src: []rune(s)} }

func (l *lexer) next() rune {
	if l.i >= len(l.src) {
		return 0
	}
	r := l.src[l.i]
	l.i++
	return r
}

func (l *lexer) peek() rune {
	if l.i >= len(l.src) {
		return 0
	}
	return l.src[l.i]
}

func (l *lexer) emit(k tokKind, s string) token { return token{k: k, s: s} }

func (l *lexer) scan() token {
	for {
		r := l.peek()
		if r == 0 {
			return l.emit(tEOF, "")
		}
		if unicode.IsSpace(r) {
			l.next()
			continue
		}
		break
	}
	r := l.next()
	switch r {
	case '(':
		return l.emit(tLParen, "(")
	case ')':
		return l.emit(tRParen, ")")
	case ',':
		return l.emit(tComma, ",")
	case ';':
		return l.emit(tSemicolon, ";")
	case '*':
		return l.emit(tStar, "*")
	case '=':
		return l.emit(tEq, "=")
	case '<':
		if l.peek() == '=' {
			l.next()
			return l.emit(tLe, "<=")
		}
		return l.emit(tLt, "<")
	case '>':
		if l.peek() == '=' {
			l.next()
			return l.emit(tGe, ">=")
		}
		return l.emit(tGt, ">")
	case '\'':
		var b strings.Builder
		for {
			ch := l.next()
			if ch == 0 {
				break
			}
			if ch == '\'' {
				if l.peek() == '\'' {
					l.next()
					b.WriteRune('\'')
					continue
				}
				break
			}
			b.WriteRune(ch)
		}
		return l.emit(tString, b.String())
	}
	if unicode.IsDigit(r) {
		var b strings.Builder
		b.WriteRune(r)
		for unicode.IsDigit(l.peek()) {
			b.WriteRune(l.next())
		}
		return l.emit(tNumber, b.String())
	}
	if unicode.IsLetter(r) || r == '_' {
		var b strings.Builder
		b.WriteRune(r)
		for {
			p := l.peek()
			if unicode.IsLetter(p) || unicode.IsDigit(p) || p == '_' {
				b.WriteRune(l.next())
			} else {
				break
			}
		}
		s := strings.ToUpper(b.String())
		switch s {
		case "AND":
			return l.emit(tAnd, s)
		case "JSON":
			return l.emit(tJSON, s)
		case "CREATE":
			return l.emit(tCreate, s)
		case "TABLE":
			return l.emit(tTable, s)
		case "INDEX":
			return l.emit(tIndex, s)
		case "ON":
			return l.emit(tOn, s)
		case "INSERT":
			return l.emit(tInsert, s)
		case "INTO":
			return l.emit(tInto, s)
		case "VALUES":
			return l.emit(tValues, s)
		case "DELETE":
			return l.emit(tDelete, s)
		case "FROM":
			return l.emit(tFrom, s)
		case "WHERE":
			return l.emit(tWhere, s)
		case "SELECT":
			return l.emit(tSelect, s)
		case "BEGIN":
			return l.emit(tBegin, s)
		case "COMMIT":
			return l.emit(tCommit, s)
		case "ROLLBACK":
			return l.emit(tRollback, s)
		}
		return l.emit(tIdent, b.String())
	}
	return l.emit(tEOF, "")
}

func (t token) asInt() (int, bool) {
	v, err := strconv.Atoi(t.s)
	if err != nil {
		return 0, false
	}
	return v, true
}
