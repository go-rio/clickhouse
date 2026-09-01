package chproto

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Interpolate substitutes ? placeholders with SQL literals, honoring
// ClickHouse quoting: '...' strings (with \ escapes), `...` and "..."
// identifiers, -- and /* */ comments, and the \? literal-question escape the
// rendered SQL may carry. rio's ClickHouse channel has always bound
// client-side; this owns that duty without a driver.
func Interpolate(query string, args []any) (string, error) {
	if len(args) == 0 && !strings.ContainsRune(query, '?') {
		return query, nil
	}
	buf := make([]byte, 0, len(query)+32*len(args))
	arg := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch ch {
		case '\\':
			if i+1 < len(query) && query[i+1] == '?' {
				buf = append(buf, '?')
				i++
				continue
			}
			buf = append(buf, ch)
		case '?':
			if arg >= len(args) {
				return "", fmt.Errorf("chproto: %d placeholders but %d arguments", arg+1, len(args))
			}
			var err error
			if buf, err = appendLiteral(buf, args[arg]); err != nil {
				return "", err
			}
			arg++
		case '\'':
			end, ok := skipQuoted(query, i, '\'', true)
			if !ok {
				return "", fmt.Errorf("chproto: unterminated string literal")
			}
			buf = append(buf, query[i:end]...)
			i = end - 1
		case '`', '"':
			end, ok := skipQuoted(query, i, ch, false)
			if !ok {
				return "", fmt.Errorf("chproto: unterminated quoted identifier")
			}
			buf = append(buf, query[i:end]...)
			i = end - 1
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				end := i
				for end < len(query) && query[end] != '\n' {
					end++
				}
				buf = append(buf, query[i:end]...)
				i = end - 1
			} else {
				buf = append(buf, ch)
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				depth, end := 1, i+2
				for end < len(query) && depth > 0 {
					if end+1 < len(query) && query[end] == '/' && query[end+1] == '*' {
						depth++
						end += 2
					} else if end+1 < len(query) && query[end] == '*' && query[end+1] == '/' {
						depth--
						end += 2
					} else {
						end++
					}
				}
				if depth != 0 {
					return "", fmt.Errorf("chproto: unterminated comment")
				}
				buf = append(buf, query[i:end]...)
				i = end - 1
			} else {
				buf = append(buf, ch)
			}
		default:
			buf = append(buf, ch)
		}
	}
	if arg != len(args) {
		return "", fmt.Errorf("chproto: %d placeholders but %d arguments", arg, len(args))
	}
	return string(buf), nil
}

// skipQuoted returns the index just past the closing quote.
func skipQuoted(s string, start int, quote byte, backslash bool) (int, bool) {
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if backslash {
				i++
			}
		case quote:
			if i+1 < len(s) && s[i+1] == quote { // doubled quote
				i++
				continue
			}
			return i + 1, true
		}
	}
	return 0, false
}

// appendLiteral renders one binding as a ClickHouse literal.
func appendLiteral(buf []byte, v any) ([]byte, error) {
	if valuer, ok := v.(driver.Valuer); ok {
		resolved, err := valuer.Value()
		if err != nil {
			return nil, err
		}
		v = resolved
	}
	switch x := v.(type) {
	case nil:
		return append(buf, "NULL"...), nil
	case string:
		return appendQuoted(buf, x), nil
	case []byte:
		return appendQuoted(buf, string(x)), nil
	case int64:
		return strconv.AppendInt(buf, x, 10), nil
	case int:
		return strconv.AppendInt(buf, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(buf, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(buf, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(buf, int64(x), 10), nil
	case uint64:
		return strconv.AppendUint(buf, x, 10), nil
	case uint32:
		return strconv.AppendUint(buf, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(buf, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(buf, uint64(x), 10), nil
	case uint:
		return strconv.AppendUint(buf, uint64(x), 10), nil
	case float64:
		return appendFloat(buf, x)
	case float32:
		return appendFloat(buf, float64(x))
	case bool:
		if x {
			return append(buf, "true"...), nil
		}
		return append(buf, "false"...), nil
	case time.Time:
		// rio binds times as text before they reach the channel; direct Raw
		// arguments still deserve a correct literal.
		return appendQuoted(buf, x.UTC().Format("2006-01-02 15:04:05.000000+00:00")), nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return appendFloat(buf, rv.Float())
	case reflect.Bool:
		if rv.Bool() {
			return append(buf, "true"...), nil
		}
		return append(buf, "false"...), nil
	case reflect.String:
		return appendQuoted(buf, rv.String()), nil
	}
	return nil, fmt.Errorf("chproto: cannot bind %T", v)
}

func appendFloat(buf []byte, f float64) ([]byte, error) {
	if f != f || f > 1.7976931348623157e308 || f < -1.7976931348623157e308 {
		return nil, fmt.Errorf("chproto: cannot bind non-finite float %v", f)
	}
	return strconv.AppendFloat(buf, f, 'g', -1, 64), nil
}

// appendQuoted writes a single-quoted literal, escaping backslash and quote.
func appendQuoted(buf []byte, s string) []byte {
	buf = append(buf, '\'')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			buf = append(buf, '\\', '\'')
		case '\\':
			buf = append(buf, '\\', '\\')
		default:
			buf = append(buf, s[i])
		}
	}
	return append(buf, '\'')
}
