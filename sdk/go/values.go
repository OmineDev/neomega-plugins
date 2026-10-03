package neomega

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unicode/utf8"
)

const MaxValueBytes = 16 << 20

// DecodeJSON decodes bounded JSON without losing integer precision. Duplicate
// object keys and non-UTF-8 input are rejected rather than silently normalized.
func DecodeJSON(data []byte) (any, error) {
	if len(data) > MaxValueBytes || !utf8.Valid(data) {
		return nil, errors.New("invalid JSON size or UTF-8")
	}
	if err := validJSONStrings(data); err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	nodes := 0
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		nodes++
		if depth > 256 || nodes > 1000000 {
			return nil, errors.New("JSON exceeds nesting or node limit")
		}
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return token, nil
		}
		switch delim {
		case '{':
			obj := map[string]any{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("invalid object key")
				}
				if _, found := obj[key]; found {
					return nil, errors.New("duplicate JSON object key")
				}
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				obj[key] = v
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, errors.New("invalid object")
			}
			return obj, nil
		case '[':
			arr := []any{}
			for d.More() {
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, errors.New("invalid array")
			}
			return arr, nil
		default:
			return nil, errors.New("unexpected JSON delimiter")
		}
	}
	value, err := read(0)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return value, nil
}

// EncodeJSON retains json.Number values and applies the same bounded value
// contract as DecodeJSON. Bus values remain explicit tagged wire values.
func EncodeJSON(value any) ([]byte, error) {
	nodes := 0
	if err := validNativeStrings(reflect.ValueOf(value), 0, &nodes); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if _, err = DecodeJSON(data); err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}
	return data, nil
}

// encoding/json replaces unpaired UTF-16 escapes; reject those at the wire
// boundary so an identity or value is never silently changed by decoding.
func validJSONStrings(data []byte) error {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return errors.New("invalid JSON string escape")
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return errors.New("invalid Unicode escape")
		}
		value, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return errors.New("invalid Unicode escape")
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return errors.New("unpaired Unicode surrogate")
		}
		if value >= 0xd800 && value <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return errors.New("unpaired Unicode surrogate")
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return errors.New("unpaired Unicode surrogate")
			}
			i += 6
		}
	}
	return nil
}

func validNativeStrings(v reflect.Value, depth int, nodes *int) error {
	*nodes++
	if depth > 256 || *nodes > 1000000 {
		return errors.New("JSON exceeds nesting or node limit")
	}
	if !v.IsValid() {
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		if !utf8.ValidString(v.String()) {
			return errors.New("invalid UTF-8 string")
		}
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			return validNativeStrings(v.Elem(), depth+1, nodes)
		}
	case reflect.Map:
		it := v.MapRange()
		for it.Next() {
			if err := validNativeStrings(it.Key(), depth+1, nodes); err != nil {
				return err
			}
			if err := validNativeStrings(it.Value(), depth+1, nodes); err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := 0; i < v.Len(); i++ {
			if err := validNativeStrings(v.Index(i), depth+1, nodes); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath != "" {
				continue
			}
			if err := validNativeStrings(v.Field(i), depth+1, nodes); err != nil {
				return err
			}
		}
	}
	return nil
}
