package unsafe

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hgiasac/hasura-client-simple"
)

func Decode(r io.ReadCloser, v interface{}) error {
	var rawResult hasura.QueryResult

	err := json.NewDecoder(r).Decode(&rawResult)
	if err != nil {
		return err
	}

	return decode(&rawResult, v)
}

func DecodeBytes(input []byte, v interface{}) error {
	var rawResult hasura.QueryResult

	err := json.Unmarshal(input, &rawResult)
	if err != nil {
		return err
	}

	return decode(&rawResult, v)
}

func DecodeBulk(r io.ReadCloser, v interface{}, args ...interface{}) error {
	var rawResult []hasura.QueryResult

	err := json.NewDecoder(r).Decode(&rawResult)
	if err != nil {
		return err
	}

	return decodeBulk(rawResult, append([]interface{}{v}, args...))
}

func DecodeBulkBytes(input []byte, v interface{}, args ...interface{}) error {
	var rawResult []hasura.QueryResult

	err := json.Unmarshal(input, &rawResult)
	if err != nil {
		return err
	}

	return decodeBulk(rawResult, append([]interface{}{v}, args...))
}

func decode(raw *hasura.QueryResult, v interface{}) error {
	if raw == nil || len(raw.Result) < 2 {
		return nil
	}

	sliceValue, structType, err := validateInput(v)
	if err != nil {
		return err
	}

	// parse column indexes by keys
	columnMap := make(map[string]int)
	fieldIndexMap := make(map[int]int)

	// the first row is header
	for i, c := range raw.Result[0] {
		columnMap[c] = i
	}
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		tag := field.Tag.Get("column")
		if tag == "-" {
			continue
		}

		if tag == "" {
			tag = field.Name
		}
		if columnIndex, ok := columnMap[tag]; ok {
			fieldIndexMap[i] = columnIndex
		}
	}

	for y, row := range raw.Result[1:] {

		itemPtr := reflect.New(structType)
		item := reflect.Indirect(itemPtr)
		for i := 0; i < item.NumField(); i++ {
			colIndex, ok := fieldIndexMap[i]
			if !ok {
				continue
			}
			sValue := row[colIndex]

			// ignore null values
			if strings.ToUpper(sValue) == "NULL" {
				continue
			}

			field := item.Field(i)

			switch field.Interface().(type) {
			case string:
				field.SetString(sValue)
			case bool:
				bValue, err := strconv.ParseBool(sValue)
				if err != nil {
					return fmt.Errorf("expected bool, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(bValue))
			case int:
				iValue, err := strconv.Atoi(sValue)
				if err != nil {
					return fmt.Errorf("expected int, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(iValue))
			case int8:
				iValue, err := strconv.ParseInt(sValue, 10, 8)
				if err != nil {
					return fmt.Errorf("expected int8, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(int8(iValue)))
			case int16:
				iValue, err := strconv.ParseInt(sValue, 10, 16)
				if err != nil {
					return fmt.Errorf("expected int16, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(int16(iValue)))
			case int32:
				iValue, err := strconv.ParseInt(sValue, 10, 32)
				if err != nil {
					return fmt.Errorf("expected int32, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(int32(iValue)))
			case int64:
				iValue, err := strconv.ParseInt(sValue, 10, 64)
				if err != nil {
					return fmt.Errorf("expected int64, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(iValue))
			case uint:
				iValue, err := strconv.ParseUint(sValue, 10, 16)
				if err != nil {
					return fmt.Errorf("expected uint, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(uint(iValue)))
			case uint8:
				iValue, err := strconv.ParseUint(sValue, 10, 8)
				if err != nil {
					return fmt.Errorf("expected int8, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(uint8(iValue)))
			case uint16:
				iValue, err := strconv.ParseUint(sValue, 10, 16)
				if err != nil {
					return fmt.Errorf("expected uint16, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(uint16(iValue)))
			case uint32:
				iValue, err := strconv.ParseUint(sValue, 10, 32)
				if err != nil {
					return fmt.Errorf("expected uint32, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(uint32(iValue)))
			case uint64:
				iValue, err := strconv.ParseUint(sValue, 10, 64)
				if err != nil {
					return fmt.Errorf("expected uint64, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(iValue))
			case float32:
				fValue, err := strconv.ParseFloat(sValue, 32)
				if err != nil {
					return fmt.Errorf("expected float32, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(float32(fValue)))
			case float64:
				fValue, err := strconv.ParseFloat(sValue, 32)
				if err != nil {
					return fmt.Errorf("expected float64, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(fValue))
			case complex64:
				cValue, err := strconv.ParseComplex(sValue, 64)
				if err != nil {
					return fmt.Errorf("expected complex64, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(complex64(cValue)))
			case complex128:
				cValue, err := strconv.ParseComplex(sValue, 128)
				if err != nil {
					return fmt.Errorf("expected complex128, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(cValue))
			case time.Time:
				tValue, err := decodeTime(sValue)
				if err != nil {
					return fmt.Errorf("expected date time format, got %s. row: %d, column: %d", sValue, y, i)
				}
				field.Set(reflect.ValueOf(tValue))
			default:
				// try to decode json
				err := json.Unmarshal([]byte(sValue), field.Addr().Interface())
				if err != nil {
					return err
				}
			}
		}
		sliceValue.Set(reflect.Append(*sliceValue, itemPtr.Elem()))
	}
	return nil
}

func validateInput(v interface{}) (*reflect.Value, reflect.Type, error) {
	slicePtr := reflect.ValueOf(v)
	if slicePtr.Kind() != reflect.Ptr {
		return nil, nil, fmt.Errorf("expected input interface to be a pointer, got %s", slicePtr.Kind())
	}

	sliceValue := slicePtr.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return nil, nil, fmt.Errorf("expected input type to be a slice, got %s", sliceValue.Kind())
	}

	structType := sliceValue.Type().Elem()
	if structType.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected input item to be a struct, got %s", structType.Kind())
	}

	return &sliceValue, structType, nil
}

func decodeBulk(inputs []hasura.QueryResult, results []interface{}) error {
	currentIndex := 0
	for _, r := range inputs {
		if r.ResultType != hasura.TuplesOk {
			continue
		}

		err := decode(&r, results[currentIndex])
		if err != nil {
			return err
		}
		currentIndex++
	}

	return nil
}

func decodeTime(input string) (t time.Time, err error) {
	var layouts []string
	if len(input) <= 10 {
		layouts = []string{"2006-01-02", "03:04", "03:04:05", "03:04:05PM"}
	} else {
		layouts = []string{
			"2006-01-02 15:04:05.999999-07",
			time.RFC3339, time.RFC3339Nano, time.RFC1123,
			time.RFC1123Z, time.RFC822, time.RFC822Z,
		}
	}

	for _, lo := range layouts {
		if t, err = time.Parse(lo, input); err == nil {
			return
		}
	}
	return
}
