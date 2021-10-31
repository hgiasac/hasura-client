package unsafe

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

type testDecodeOutput struct {
	String     string                 `column:"string"`
	Bool       bool                   `column:"bool"`
	Int        int                    `column:"int"`
	Int8       int8                   `column:"int8"`
	Int16      int16                  `column:"int16"`
	Int32      int32                  `column:"int32"`
	Int64      int64                  `column:"int64"`
	Uint       uint                   `column:"uint"`
	Uint8      uint8                  `column:"uint8"`
	Uint16     uint16                 `column:"uint16"`
	Uint32     uint32                 `column:"uint32"`
	Uint64     uint64                 `column:"uint64"`
	Float32    float32                `column:"float32"`
	Float64    float64                `column:"float64"`
	Complex64  complex64              `column:"complex64"`
	Complex128 complex128             `column:"complex128"`
	DateTime   time.Time              `column:"datetime"`
	JSON       map[string]interface{} `column:"json"`
}

type testDecodeFixture struct {
	Text   string
	Output []testDecodeOutput
}

func (to testDecodeOutput) Equal(value testDecodeOutput) error {
	if to.String != value.String {
		return fmt.Errorf("String: expected %s, got %s", to.String, value.String)
	}
	if to.Bool != value.Bool {
		return fmt.Errorf("Bool: expected %v, got %v", to.Bool, value.Bool)
	}
	if to.Int != value.Int {
		return fmt.Errorf("Int: expected %d, got %d", to.Int, value.Int)
	}
	if to.Int8 != value.Int8 {
		return fmt.Errorf("Int8: expected %d, got %d", to.Int8, value.Int8)
	}
	if to.Int16 != value.Int16 {
		return fmt.Errorf("Int16: expected %d, got %d", to.Int16, value.Int16)
	}
	if to.Int32 != value.Int32 {
		return fmt.Errorf("Int32: expected %d, got %d", to.Int32, value.Int32)
	}
	if to.Int64 != value.Int64 {
		return fmt.Errorf("Int64: expected %d, got %d", to.Int64, value.Int64)
	}
	if to.Uint != value.Uint {
		return fmt.Errorf("Uint: expected %d, got %d", to.Uint, value.Uint)
	}
	if to.Uint8 != value.Uint8 {
		return fmt.Errorf("Uint8: expected %d, got %d", to.Uint8, value.Uint8)
	}
	if to.Uint16 != value.Uint16 {
		return fmt.Errorf("Uint16: expected %d, got %d", to.Uint16, value.Uint16)
	}
	if to.Uint32 != value.Uint32 {
		return fmt.Errorf("Uint32: expected %d, got %d", to.Uint32, value.Uint32)
	}
	if to.Uint64 != value.Uint64 {
		return fmt.Errorf("Uint64: expected %d, got %d", to.Uint64, value.Uint64)
	}
	if to.Float32 != value.Float32 {
		return fmt.Errorf("Float32: expected %f, got %f", to.Float32, value.Float32)
	}

	if math.Abs(to.Float64-value.Float64) > 0.0001 {
		return fmt.Errorf("Float64: expected %f, got %f", to.Float64, value.Float64)
	}
	if to.Complex64 != value.Complex64 {
		return fmt.Errorf("Complex64: expected %f, got %f", to.Complex64, value.Complex64)
	}

	if to.Complex128 != value.Complex128 {
		return fmt.Errorf("Complex128: expected %f, got %f", to.Complex128, value.Complex128)
	}
	if math.Abs(float64(to.DateTime.Unix()-value.DateTime.Unix())) > float64(time.Millisecond) {
		return fmt.Errorf("DateTime: expected %s, got %s", to.DateTime, value.DateTime)
	}
	return nil
}

func getDecodeQueryTestFixtures() []testDecodeFixture {
	return []testDecodeFixture{
		{
			Text: `{
				"result_type": "TuplesOk",
				"result": [
					["string", "bool", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "complex64", "complex128", "datetime", "json"],
					["string1", "true", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11.5", "12.3", "13.6", "14.8", "2021-10-10T10:10:10Z", "{\"foo\": \"bar\"}"],
					["string2", "false", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100", "110.5", "120.3", "130.6", "140.8", "2021-10-31 08:58:40.675776+00", "{\"foo_num\": 1}"],
					["null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null"]
				]
			}`,
			Output: []testDecodeOutput{
				{"string1", true, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11.5, 12.3, 13.6, 14.8, time.Date(2021, 10, 10, 10, 10, 10, 10, time.UTC), map[string]interface{}{"foo": "bar"}},
				{"string2", false, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110.5, 120.3, 130.6, 140.8, time.Date(2021, 10, 31, 8, 58, 40, 675776, time.UTC), map[string]interface{}{"foo_num": 1}},
				{"", false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, time.Time{}, nil},
			},
		},
	}
}

func TestDecodeQueryResult(t *testing.T) {
	for _, qt := range getDecodeQueryTestFixtures() {

		var output []testDecodeOutput
		err := DecodeBytes([]byte(qt.Text), &output)
		if err != nil {
			t.Error(err)
			return
		}

		if len(output) != len(qt.Output) {
			t.Errorf("expected %d rows, got %d row", len(qt.Output), len(output))
		}

		for i, o := range output {
			if err := qt.Output[i].Equal(o); err != nil {
				t.Errorf("[%d] %s", i, err)
			}
		}
	}
}

func TestDecodeBulkQueryResult(t *testing.T) {
	testFixtures := getDecodeQueryTestFixtures()
	var sJSONResults []string
	sCommandOk := `{
		"result_type": "CommandOk",
		"result": null
	}`
	for _, qt := range testFixtures {
		sJSONResults = append(sJSONResults, sCommandOk)
		sJSONResults = append(sJSONResults, qt.Text)
	}

	sJSONResults = append(sJSONResults, sCommandOk)
	sJSONResults = append(sJSONResults, `{ 
		"result_type": "TuplesOk",
		"result": [
			["col1", "col2"],
			["foo", "1"]
		]
	}`)

	var output []testDecodeOutput
	var output2 []struct {
		Column1 string `column:"col1"`
		Column2 int    `column:"col2"`
	}

	err := DecodeBulkBytes([]byte(fmt.Sprintf("[%s]", strings.Join(sJSONResults, ","))), &output, &output2)
	if err != nil {
		t.Error(err)
		return
	}

	if len(output) != len(testFixtures[0].Output) {
		t.Errorf("expected %d rows, got %d row", len(testFixtures[0].Output), len(output))
	}

	for i, o := range output {
		if err := testFixtures[0].Output[i].Equal(o); err != nil {
			t.Errorf("[%d] %s", i, err)
		}
	}

	if len(output2) != 1 {
		t.Errorf("output2: expected 1 rows, got %d row", len(output2))
	}

	if output2[0].Column1 != "foo" {
		t.Errorf("output2[col1]: expected foo, got %s", output2[0].Column1)
	}
	if output2[0].Column2 != 1 {
		t.Errorf("output2[col2]: expected 1, got %d", output2[0].Column2)
	}
}
