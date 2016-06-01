package odbcimporter

import (
	"testing"
)

func Test_parseArray(t *testing.T) {
	givenPostgresArrays := []string{
		"{a, b,c}",
		"{{\"omg{\\\"a\\\"} \\\"abc\",test}, {1,2}}",
		"{}",
		"",
	}
	expectedArrays := []string{
		"[\"a\",\"b\",\"c\"]",
		"[[\"omg{\\\"a\\\"} \\\"abc\",\"test\"], [\"1\",\"2\"]]",
		"[]",
		"",
	}
	for i, givenArray := range givenPostgresArrays {
		result := parseArray(givenArray, &ColumnInfo{"name", "array", 0, "char"})
		if result != expectedArrays[i] {
			t.Errorf("expected parsed array to be %s, got %s", expectedArrays[i], result)
		}
	}
	result := parseArray("{t,false}", &ColumnInfo{"name", "array", 0, "bool"})
	if result != "[true,false]" {
		t.Errorf("expected parsed array to be %s, got %s", "[true,false]", result)
	}
	result = parseArray("{1,2}", &ColumnInfo{"name", "array", 0, "int4"})
	if result != "[1,2]" {
		t.Errorf("expected parsed array to be %s, got %s", "[1,2]", result)
	}
}

func Test_parseSet(t *testing.T) {
	givenMysqlSets := []string{
		"\"a\"{,{\"}b,c",
		"",
	}
	expectedArrays := []string{
		"[\"\\\"a\\\"{\",\"{\\\"}b\",\"c\"]",
		"[]",
	}
	for i, givenSet := range givenMysqlSets {
		result := parseSet(givenSet)
		if result != expectedArrays[i] {
			t.Errorf("expected parsed array to be %s, got %s", expectedArrays[i], result)
		}
	}
}

func Test_parseHstore(t *testing.T) {
	givenHstoreData := []string{
		"\"a\"=>\"\\\"=>\\\"b\",\"b\"=>\"c\"",
		"",
	}
	expectedJsonValues := []string{
		"{\"a\":\"\\\"=>\\\"b\",\"b\":\"c\"}",
		"{}",
	}
	for i, givenDatum := range givenHstoreData {
		result := parseHstore(givenDatum)
		if result != expectedJsonValues[i] {
			t.Errorf("expected parsed JSON to be %s, got %s", expectedJsonValues[i], result)
		}
	}
}
