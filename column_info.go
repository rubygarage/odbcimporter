package odbcimporter

import (
	"fmt"
	"strings"
)

type ColumnInfo struct {
	name         string
	originalType string
	columnLength interface{}
	userType     string
}

func (ci *ColumnInfo) IsValid() bool {
	return ci.originalType != "" && ci.name != ""
}

func (ci *ColumnInfo) ParsingType() string {
	columnType := strings.ToLower(strings.Trim(ci.originalType, "\"'_ "))
	if columnType == "user-defined" {
		columnType = ci.UserType()
	}
	return columnType
}

func (ci *ColumnInfo) UserType() string {
	if ci.userType == "" {
		return "varchar"
	}
	return strings.ToLower(strings.Trim(ci.userType, "\"'_ "))
}

func (ci *ColumnInfo) UserRedshiftType() string {
	return parseRedshiftType(ci.UserType(), 0)
}

func (ci *ColumnInfo) HasQuotedElements() bool {
	return strings.Contains(ci.UserRedshiftType(), "char") || strings.Contains(ci.UserRedshiftType(), "timestamp")
}

func (ci *ColumnInfo) RedshiftType() string {
	columnLength, ok := ci.columnLength.(int64)
	if !ok {
		columnLength = 0
	}
	return parseRedshiftType(ci.ParsingType(), int64(columnLength))
}

func parseRedshiftType(columnType string, columnLength int64) string {
	switch columnType {
	case "smallint", "int2":
		return "smallint"
	case "integer", "int", "int4":
		return "integer"
	case "bigint", "int8":
		return "bigint"
	case "real", "float4":
		return "real"
	case "double precision", "float8", "float", "decimal", "numeric":
		return "double precision"
	case "boolean", "bool":
		return "boolean"
	case "char", "character", "nchar", "bpchar":
		var columnMaxModifier int64 = 1
		if columnLength > 0 && columnLength < 255 {
			columnMaxModifier = columnLength
		} else if columnLength >= 255 {
			columnMaxModifier = 255
		}
		return fmt.Sprintf("char(%d)", columnMaxModifier)
	case "varchar", "character varying", "nvarchar", "text":
		var columnMaxModifier int64 = 255
		if columnLength > 0 && columnLength < 65535 {
			columnMaxModifier = columnLength
		} else if columnLength >= 65535 {
			columnMaxModifier = 65535
		}
		return fmt.Sprintf("varchar(%d)", columnMaxModifier)
	case "array", "json", "hstore", "mediumtext", "longtext":
		return "varchar(max)"
	case "timestamp", "timestamp without time zone", "datetime":
		return "timestamp"
	case "date":
		return "date"
	}
	return "varchar"
}
