package odbcimporter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parse PostgreSQL multidimensional ARRAY data
func parseArray(source string, columnInfo *ColumnInfo) string {
	return wrapFuncWithEscapingCurlyBraces(source, func(arrayString string) string {
		arrayContentRegexp := regexp.MustCompile("{([^}{]+)}")
		result := arrayContentRegexp.ReplaceAllStringFunc(arrayString, func(submatch string) string {
			arraySlice := dbArrayToSlice(submatch)
			if columnInfo.HasQuotedElements() {
				return "{" + quoteArrayElements(arraySlice) + "}"
			} else {
				return "{" + strings.Join(parseElementByType(arraySlice, columnInfo.UserRedshiftType()), ",") + "}"
			}
		})

		result = strings.Replace(result, "{", "[", -1)
		return strings.Replace(result, "}", "]", -1)
	})
}

// Parse Mysql SET type data
func parseSet(source string) string {
	if source == "" {
		return "[]"
	}
	sourceWithEscapedQuotes := strings.Replace(source, "\"", "\\\"", -1)
	return "[" + quoteArrayElements(strings.Split(sourceWithEscapedQuotes, ",")) + "]"
}

// Parse Postgres HSTORE type data
func parseHstore(source string) string {
	return "{" + strings.Replace(source, "\"=>\"", "\":\"", -1) + "}"
}

// parse elements of array by type string
func parseElementByType(array []string, elementType string) []string {
	var elements []string
	for _, value := range array {
		element := value
		if elementType == "boolean" {
			value, err := strconv.ParseBool(value)
			if err == nil {
				element = fmt.Sprintf("%t", value)
			}
		}
		elements = append(elements, element)
	}
	return elements
}

// Replace curly braces inside quoted strings and return them back after passed function call
func wrapFuncWithEscapingCurlyBraces(source string, wrappedFunc func(string) string) string {
	var substrings []string
	for i, str := range regexpSplitBySubmatchIndex(source, "(?:[^\\\\])(\")", 1) {
		if i%2 != 0 {
			str = strings.Replace(str, "{", "*zenit-open-curly-brace*", -1)
			str = strings.Replace(str, "}", "*zenit-closed-curly-brace*", -1)
		}
		substrings = append(substrings, str)
	}
	result := strings.Join(substrings, "\"")
	result = wrappedFunc(result)
	result = strings.Replace(result, "*zenit-open-curly-brace*", "{", -1)
	result = strings.Replace(result, "*zenit-closed-curly-brace*", "}", -1)
	return result
}

func quoteArrayElements(array []string) string {
	var elements []string
	for _, v := range array {
		elements = append(elements, "\""+v+"\"")
	}
	return strings.Join(elements, ",")
}

// Parse the output string from the array type.
// Regex used: (((?P<value>(([^",\\{}\s(NULL)])+|"([^"\\]|\\"|\\\\)*")))(,)?)
func dbArrayToSlice(array string) []string {
	var (
		// unquoted array values must not contain: (" , \ { } whitespace NULL)
		// and must be at least one char
		unquotedChar  = `[^",\\{}\s(NULL)]`
		unquotedValue = fmt.Sprintf("(%s)+", unquotedChar)
		// quoted array values are surrounded by double quotes, can be any
		// character except " or \, which must be backslash escaped:
		quotedChar  = `[^"\\]|\\"|\\\\`
		quotedValue = fmt.Sprintf("\"(%s)*\"", quotedChar)
		// an array value may be either quoted or unquoted:
		arrayValue = fmt.Sprintf("(?P<value>(%s|%s))", unquotedValue, quotedValue)
		// Array values are separated with a comma IF there is more than one value:
		arrayPattern = fmt.Sprintf("((%s)(,)?)", arrayValue)
	)
	return findAllByStringSubmatch(array, arrayPattern, 2)
}

func findAllByStringSubmatch(source, pattern string, submatchIndex int) []string {
	patternRegexp := regexp.MustCompile(pattern)
	results := make([]string, 0)
	matches := patternRegexp.FindAllStringSubmatch(source, -1)
	for _, match := range matches {
		element := strings.Trim(match[submatchIndex], "\"")
		results = append(results, element)
	}
	return results
}

// Split source string by delimiter regexp string and submatch index
func regexpSplitBySubmatchIndex(source, delimeter string, submatchIndex int) []string {
	splitRegexp := regexp.MustCompile(delimeter)
	indexes := splitRegexp.FindAllStringSubmatchIndex(source, -1)
	laststart := 0
	result := make([]string, len(indexes)+1)
	for i, element := range indexes {
		result[i] = source[laststart:(element[submatchIndex*2])]
		laststart = element[(submatchIndex*2)+1]
	}
	result[len(indexes)] = source[laststart:len(source)]
	return result
}
