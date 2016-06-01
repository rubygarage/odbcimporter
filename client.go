package odbcimporter

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/zenitmedia/importer/domains"
)

const dateLayout = "2006-01-02"
const postgresLayout = "postgresql://%s:%s@%s:%s/%s?sslmode=disable"
const mysqlLayout = "%s:%s@tcp(%s:%s)/%s"

type Client struct {
	domains.Credentials
	httpClient *http.Client
	url        string
}

func NewClient(credentials domains.Credentials) *Client {
	return &Client{
		Credentials: credentials,
		httpClient:  &http.Client{},
	}
}

func ColumnInfoRow(columnName string, columnInfo *ColumnInfo) interface{} {
	switch columnName {
	case "column_name":
		return &columnInfo.name
	case "character_maximum_length":
		return &columnInfo.columnLength
	case "data_type":
		return &columnInfo.originalType
	case "udt_name":
		return &columnInfo.userType
	default:
		panic("unknown column " + columnName)
	}
}

func (this *Client) Events(events chan<- domains.Event) (int, error) {
	defer close(events)

	adapters := map[string]string{
		"0": "mysql",
		"2": "postgresql_unicode",
		"3": "redshift",
	}
	adapter := adapters[this.Credentials["adapter"]]
	driverName := "postgres"
	if adapter == "mysql" {
		driverName = "mysql"
	}
	dbLayout := postgresLayout
	if adapter == "mysql" {
		dbLayout = mysqlLayout
	}
	dbUrl := fmt.Sprintf(dbLayout,
		this.Credentials["username"],
		this.Credentials["password"],
		this.Credentials["host"],
		this.Credentials["port"],
		this.Credentials["database_name"],
	)
	schemaName := "public"
	if adapter == "mysql" {
		schemaName = this.Credentials["database_name"]
	}
	db, err := sql.Open(driverName, dbUrl)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return 0, err
	}

	delimiter := `"`
	if adapter == "mysql" {
		delimiter = "`"
	}

	limit := 10000
	if adapter == "redshift" {
		limit = 0
	}

	rows, err := db.Query(schemaTablesQuery(schemaName))
	if err != nil {
		return 0, err
	}

	var eventsCount int
	defer rows.Close()
	for rows.Next() {
		var tableString string
		rows.Scan(&tableString)

		table := strings.TrimSpace(tableString)
		if len(table) == 0 {
			continue
		}

		columnsSchema, tableSchema, err := columnsAndTableSchema(db, schemaName, table, adapter)
		if err != nil {
			return 0, err
		}
		count, err := countTableRecords(db, table, delimiter)
		if err != nil {
			return 0, err
		}
		if count == 0 {
			continue
		}

		var primaryKeys []string
		if limit != 0 {
			primaryKeys, err = primaryKeyColumns(db, schemaName, table, delimiter)
			if err != nil {
				return 0, err
			}
		}

		if limit == 0 || len(primaryKeys) > 0 {
			pagesCount := 1
			if limit != 0 {
				pagesCount = count/limit + 1
			}

			for page := 0; page < pagesCount; page++ {
				count, err := sendTableEvents(db, events, primaryKeys, table, delimiter, adapter, tableSchema, columnsSchema, page, limit)
				if err != nil {
					return 0, err
				}
				eventsCount += count
			}
		}
	}

	return eventsCount, nil
}

func eventDataByColumnName(datum string, columnInfo *ColumnInfo) string {
	switch columnInfo.ParsingType() {
	case "array":
		return parseArray(datum, columnInfo)
	case "set":
		return parseSet(datum)
	case "hstore":
		return parseHstore(datum)
	}
	return datum
}

func primaryKeyQuery(schemaName, table string) string {
	return fmt.Sprintf(fmt.Sprintf(`
		SELECT k.column_name
		FROM information_schema.table_constraints t
		JOIN information_schema.key_column_usage k
		USING (constraint_name, table_schema, table_name)
		WHERE t.constraint_type='PRIMARY KEY'
			AND t.table_schema='%s'
			AND t.table_name='%s'
	`, schemaName, table))
}

func schemaTablesQuery(schemaName string) string {
	return fmt.Sprintf(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema='%[1]s'
			AND table_type='BASE TABLE'
	`, schemaName)
}

func tableSchemaQuery(schemaName, tableName, adapter string) string {
	columns := "column_name, character_maximum_length, data_type"
	if adapter == "postgresql_unicode" {
		columns = columns + ", udt_name"
	}
	return fmt.Sprintf(`
		SELECT %s
		FROM information_schema.columns
		WHERE table_schema='%s'
			AND table_name='%s'
		ORDER BY ordinal_position ASC
	`, columns, schemaName, tableName)
}

func eventsQuery(table, delimiter, adapter string, primaryKeyColumns []string, page, limit int, tableSchema map[string]string) string {
	paginatingClause := ""
	if limit != 0 {
		offset := limit * page
		paginatingClause = fmt.Sprintf(
			"ORDER BY %s LIMIT %d OFFSET %d", strings.Join(primaryKeyColumns, ", "), limit, offset,
		)
	}

	var columns []string
	for columnName := range tableSchema {
		columnName = delimiter + columnName + delimiter
		columns = append(columns, columnName)
	}

	return fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(columns, ", "), delimiter+table+delimiter, paginatingClause)
}

func countTableRecords(db *sql.DB, table, delimiter string) (count int, err error) {
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s`, delimiter+table+delimiter)
	countRow := db.QueryRow(countQuery)
	err = countRow.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func columnsAndTableSchema(db *sql.DB, schemaName, table, adapter string) (map[string]*ColumnInfo, map[string]string, error) {
	rows, err := db.Query(tableSchemaQuery(schemaName, table, adapter))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	tableSchema := make(map[string]string)
	columnsSchema := make(map[string]*ColumnInfo)
	for rows.Next() {
		cols, _ := rows.Columns()
		columnInfo := ColumnInfo{}
		data := make([]interface{}, len(cols))
		for i, column := range cols {
			data[i] = ColumnInfoRow(column, &columnInfo)
		}
		rows.Scan(data...)
		if !columnInfo.IsValid() {
			continue
		}

		columnsSchema[columnInfo.name] = &columnInfo
		tableSchema[columnInfo.name] = columnInfo.RedshiftType()
	}
	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}
	return columnsSchema, tableSchema, nil
}

func primaryKeyColumns(db *sql.DB, schemaName, table, delimiter string) (primaryKeys []string, err error) {
	rows, err := db.Query(primaryKeyQuery(schemaName, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var primaryKeyPart string
		rows.Scan(&primaryKeyPart)
		primaryKeys = append(primaryKeys, delimiter+primaryKeyPart+delimiter)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return primaryKeys, nil
}

func sendTableEvents(db *sql.DB, events chan<- domains.Event, primaryKeys []string, table, delimiter, adapter string, tableSchema map[string]string, columnsSchema map[string]*ColumnInfo, page, limit int) (int, error) {
	rows, err := db.Query(eventsQuery(table, delimiter, adapter, primaryKeys, page, limit, tableSchema))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var eventBuffer []domains.Event
	for rows.Next() {
		cols, _ := rows.Columns()
		data := make([]interface{}, len(cols), len(cols))
		values := make([]interface{}, len(cols), len(cols))
		for i, _ := range cols {
			data[i] = &(values[i])
		}
		rows.Scan(data...)
		event := domains.Event{}
		for i, col := range cols {
			datum, ok := (values[i]).([]uint8)
			if ok {
				event[col] = eventDataByColumnName(string(datum), columnsSchema[col])
			} else {
				event[col] = values[i]
			}
		}
		event["zenit_table_suffix"] = table
		event["zenit_table_schema"] = tableSchema
		eventBuffer = append(eventBuffer, event)
	}
	err = rows.Err()
	if err != nil {
		return 0, err
	}
	for _, event := range eventBuffer {
		events <- event
	}
	return len(eventBuffer), nil
}
