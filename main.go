package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	_ "log"
	"os"
	"strings"

	au "github.com/logrusorgru/aurora"
)

// Database schema
const columnIDSize = 4
const columnUsernameSize = 32
const columnEmailSize = 255
const rowSize = columnIDSize + columnUsernameSize + columnEmailSize

const idOffset = 0
const usernameOffset = idOffset + columnIDSize
const emailOffset = usernameOffset + columnUsernameSize

// Database internals
const pageSize = 4096
const tableMaxPages = 100
const rowsPerPage = int(pageSize / rowSize)
const tableMaxRows = tableMaxPages * rowsPerPage

// Table : table, stores `Row`s
type Table struct {
	numRows int32
	pages   [tableMaxPages * pageSize]byte
}

var table Table

func (t Table) rowSlot(rowNum int) int {
	pageNum := int(rowNum / rowsPerPage)

	rowOffset := rowNum % rowsPerPage
	byteOffset := rowOffset * rowSize

	return pageNum*rowsPerPage + byteOffset
}

// ExitStatus stores status codes for exits
type ExitStatus int

const (
	// SUCCESS : Successful execution
	SUCCESS ExitStatus = iota
	// FAILURE : Error in execution
	FAILURE
)

// Integer returns status code for ExitStatus
func (e ExitStatus) Integer() int {
	return [2]int{0, 1}[e]
}

// StatementType : Plausible statement types encountered during parsing
type StatementType int

const (
	// INSERT Statement
	INSERT StatementType = iota
	// SELECT Statement
	SELECT
	// ERROR occured in parsing of statement
	ERROR
	// INVALID Statement
	INVALID
)

// Row represents table rows for database
type Row struct {
	id       uint32
	username string
	email    string
}

// Serialize serailizes `Row` to `SerialisedRow`
func (r Row) Serialize() SerializedRow {
	s := SerializedRow{}

	binary.LittleEndian.PutUint32(s.id[:], r.id)
	copy(s.username[:], []byte(r.username))
	copy(s.email[:], []byte(r.email))

	// log.Println("Row:", r, "Serialized:", s)

	return s
}

// SerializedRow is serialized `Row`
type SerializedRow struct {
	id       [columnIDSize]byte
	username [columnUsernameSize]byte
	email    [columnEmailSize]byte
}

// Deserialize deserailizes `SerialisedRow` to `Row`
func (s SerializedRow) Deserialize() Row {
	r := Row{}

	r.id = uint32(binary.LittleEndian.Uint32(s.id[:]))
	r.username = string(s.username[:])
	r.email = string(s.email[:])

	return r
}

func (s SerializedRow) toByteArray() [rowSize]byte {
	b := [rowSize]byte{}

	copy(b[:usernameOffset], s.id[:])
	copy(b[usernameOffset:emailOffset], s.username[:])
	copy(b[emailOffset:], s.email[:])

	// log.Println("Serialised:", s, "byteArray:", b)

	return b
}

// Statement is a structure to store parsed statements
type Statement struct {
	t   StatementType
	row [rowSize]byte
}

func decodeRow(row []byte) Row {
	sr := SerializedRow{}

	copy(sr.id[:], row[:usernameOffset])
	copy(sr.username[:], row[usernameOffset:emailOffset])
	copy(sr.email[:], row[emailOffset:])

	r := sr.Deserialize()

	// log.Println("decodedRow:", r)

	return r
}

func prompt() {
	fmt.Printf("%s%s ", au.BrightYellow("gobi"), au.Bold(">"))
}

func handleMetaCommand(command string) error {
	switch command {
	case ".exit":
		os.Exit(SUCCESS.Integer())
	default:
		return fmt.Errorf("Unrecognized meta command: '%s'", command)
	}
	return nil
}

func prepareStatement(command string) (statement Statement, err error) {
	if strings.HasPrefix(command, "insert") {
		r := Row{}
		_, err := fmt.Sscanf(command,
			"insert %d %s %s",
			&r.id,
			&r.username,
			&r.email)

		if err != nil {
			statement.t = ERROR
		} else {
			statement.t = INSERT
			statement.row = r.Serialize().toByteArray()
		}

		return statement, nil
	} else if strings.HasPrefix(command, "select") {
		statement.t = SELECT
		return statement, nil
	} else {
		statement.t = INVALID
		return statement, fmt.Errorf("Unrecognized query command: '%s'", command)
	}
}

func executeQuery(statement Statement) error {
	switch statement.t {
	case INSERT:
		err := executeInsert(statement)
		if err != nil {
			return err
		}

		fmt.Println("Executed.")
		return nil

	case SELECT:
		executeSelect(statement)

		fmt.Println("Executed.")
		return nil

	case INVALID:
		return fmt.Errorf("Invalid operation")

	case ERROR:
		return fmt.Errorf("Syntax error")

	default:
		return fmt.Errorf("Unexpected behaviour: Operation doesn't exist")
	}
}

func executeInsert(statement Statement) error {
	if int(table.numRows) >= tableMaxRows {
		return fmt.Errorf("Table capacity exceeded")
	}

	row := statement.row
	loc := table.rowSlot(int(table.numRows))

	copy(table.pages[loc:], row[:])
	table.numRows++

	return nil
}

func executeSelect(statement Statement) error {
	r := Row{}

	for i := 0; i < int(table.numRows); i++ {
		slot := table.rowSlot(i)
		r = decodeRow(table.pages[slot : slot+rowSize])

		fmt.Println(r)
	}

	return nil
}

func handleInput(input string) error {
	if input[0] == '.' {
		err := handleMetaCommand(input)
		if err != nil {
			fmt.Println(err)
			// panic(err)
		}
	} else {
		statement, err := prepareStatement(input)
		if err != nil {
			fmt.Println(err)
			// panic(err)
		}

		err = executeQuery(statement)
		if err != nil {
			fmt.Println(err)
			// panic(err)
		}
	}

	return nil
}

func main() {
	var input string

	scanner := bufio.NewScanner(os.Stdin)
	table.numRows = 0

	for {
		prompt()
		if scanner.Scan() {
			input = scanner.Text()
		}

		err := handleInput(input)
		if err != nil {
			fmt.Println(err)
			// panic(err)
		}
	}
}
