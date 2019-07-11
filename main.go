package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	au "github.com/logrusorgru/aurora"
)

type ExitStatus int

const (
	SUCCESS ExitStatus = iota
	FAILURE
)

func (e ExitStatus) Integer() int {
	return [2]int{0, 1}[e]
}

type StatementType int

const (
	INSERT StatementType = iota
	SELECT
	INVALID
)

type Statement struct {
	t StatementType
}

func prompt() {
	fmt.Printf("%s%s ", au.BrightYellow("gobi"), au.Bold(">"))
}

func handleMetaCommand(command string) error {
	switch command {
	case ".exit":
		os.Exit(SUCCESS.Integer())
	default:
		return fmt.Errorf("Unrecognized meta command: '%s'.", command)
	}
	return nil
}

func prepareQueryCommand(command string) (statement Statement, err error) {
	if strings.HasPrefix(command, "insert") {
		statement.t = INSERT
		return statement, nil
	} else if strings.HasPrefix(command, "select") {
		statement.t = SELECT
		return statement, nil
	} else {
		statement.t = INVALID
		return statement, fmt.Errorf("Unrecognized query command: '%s'.", command)
	}
}

func executeQuery(statement Statement) error {
	switch statement.t {
	case INSERT:
		fmt.Println("Insert operation.")
		return nil
	case SELECT:
		fmt.Println("Select operation.")
		return nil
	case INVALID:
		return fmt.Errorf("Invalid operation.")
	default:
		return fmt.Errorf("Unexpected behaviour: Operation doesn't exist.")
	}
}

func handleInput(input string) error {
	if input[0] == '.' {
		err := handleMetaCommand(input)
		if err != nil {
			fmt.Println(err)
			// panic(err)
		}
	} else {
		statement, err := prepareQueryCommand(input)
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
