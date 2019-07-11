package main

import (
	"fmt"
	"os"

	au "github.com/logrusorgru/aurora"
)

func prompt() {
	fmt.Printf("%s%s ", au.BrightYellow("gobi"), au.Bold(">"))
}

func handleInput(input string) (err error) {
	switch input {
	case ".exit":
		os.Exit(0)
		return nil
	default:
		s := fmt.Sprintf("Unrecognized command: '%s'.", input)
		return fmt.Errorf("%s", au.Red(s))
	}
}

func main() {
	var input string

	for {
		prompt()
		fmt.Scanln(&input)

		err := handleInput(input)
		if err != nil {
			fmt.Println(err)
			// panic(err)
		}
	}
}
