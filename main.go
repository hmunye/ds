package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/hmunye/ds/messenger"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Bytes()

		msg, err := messenger.ParseMessage(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			continue
		}

		fmt.Println(msg.FormatMessage())
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
	}
}
