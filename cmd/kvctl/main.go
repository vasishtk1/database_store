// kvctl is the commnand line client in order to interact with the KV store
// Usage:
//
//	kvctl get <key>
//	kvctl put <key> <value>
//	kvctl delete <key>
// default localhost: 8080
// opens a TCP connetion to the server, sends a comnand, reads a response and then closes connection
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"
)

// serverAddr holds the address of the KV server to connect to.
var serverAddr string

// sendCommand opens a TCP connection to serverAddr, sends one command line,
// reads back the single-line response, and closes the connection.
func sendCommand(command string) (string, error) {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return "", fmt.Errorf("cannot connect to %s: %w", serverAddr, err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "%s\n", command)

	// Read exactly one response line back from the server.
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	return "", fmt.Errorf("server closed connection without responding")
}

func main() {
	// Cobra automatically generates --help output for every command.
	rootCmd := &cobra.Command{
		Use:   "kvctl",
		Short: "CLI client for the distributed KV store",
		Long:  "kvctl lets you get, put, and delete keys from the KV store over TCP.",
	}

	// --server flag is inherited by all subcommands (PersistentFlags).
	rootCmd.PersistentFlags().StringVar(
		&serverAddr, "server", "localhost:8080",
		"Address of the KV server to connect to",
	)

	// ── GET ──────────────────────────────────────────────────────────────────
	// Retrieves the value for a key. Prints NULL if the key doesn't exist.
	getCmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Retrieve the value for a key",
		Args:  cobra.ExactArgs(1), // Cobra enforces exactly 1 argument for us
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := sendCommand("GET " + args[0])
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error:", err)
				os.Exit(1)
			}
			fmt.Println(resp)
		},
	}

	// ── PUT ──────────────────────────────────────────────────────────────────
	// Stores a value under a key. The value can contain spaces.
	putCmd := &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Set a key to a value",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := sendCommand("PUT " + args[0] + " " + args[1])
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error:", err)
				os.Exit(1)
			}
			fmt.Println(resp)
		},
	}

	// ── DELETE ───────────────────────────────────────────────────────────────
	// Removes a key from the store.
	deleteCmd := &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key from the store",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := sendCommand("DELETE " + args[0])
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error:", err)
				os.Exit(1)
			}
			fmt.Println(resp)
		},
	}

	rootCmd.AddCommand(getCmd, putCmd, deleteCmd)

	// Execute parses os.Args, routes to the right subcommand, and runs it.
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
