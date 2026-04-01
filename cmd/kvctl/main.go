// kvctl is the command-line client for the KV store.
//
// It connects to a running server over TCP, sends one command,
// prints the response, and exits. Think of it like the redis-cli
// equivalent for our store.
//
// Usage:
//
//	kvctl get <key>
//	kvctl put <key> <value>
//	kvctl delete <key>
//
// The --server flag (default localhost:8080) points at the running server.
// Example: kvctl --server localhost:9000 get mykey

// opens a TCP connetion to the server, sends a comnand, reads a respond and then closes connection
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"
)

// serverAddr holds the address of the KV server to connect to.
// It is set by the --server persistent flag on the root command.
var serverAddr string

// sendCommand opens a TCP connection to serverAddr, sends one command line,
// reads back the single-line response, and closes the connection.
//
// Opening a fresh connection per command keeps the client stateless and
// simple. In a production client you'd pool connections for performance,
// but that complexity isn't needed here.
func sendCommand(command string) (string, error) {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return "", fmt.Errorf("cannot connect to %s: %w", serverAddr, err)
	}
	defer conn.Close()

	// Send the command followed by a newline — the server reads line by line.
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
	// rootCmd is the top-level "kvctl" command.
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
			// args[0] = key, args[1] = value
			// Cobra joins the two positional args; the server's SplitN(3)
			// correctly handles values that contain spaces passed as one arg.
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
