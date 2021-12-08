package helper

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"digi.dev/digi/api"
	"digi.dev/digi/pkg/core"
	"github.com/creack/pty"
	"golang.org/x/term"
	"gopkg.in/yaml.v2"
)

var homeDir string

func init() {
	var err error
	homeDir, err = os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	homeDir = filepath.Join(homeDir, ".digi")
}

func RunMake(args map[string]string, cmd string, quiet bool) error {
	cmd_ := exec.Command("make", "-s", "--ignore-errors", cmd)
	cmd_.Env = os.Environ()

	for k, v := range args {
		cmd_.Env = append(cmd_.Env,
			fmt.Sprintf("%s=%s", k, v),
		)
	}

	if os.Getenv("WORKDIR") == "" {
		curDir, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		cmd_.Env = append(cmd_.Env,
			fmt.Sprintf("WORKDIR=%s", curDir),
		)
	}
	cmd_.Dir = homeDir

	ptmx, err := pty.Start(cmd_)
	if err != nil {
		panic(err)
	}
	defer func() { _ = ptmx.Close() }()

	// Start a shell session: github.com/creack/pty
	// Handle pty size.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	go func() {
		for range ch {
			if err := pty.InheritSize(os.Stdin, ptmx); err != nil {
				log.Printf("error resizing pty: %s", err)
			}
		}
	}()
	ch <- syscall.SIGWINCH                        // Initial resize.
	defer func() { signal.Stop(ch); close(ch) }() // Cleanup signals when done.

	// Set stdin in raw mode.
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }() // Best effort.

	// Copy stdin to the pty and the pty to stdout.
	// NOTE: The goroutine will keep reading until the next keystroke before returning.
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()

	_, _ = io.Copy(os.Stdout, ptmx)

	return nil
}

func CreateAlias(kind, name, namespace string) {
	var workDir string
	if workDir = os.Getenv("WORKDIR"); workDir == "" {
		workDir = "."
	}

	// TBD parse gvr in separate method
	type gvr struct {
		Group   string `yaml:"group,omitempty"`
		Version string `yaml:"version,omitempty"`
		Kind    string `yaml:"kind,omitempty"`
	}

	raw := gvr{}
	modelFile, err := ioutil.ReadFile(filepath.Join(workDir, kind, "model.yaml"))
	if err != nil {
		log.Printf("unable to create alias, cannot open model file: %v", err)
	}

	err = yaml.Unmarshal(modelFile, &raw)
	if err != nil {
		log.Fatalf("unable to create alias, cannot unmarshal model file: %v", err)
	}

	auri := &core.Auri{
		Kind: core.Kind{
			Group:   raw.Group,
			Version: raw.Version,
			Name:    raw.Kind,
		},
		Name:      name,
		Namespace: namespace,
	}
	alias := api.Alias{
		Name: name,
		Auri: auri,
	}

	if err := alias.Set(); err != nil {
		log.Fatalf("unable to create alias %v", err)
	}
}
