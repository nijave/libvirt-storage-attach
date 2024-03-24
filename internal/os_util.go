package internal

import (
	"errors"
	"fmt"
	"k8s.io/klog/v2"
	"os"
	"os/exec"
	"strings"
)

func processOutput(cmd *exec.Cmd) (string, string, error) {
	klog.InfoS("processing output", "command", cmd.Args)
	bytesOut, err := cmd.Output()
	stdout := strings.TrimRight(string(bytesOut), "\n\t\r ")

	stderr := ""
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		stderr = strings.TrimRight(string(exitError.Stderr), "\n\t\r ")
	}

	return stdout, stderr, err
}

func writeTempFile(suffix, contents string) (string, error) {
	file, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("%s-", suffix))
	if err != nil {
		return "", err
	}
	file.WriteString(contents)
	file.Close()
	return file.Name(), nil
}
