package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/klog/v2"
	"os"
	"os/exec"
	"strings"
)

type LogicalVolume struct {
	LogicalVolume string `json:"lv_name"`
	VolumeGroup   string `json:"vg_name"`
}

type LvReport struct {
	Report []LogicalVolume `json:"lv"`
}

type LvmReportList struct {
	Reports []LvReport `json:"report"`
}

func getVolumeVolumeGroupMapping() map[string]string {
	mapping := make(map[string]string)

	stdout, err := exec.Command(
		"lvs",
		"-o", "vg_name,lv_name",
		"--reportformat", "json",
	).Output()

	if err != nil {
		klog.ErrorS(err, "failed to get logical volume info")
		return mapping
	}

	var report LvmReportList
	err = json.Unmarshal(stdout, &report)

	if err != nil {
		klog.ErrorS(err, "failed to unmarshal logical volume info")
		return mapping
	}

	// There should only be a single report
	for _, report := range report.Reports {
		for _, lv := range report.Report {
			mapping[lv.LogicalVolume] = lv.VolumeGroup
		}
	}

	return mapping
}

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
