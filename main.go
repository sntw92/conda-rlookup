package main

import (
	"conda-rlookup/helpers"
	"fmt"
	"os"
)

func main() {

	tarFlname := "testfl.tar.bz2"
	expectedSha256sum := "4fb591cefb5d624eac7245e0426c894734da27ebaca5d58a69ebc54bedc66512"
	allowedFiles := []string{
		"info/about.json",
		"info/index.json",
		"info/files",
		"info/paths.json",
	}

	f, err := os.OpenFile(tarFlname, os.O_RDONLY, 0640)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open tarfile %s: %s\n", tarFlname, err.Error())
		os.Exit(1)
	}

	s, err := helpers.TarBz2ExtractFilesAndGetSha256sum(f, "well/now", allowedFiles)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not extract file %s: %s\n", tarFlname, err.Error())
		os.Exit(2)
	}
	if s == expectedSha256sum {
		fmt.Printf("[OK] Sha's match: %s\n", s)
	} else {
		fmt.Printf("[ERROR] Sha mismatch: actual %s vs expected %s\n", s, expectedSha256sum)
		os.Exit(3)
	}
}
