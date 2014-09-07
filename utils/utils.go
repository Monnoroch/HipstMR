package utils

import (
	"fmt"
)

type Runnable interface {
	Run() error
	RunProcess(binaryPath string) (string, string, error)
}

func Go(self Runnable, sig chan struct{}) {
	go func() {
		if err := self.Run(); err != nil {
			fmt.Println("Error Go:", err)
		}
		sig <- struct{}{}
	}()
}

func GoForever(self Runnable) {
	go func() {
		if err := self.Run(); err != nil {
			fmt.Println("Error GoForever:", err)
		}
		GoForever(self)
	}()
}

func RunProcessDebug(self Runnable, binaryPath string) error {
	sout, serr, err := self.RunProcess(binaryPath)
	if serr != "" {
		fmt.Println("~~~~~~Stderr:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		fmt.Print(serr)
	}
	if sout != "" {
		fmt.Println("~~~~~~Stdout:~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		fmt.Print(sout)
	}
	if serr != "" || sout != "" {
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	}
	return err
}

func GoProcess(self Runnable, binaryPath string, sig chan struct{}) {
	go func() {
		if _, _, err := self.RunProcess(binaryPath); err != nil {
			fmt.Println("Error GoProcess:", err)
		}
		sig <- struct{}{}
	}()
}

func GoProcessDebug(self Runnable, binaryPath string, sig chan struct{}) {
	go func() {
		if err := RunProcessDebug(self, binaryPath); err != nil {
			fmt.Println("Error GoProcessDebug:", err)
		}
		sig <- struct{}{}
	}()
}

func GoProcessForever(self Runnable, binaryPath string) {
	go func() {
		if _, _, err := self.RunProcess(binaryPath); err != nil {
			fmt.Println("Error GoProcessForever:", err)
		}
		GoProcessForever(self, binaryPath)
	}()
}

func GoProcessDebugForever(self Runnable, binaryPath string) {
	go func() {
		if err := RunProcessDebug(self, binaryPath); err != nil {
			fmt.Println("Error GoProcessDebugForever:", err)
		}
		GoProcessDebugForever(self, binaryPath)
	}()
}
