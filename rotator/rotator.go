// package rotator implements a simple logfile rotator. Logs are read from an
// io.Reader and are written to a file until they reach a specified size. The
// log is then gzipped to another file and truncated.
package rotator

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// A Rotator reads log lines from an input source and writes them to a file,
// splitting it up into gzipped chunks once the filesize reaches a certain
// threshold.
type Rotator struct {
	size      int64
	threshold int64
	filename  string
	in        *bufio.Scanner
	out       *os.File
	tee       bool
	wg        sync.WaitGroup
}

// New returns a new Rotator that is ready to start rotating logs from its
// input.
func New(in io.Reader, filename string, thresholdKB int64, tee bool) (*Rotator, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &Rotator{
		size:      stat.Size(),
		threshold: 1000 * thresholdKB,
		filename:  filename,
		in:        bufio.NewScanner(in),
		out:       f,
		tee:       tee,
	}, nil
}

// Run begins reading lines from the input and rotating logs as necessary.
func (r *Rotator) Run() error {
	for r.in.Scan() {
		if r.size >= r.threshold {
			if err := r.rotate(); err != nil {
				return err
			}
		}

		line := r.in.Bytes()

		n, _ := r.out.Write(line)
		m, _ := r.out.Write([]byte{'\n'})

		if r.tee {
			os.Stdout.Write(line)
			os.Stdout.Write([]byte{'\n'})
		}

		r.size += int64(n + m)
	}

	return nil
}

// Close closes the output logfile.
func (r *Rotator) Close() error {
	err := r.out.Close()
	r.wg.Wait()
	return err
}

func (r *Rotator) rotate() error {
	dir := filepath.Dir(r.filename)
	glob := filepath.Join(dir, filepath.Base(r.filename)+".*")
	existing, err := filepath.Glob(glob)
	if err != nil {
		return err
	}

	maxNum := 0
	for _, name := range existing {
		parts := strings.Split(name, ".")
		if len(parts) < 2 {
			continue
		}
		numIdx := len(parts) - 1
		if parts[numIdx] == "gz" {
			numIdx--
		}
		num, err := strconv.Atoi(parts[numIdx])
		if err != nil {
			continue
		}
		if num > maxNum {
			maxNum = num
		}
	}

	err = r.out.Close()
	if err != nil {
		return err
	}
	rotname := fmt.Sprintf("%s.%d", r.filename, maxNum+1)
	err = os.Rename(r.filename, rotname)
	if err != nil {
		return err
	}
	r.out, err = os.OpenFile(r.filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	r.size = 0

	r.wg.Add(1)
	go func() {
		err := compress(rotname)
		if err == nil {
			os.Remove(rotname)
		}
		r.wg.Done()
	}()

	return nil
}

func compress(name string) (err error) {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	arc, err := os.OpenFile(name+".gz", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	z := gzip.NewWriter(arc)
	if _, err = io.Copy(z, f); err != nil {
		return err
	}
	if err = z.Close(); err != nil {
		return err
	}
	return arc.Close()
}
