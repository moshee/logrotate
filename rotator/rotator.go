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
	return r.out.Close()
}

func (r *Rotator) rotate() error {
	dir := filepath.Dir(r.filename)

	existing, err := filepath.Glob(filepath.Join(dir, r.filename+".*.gz"))
	if err != nil {
		return err
	}

	maxNum := 0
	for _, name := range existing {
		parts := strings.Split(name, ".")
		if len(parts) < 3 {
			continue
		}
		num, err := strconv.Atoi(parts[len(parts)-2])
		if err != nil {
			continue
		}
		if num > maxNum {
			maxNum = num
		}
	}

	arcName := fmt.Sprintf("%s.%d.gz", r.filename, maxNum+1)
	arcPath := filepath.Join(dir, arcName)

	arc, err := os.OpenFile(arcPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	r.out.Seek(0, os.SEEK_SET)
	z := gzip.NewWriter(arc)
	if _, err = io.Copy(z, r.out); err != nil {
		return err
	}
	if err = z.Close(); err != nil {
		return err
	}
	if err := arc.Close(); err != nil {
		return err
	}
	if err = r.out.Close(); err != nil {
		return err
	}

	r.out, err = os.OpenFile(r.filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	r.size = 0

	return nil
}
