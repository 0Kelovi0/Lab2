package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// task — діапазон індексів лівої половини "актуальної" частини масиву
// довжиною n. Воркер сам обчислює симетричний індекс n-1-i.
type task struct {
	lo, hi int // [lo, hi)
	n      int // актуальна довжина масиву на цій хвилі
}

func process(a []int64, t task) {
	last := t.n - 1
	for i := t.lo; i < t.hi; i++ {
		a[i] += a[last-i]
	}
}

// reduce виконує хвилі згортання й повертає суму.
// Контролер (ця функція) роздає задачі воркерам і синхронізує хвилі.
func reduce(a []int64, workers, chunk int) int64 {
	tasks := make(chan task, workers*2)

	var poolWG sync.WaitGroup // життєвий цикл воркерів
	var waveWG sync.WaitGroup // бар'єр між хвилями

	for range workers {
		poolWG.Add(1)
		go func() {
			defer poolWG.Done()
			for t := range tasks {
				process(a, t)
				waveWG.Done()
			}
		}()
	}

	n := len(a)
	for n > 1 {
		half := n / 2

		// На малих хвилях зменшуємо чанк, щоб задіяти всіх воркерів.
		c := min(chunk, max((half+workers-1)/workers, 1024))

		for lo := 0; lo < half; lo += c {
			waveWG.Add(1)
			tasks <- task{lo: lo, hi: min(lo+c, half), n: n}
		}
		waveWG.Wait() // усі пари хвилі пораховані

		n -= half // актуальна довжина: ceil(n/2)
	}

	close(tasks) // воркери завершуються, витоків немає
	poolWG.Wait()

	return a[0]
}

// fill паралельно заповнює масив: a[i] = i%10 + 1.
func fill(a []int64, workers int) {
	var wg sync.WaitGroup
	step := (len(a) + workers - 1) / workers
	for lo := 0; lo < len(a); lo += step {
		hi := min(lo+step, len(a))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				a[i] = int64(i%10) + 1
			}
		}()
	}
	wg.Wait()
}

// expected — еталонна сума для заповнення a[i] = i%10 + 1.
func expected(n int) int64 {
	q, r := int64(n/10), int64(n%10)
	return q*55 + r*(r+1)/2
}

func main() {
	n := flag.Int("n", 1_000_000_000, "кількість елементів масиву")
	workers := flag.Int("workers", runtime.NumCPU(), "кількість робочих потоків")
	chunk := flag.Int("chunk", 1<<18, "макс. розмір діапазону індексів для одного завдання")
	flag.Parse()

	if *n < 1 || *workers < 1 || *chunk < 1 {
		fmt.Fprintln(os.Stderr, "n, workers і chunk мають бути > 0")
		os.Exit(2)
	}

	fmt.Printf("n=%d (~%.1f ГБ), workers=%d, chunk=%d\n",
		*n, float64(*n)*8/(1<<30), *workers, *chunk)

	a := make([]int64, *n)
	fill(a, *workers)

	start := time.Now()
	sum := reduce(a, *workers, *chunk)
	elapsed := time.Since(start)

	want := expected(*n)
	fmt.Printf("сума:     %d\nочікувано: %d\nзбіг:     %v\nчас:      %v\n",
		sum, want, sum == want, elapsed)
}
