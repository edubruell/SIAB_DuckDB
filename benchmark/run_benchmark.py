"""
Run the prep at several delivery sizes and record what each run cost.

Five configurations, all over the same synthetic delivery:

  r-duckdb            R/run_testdata.R: dplyr and dbplyr, DuckDB doing the work
  py-duckdb-memory    python/main.py over a DuckDB store, in-memory handover
  py-duckdb-parquet   the same, handing each table over as a Parquet file
  py-parquet          python/main.py over a folder of Parquet tables
  stata               the original prep, benchmark/stata/bench_pipeline.do

Each run is a subprocess of its own under `/usr/bin/time -l`, so the peak
resident memory it reports is that run's and nothing else's. What comes out is
two CSV files in the results folder: one row per run in `runs.csv`, and one row
per step in `steps.csv`, read off the timestamps both arms already write into
their per-step logs.

Each configuration gets its own copy of the store, because a run overwrites the
`data` table it works in. On APFS a copy is a clone and costs nothing until one
of the two is written to.

Sizes are given as copies of the FDZ test delivery, which is 577,776 rows. The
real SIAB 7523 v2 core file is 82,389,923 rows, which is 143 copies.

A copy count that matches the real core file does not give a real delivery's
shape, and two differences decide how a row here may be read:

  * **The rows are fatter.** The test core file is 95.5 bytes a row -- 46
    variables, 21 `int8`, 19 `int16`, 3 `int32`, 3 `double` -- where a real SIAB
    7514 is 2.7 GB over 52M rows, 51.9 bytes a row. Stacking copies inherits the
    fat row, so a disk or memory figure from this fixture runs about 1.84x a real
    delivery's. Wall clock and the ranking between arms are unaffected, because
    all five arms read the same rows; a claim of the form "this arm runs out of
    memory at size X" is affected, and measures the test data's storage types
    rather than the arm.
  * **The establishment side overshoots.** The test delivery carries 292,693
    `bhp_basis_v1` rows per 577,776 core rows, a ratio of 0.51, where a real
    delivery carries 16,817,726 per 82,389,923, a ratio of 0.20. At 143 copies
    the core lands within 0.3 percent of a real SIAB while `bhp_basis` reaches
    41.9M rows against the real 16.8M. The merges are over-stressed, not
    under-stressed, which is the safe direction to be wrong in.

For calibration: the original Stata prep has been run over a real 7514 on a
16 GB machine, at most 308 bytes an input row, against 535 bytes an input row
measured here at ten copies.

Two further gaps measured on 2026-09-21 are closed. The generated delivery is
written at the source's own width rather than twice it, and both read-ins cast
each Stata integer to the width the delivery declared rather than to 32 bits,
which took an `orig` row from 200 bytes to 109. The 535 bytes an input row above
predates both and is the last figure on this page that does.

Run it with:

  uv run --project python python benchmark/run_benchmark.py --copies 1 10 \
      --root ~/data/siab_bench

`--root` is where the fixtures go. Keep it outside any folder that syncs.

Author(s): Eduard Brüll
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
import threading
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent

# The size of one copy, counted in the FDZ test delivery's core spell file,
# and the size of the real SIAB 7523 v2 core file the copies stand in for.
ROWS_PER_COPY = 577_776
REAL_SIAB_ROWS = 82_389_923

CONFIGURATIONS = ("r-duckdb", "py-duckdb-memory", "py-duckdb-parquet",
                  "py-parquet", "stata")

# `/usr/bin/time -l` prints this line on macOS, in bytes.
PEAK_RSS = re.compile(r"^\s*(\d+)\s+maximum resident set size", re.MULTILINE)

# What each arm prints when it is done. The R runner prints a summary block,
# the Python runner one sentence.
R_ROWS = re.compile(r"^\.?\s*rows:\s*(\d+)", re.MULTILINE)
PY_ROWS = re.compile(r"Pipeline finished, (\d+) rows")

# What a Stata batch log says when a do-file stopped: `r(111);` on its own line.
STATA_ERROR = re.compile(r"^r\((\d+)\);", re.MULTILINE)

# The timestamp both arms put on every log line: `INFO [2026-09-18 19:31:59]`.
LOG_STAMP = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")


def folder_bytes(path: Path) -> int:
    """How much disk a store takes, whether it is a file or a folder."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def store_bytes(path: Path) -> int:
    """The store and, for a DuckDB one, the write-ahead log beside it.

    A run holds everything it has not checkpointed in the log, so a reading
    that leaves it out is not the disk the run is using.
    """
    return folder_bytes(path) + folder_bytes(Path(f"{path}.wal"))


class DiskWatch:
    """Sample a store's size while a run works in it, for its high-water mark.

    Both arms now checkpoint after every step and compact the store at the end
    of the prep, so the file a run leaves behind is no longer the largest it
    ever was: `store_bytes_after` measures the delivered store and this
    measures the disk the run needed to produce it. A quarter-second sample
    costs one `stat()` per file and misses nothing that lasts a step.
    """

    def __init__(self, path: Path, interval: float = 0.25):
        self.path, self.interval, self.peak = path, interval, 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        while not self._stop.is_set():
            try:
                self.peak = max(self.peak, store_bytes(self.path))
            except OSError:
                pass
            self._stop.wait(self.interval)

    def __enter__(self) -> "DiskWatch":
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, store_bytes(self.path))


def clone(source: Path, target: Path) -> None:
    """Copy a store, using an APFS clone where the filesystem offers one."""
    if target.exists():
        shutil.rmtree(target) if target.is_dir() else target.unlink()
    cloned = subprocess.run(["cp", "-c", "-R", str(source), str(target)])
    if cloned.returncode != 0:
        # `-c` is APFS's clone and is not everywhere. A plain copy is the same
        # store, and costs the disk the clone would have saved.
        subprocess.run(["cp", "-R", str(source), str(target)], check=True)


def step_times(log_dir: Path) -> list[tuple[str, int, int]]:
    """How long each step took, from the last stamp in each step's log.

    A step's time is its own finish minus the step before it, with the first
    step measured from the earliest stamp of the run. Taking it from the first
    and last stamp inside one log would be wrong for the R arm: `R/run_testdata.R`
    builds the whole chain as one pipe, and R evaluates a pipe's argument only
    when the function uses it, so every R step writes its opening line while
    the pipe is being built, in reverse order, and only its later lines while
    it runs. Finish stamps are in pipeline order in all three arms.

    All three stamp to the second, which is nothing at the sizes this script
    exists for and is not worth a change to any arm to improve. The span from
    a log's own first to last line is kept beside the step time, because for
    the Python and Stata arms it is the step's own duration and for the R arm
    it shows the laziness.
    """
    logs = []
    for path in sorted(log_dir.glob("*.log")):
        stamps = LOG_STAMP.findall(path.read_text(errors="replace"))
        if not stamps:
            continue
        logs.append((
            path.stem,
            datetime.strptime(stamps[0], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(stamps[-1], "%Y-%m-%d %H:%M:%S"),
        ))
    if not logs:
        return []

    logs.sort(key=lambda entry: entry[2])
    previous = min(entry[1] for entry in logs)
    times = []
    for name, first, last in logs:
        times.append((name, int((last - previous).total_seconds()),
                      int((last - first).total_seconds())))
        previous = last
    return times


def build_fixture(root: Path, copies: int, need_parquet: bool,
                  need_stata: bool, seed: int) -> dict[str, Path]:
    """Write the delivery and the stores one size needs, if they are not there.

    The DuckDB store is built once and cloned per run; the Parquet store is a
    second build, because the two hold their tables in different shapes.
    """
    delivery = root / f"delivery_{copies}x"
    duckdb_store = root / f"orig_{copies}x.duckdb"
    parquet_store = root / f"orig_{copies}x_parquet"

    if not duckdb_store.exists() or not delivery.exists():
        command = [sys.executable, str(HERE / "make_delivery.py"),
                   "--copies", str(copies),
                   "--store", str(duckdb_store),
                   "--delivery", str(delivery),
                   "--seed", str(seed)]
        if need_stata:
            command.append("--core-dta")
        subprocess.run(command, check=True)

    if need_parquet and not parquet_store.exists():
        subprocess.run([sys.executable, str(HERE / "make_delivery.py"),
                        "--copies", str(copies),
                        "--store", str(parquet_store),
                        "--seed", str(seed)], check=True)

    return {"delivery": delivery, "duckdb": duckdb_store,
            "parquet": parquet_store}


def run_command(command: list[str], environment: dict[str, str],
                output_file: Path, cwd: Path = PROJECT,
                watch: Path | None = None) -> tuple[float, int, int, str]:
    """Run one configuration and give back wall clock, peak memory, peak disk
    and output.

    `/usr/bin/time -l` writes its report to standard error after the command
    it wrapped, so both streams are kept and searched together. `watch` is the
    store the run works in, sampled while it runs for the high-water mark.
    """
    started = datetime.now()
    with DiskWatch(watch) if watch is not None else nullcontext() as disk:
        finished = subprocess.run(["/usr/bin/time", "-l", *command],
                                  cwd=str(cwd),
                                  env={**os.environ, **environment},
                                  capture_output=True, text=True)
    seconds = (datetime.now() - started).total_seconds()
    peak_disk = disk.peak if disk is not None else 0
    output = finished.stdout + finished.stderr
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(output)

    match = PEAK_RSS.search(output)
    peak = int(match.group(1)) if match else 0
    if finished.returncode != 0:
        return seconds, peak, peak_disk, f"failed, exit {finished.returncode}"
    return seconds, peak, peak_disk, "ok"


def run_configuration(name: str, copies: int, paths: dict[str, Path],
                      results: Path, seed: int) -> dict:
    """Run one arm at one size and collect everything worth recording."""
    run_root = results / f"{copies}x" / name
    log_dir = run_root / "log"
    # A rerun into the same results folder must not read the previous run's
    # logs: a step dropped from the chain would otherwise still show up in
    # steps.csv, with its old time.
    if log_dir.exists():
        shutil.rmtree(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    store = run_root / ("store.duckdb" if name != "py-parquet" else "store")

    if name == "stata":
        work = run_root / "work"
        work.mkdir(parents=True, exist_ok=True)
        stata = os.environ.get(
            "SIAB_STATA",
            "/Applications/Stata/StataMP.app/Contents/MacOS/stata-mp")
        environment = {
            "SIAB_ROOT": str(PROJECT),
            "SIAB_TEST_DATA": str(paths["delivery"]),
            "SIAB_STATA_WORK": str(work),
        }

        # The Stata arm needs the delivery as one file. The generator writes
        # the core one copy at a time, because it holds a whole frame in
        # memory to write it; Stata appends them. It is not part of the
        # timing: a real delivery arrives as one file already.
        core = paths["delivery"] / "SIAB_7523_v2.dta"
        if not core.exists():
            print("  stata: appending the per-copy core files")
            run_command([stata, "-b", "do",
                         str(HERE / "stata" / "append_copies.do")],
                        environment, run_root / "append.txt", cwd=run_root)
            if not core.exists():
                print("  stata: the append did not produce a core file, "
                      "which is this size not fitting in Stata's memory")
                return {"config": name, "copies": copies,
                        "rows_in": copies * ROWS_PER_COPY, "rows_out": 0,
                        "wall_seconds": 0.0, "peak_rss_bytes": 0,
                        "store_bytes_before": 0, "store_bytes_after": 0,
                        "store_bytes_peak": 0, "status": "append failed"}

        command = [stata, "-b", "do",
                   str(HERE / "stata" / "bench_pipeline.do")]
    else:
        clone(paths["parquet"] if name == "py-parquet" else paths["duckdb"],
              store)
        if name == "r-duckdb":
            command = ["Rscript", str(PROJECT / "R" / "run_testdata.R")]
            environment = {"SIAB_TEST_DB": str(store),
                           "SIAB_TEST_DATA": str(paths["delivery"])}
        else:
            command = [sys.executable, str(PROJECT / "python" / "main.py")]
            environment = {"SIAB_DB": str(store),
                           "SIAB_RAW": str(paths["delivery"]),
                           "SIAB_LOG": str(log_dir),
                           "SIAB_BOUNDARY": ("parquet"
                                             if name == "py-duckdb-parquet"
                                             else "memory")}

    before = store_bytes(store)
    print(f"  {name}: running")
    started = datetime.now().timestamp()
    # The Stata arm keeps no store: it works in .dta files under its own
    # working folder, so that folder is what its disk figure measures.
    seconds, peak, peak_disk, status = run_command(
        command, environment, run_root / "console.txt",
        cwd=run_root if name == "stata" else PROJECT,
        watch=(run_root / "work") if name == "stata" else store)

    # The R runner writes its step logs into the project's own log folder,
    # which it does not take from the environment, and the Stata run writes
    # them beside its working folder. Both are copied here, so that every run
    # keeps its own logs whichever arm produced them. A log older than this run
    # is a leftover from an earlier one and is left where it is.
    if name in ("r-duckdb", "stata"):
        source = (PROJECT / "log") if name == "r-duckdb" else (run_root / "work" / "log")
        for log in source.glob("*.log"):
            if log.stat().st_mtime >= started:
                shutil.copy2(log, log_dir / log.name)

    output = (run_root / "console.txt").read_text()

    # A batch Stata run says nothing on the console: both the row count and the
    # error code, if there is one, are in the log it writes beside itself.
    # Stata exits 0 whatever happened, so without reading that log a do-file
    # that stopped at its first step is recorded as a run that finished in a
    # second.
    if name == "stata":
        log = run_root / "bench_pipeline.log"
        output = log.read_text(errors="replace") if log.exists() else output
        error = STATA_ERROR.search(output)
        if error:
            status = f"failed, Stata r({error.group(1)})"

    match = R_ROWS.search(output) or PY_ROWS.search(output)
    rows = int(match.group(1)) if match else 0

    print(f"  {name}: {status}, {seconds:.1f} s, "
          f"peak {peak / 1e9:.2f} GB memory, {peak_disk / 1e9:.2f} GB disk, "
          f"{rows:,} rows out")

    return {
        "config": name,
        "copies": copies,
        "rows_in": copies * ROWS_PER_COPY,
        "rows_out": rows,
        "wall_seconds": round(seconds, 1),
        "peak_rss_bytes": peak,
        "store_bytes_before": before,
        "store_bytes_after": store_bytes(store),
        "store_bytes_peak": peak_disk,
        "status": status,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Time the prep over synthetic deliveries of several sizes.")
    parser.add_argument("--copies", type=int, nargs="+", required=True,
                        help="the sizes to run, in copies of the test delivery")
    parser.add_argument("--configs", nargs="+", default=list(CONFIGURATIONS),
                        choices=list(CONFIGURATIONS),
                        help="which configurations to run")
    parser.add_argument("--root", type=Path,
                        default=Path(os.environ.get(
                            "SIAB_BENCH_ROOT",
                            Path.home() / "data" / "siab_bench")),
                        help="where the fixtures are written. Keep it outside "
                             "any folder that syncs.")
    parser.add_argument("--results", type=Path,
                        help="where the runs and their logs are recorded, "
                             "default <root>/results")
    parser.add_argument("--seed", type=int, default=20260918)
    parser.add_argument("--keep-fixtures", action="store_true",
                        help="keep each size's delivery and stores. Without "
                             "this they are deleted once the size is done, "
                             "which is what makes a sweep fit on one disk.")
    arguments = parser.parse_args()

    root = arguments.root.expanduser()
    results = (arguments.results or (root / "results")).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    results.mkdir(parents=True, exist_ok=True)

    runs_csv = results / "runs.csv"
    steps_csv = results / "steps.csv"
    stamp = datetime.now().isoformat(timespec="seconds")

    for copies in arguments.copies:
        rows = copies * ROWS_PER_COPY
        print(f"\n{copies} copies, {rows:,} rows "
              f"({rows / REAL_SIAB_ROWS:.2f} of a real SIAB 7523 v2)")
        paths = build_fixture(root, copies,
                              need_parquet="py-parquet" in arguments.configs,
                              need_stata="stata" in arguments.configs,
                              seed=arguments.seed)

        for name in arguments.configs:
            record = run_configuration(name, copies, paths, results,
                                       arguments.seed)
            record["run_at"] = stamp
            write_row(runs_csv, record)

            log_dir = results / f"{copies}x" / name / "log"
            for step, seconds, span in step_times(log_dir):
                write_row(steps_csv, {"run_at": stamp, "config": name,
                                      "copies": copies, "step": step,
                                      "seconds": seconds, "log_span": span})

            store = results / f"{copies}x" / name / (
                "store.duckdb" if name != "py-parquet" else "store")
            if not arguments.keep_fixtures and store.exists():
                shutil.rmtree(store) if store.is_dir() else store.unlink()

        if not arguments.keep_fixtures:
            for path in (paths["delivery"], paths["duckdb"], paths["parquet"]):
                if path.exists():
                    shutil.rmtree(path) if path.is_dir() else path.unlink()

    print(f"\nRuns in {runs_csv}, steps in {steps_csv}")


def write_row(path: Path, record: dict) -> None:
    """Append one row, writing the header the first time the file is touched.

    A file written by an older version of this script can carry a different
    header. Rather than write rows that silently do not line up with it, the
    new header is written again before the row.
    """
    header = ",".join(record)
    existing = (path.read_text().splitlines()[0]
                if path.exists() and path.stat().st_size else None)
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(record))
        if existing != header:
            writer.writeheader()
        writer.writerow(record)


if __name__ == "__main__":
    main()
