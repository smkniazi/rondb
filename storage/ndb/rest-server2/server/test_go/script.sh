#!/usr/bin/env bash
set -euo pipefail

if tput bold &> /dev/null; then
    BOLD=$(tput bold)
    RESET=$(tput sgr0)
else
    BOLD=""
    RESET=""
fi

# Show help by `./script.sh help` or just `./script.sh`

cmd-help() {
  [ $# == 0 ] || die "Command $cmd does not take arguments."
  cat <<EOF

${BOLD}NAME${RESET}
    script.sh -- runs tests and benchmarks. Also restart RDRS server

${BOLD}DESCRIPTION${RESET}

    Before using this script, start a cluster and export a variable pointing to
    the rdrs2 config location:

        cd .../rondb/build 
        ./mysql-test/mtr --suite rdrs2-golang --start-and-exit
        export RDRS_CONFIG_FILE=\`realpath mysql-test/var/rdrs.1.1_config.json\`

${BOLD}Available commands${RESET}

${BOLD}HELP${RESET}
    Show this help

        ./script.sh help

${BOLD}RUN TESTS${RESET}
    Run test cases

        ./script.sh test [PACKAGE] [TEST_NAME]

    ${BOLD}PACKAGE${RESET} default: all packages
        To see available packages, run ${BOLD}./script.sh list${RESET}
    ${BOLD}TEST_NAME${RESET} Name of the specific test to run 
        default: run all tests in the go package

${BOLD}RUN BENCHMARKS${RESET}
    Run a benchmarks

        ./script.sh bench [PACKAGE] [BENCHMARK_NAME] [DURATION]

    ${BOLD}PACKAGE${RESET} default: all packages 
        To see available packages, run "./script.sh list"
    ${BOLD}BENCHMARK_NAME${RESET} Name of the specific test to run 
        default: run all benchmark tests in the go package
    ${BOLD}DURATION${RESET} default: 30

${BOLD}LIST GO PACKAGES${RESET}
    Show packages that can be used with ${BOLD}./script test${RESET} and ${BOLD}./script bench${RESET}

        ./script.sh list

${BOLD}RESTART RDRS${RESET}
    Restart rdrs2 without restarting the entire cluster. This is useful for
    quick iteration. Note that stdout/stderr will be discarded

        ./script.sh restart

EOF
}

cmd-test() {
  [ -f "${RDRS_CONFIG_FILE}" ] || die 'RDRS_CONFIG_FILE must be set and point to an existing file.'
  TEST_PACKAGE="./..."
  TEST_CASE=
  case $# in
    0) ;;
    1) TEST_PACKAGE="$1" ;;
    2) TEST_PACKAGE="$1" && TEST_CASE="-run $2" ;;
    *) die "Too many arguments" ;;
  esac

  # Run Golang test, capture exit code and stdout.
  # * `-p 1` ensures that only one test is run in parallel by the `go test`
  #   process. This is necessary since some of these tests change the database
  #   state.
  # * `-count 1` is the idiomatic way to prevent `go test` from using cached
  #   test runs. This is necessary since the `go test` logic doesn't look into
  #   (all) C++ sources to detect changes, meaning cache invalidation of test
  #   runs wouldn't work correctly.
  go test -v \
     -p 1 \
     -count 1 \
     $TEST_CASE \
     $TEST_PACKAGE
}

cmd-bench() {
  [ -f "${RDRS_CONFIG_FILE}" ] || die 'RDRS_CONFIG_FILE must be set and point to an existing file.'
  BENCHMARK_PACKAGE="./..."
  BENCHMARK_NAME=Benchmark
  BENCHMARK_DURATION=30
  PROFILE=""

  case $# in
    0) ;;
    1) if [[ "$1" =~ ^[0-9]+$ ]]; then
         BENCHMARK_DURATION="$1"
       else
         BENCHMARK_PACKAGE="$1"
       fi ;;
    2) BENCHMARK_PACKAGE="$1"
       BENCHMARK_NAME="$2" ;;
    3) BENCHMARK_PACKAGE="$1"
       BENCHMARK_NAME="$2" 
       BENCHMARK_DURATION="$3"
       PROFILE="-cpuprofile cpu.out  -memprofile mem.out" ;;
    *) die "Too many arguments" ;;
  esac
  go test -v \
     -test.bench Benchmark \
     -test.run=thisexpressionwontmatchanytest \
     -benchmem \
     -benchtime=${BENCHMARK_DURATION}s \
     $PROFILE \
     $BENCHMARK_PACKAGE
}

cmd-list() {
  [ $# == 0 ] || die "Command $cmd does not take arguments."
  go list ./...
}

cmd-restart() {
  [ $# == 0 ] || die "Command $cmd does not take arguments."
  [ -f "${RDRS_CONFIG_FILE}" ] || die 'RDRS_CONFIG_FILE must be set and point to an existing file.'
  pidfile="$(< ${RDRS_CONFIG_FILE} grep -E '^ *"PIDFile"' |
             sed -r 's@^ *"PIDFile": *"(/[^"]+)",$@\1@;')"
  [ -f "$pidfile" ] || die "PID file not found: $pidfile"
  pid="$(< $pidfile)"
  rdrs2confdir="$(dirname ${RDRS_CONFIG_FILE})"
  rdrs2bin="$(realpath ${rdrs2confdir}/../../bin/rdrs2)"
  [ -x "$rdrs2bin" ] || die "rdrs2 binary not found: $rdrs2bin"
  echo "Killing rdrs2 server PID $pid"
  kill $pid
  echo -n 'Waiting for PID $pid to exit...'
  while kill -0 $pid 2> /dev/null; do sleep 1; echo -n .; done
  echo done
  echo "Restarting rdrs2 server"
  $rdrs2bin >/dev/null 2>/dev/null &
}

die() {
  echo "$*"
  exit 1
}

cmd=help
if [ $# -gt 0 ]; then
  cmd="$1"
  shift
fi
case "$cmd" in
  help|test|bench|list|restart)
    cmd-$cmd "$@" ;;
  *)
    die "Unknown sub-command: $cmd" ;;
esac
