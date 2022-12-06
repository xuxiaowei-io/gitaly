#!/bin/sh
#
# profile-gitaly: Profile host with perf and libbpt-tools.
# Must be run as root.
#
# Mandatory options:
# 	-d <DURATION_SECS> : Number of seconds to profile for
# 	-g <GIT_REPO>      : Name of Git repository being used
# 	-o <OUTPUT_DIR>    : Directory to write output to
# 	-r <RPC>           : Name of RPC being executed

set -e

usage() {
	echo "Usage: $0 -d <DURATION_SECS> -o <OUTPUT_DIR> -r <RPC> -g <GIT_REPOSITORY>"
	exit
}

main() {
	if [ "$(id -u)" -ne 0 ]; then
		echo "$0 must be run as root"
		exit 1
	fi

	while getopts "hd:g:o:r:" arg; do
		case "$arg" in
			d) seconds=${OPTARG}
				;;
			g) repo=${OPTARG}
				;;
			o) out_dir=${OPTARG}
				;;
			r) rpc=${OPTARG}
				;;
			h|*) usage
				;;
		esac
	done
	shift $((OPTIND-1))

	if [ "$seconds" -le 0 ] || [ -z "$out_dir" ] || [ -z "$rpc" ] || [ -z "$repo" ]; then
		usage
		exit 1
	fi

	if ! pidof gitaly > /dev/null; then
		echo "Gitaly is not running, aborting"
		exit 1
	fi

	# Add utilities to $PATH
	export PATH="$PATH:/usr/local/bin"

	perf_tmp_dir=$(mktemp -d /tmp/gitaly-perf.XXXXXX)

	gitaly_perf_out=$(mktemp "${perf_tmp_dir}/gitaly-perf-${rpc}-${repo}.XXXXXX")
	perf record --freq=99 --call-graph=fp --pid="$(pidof -s gitaly)" --no-inherit --output="$gitaly_perf_out" -- sleep "$seconds" &

	all_perf_out=$(mktemp "${perf_tmp_dir}/all-perf-${rpc}-${repo}.XXXXXX")
	perf record --freq=99 --call-graph=dwarf --all-cpus --output="$all_perf_out" -- sleep "$seconds" &

	timeout "$seconds" execsnoop --uid=1999 --quote > "${out_dir}/gitaly-execs.txt" &

	cpudist "$seconds" 1 > "${out_dir}/cpu-dist-on.txt" &

	cpudist --offcpu "$seconds" 1 > "${out_dir}/cpu-dist-off.txt" &

	biolatency --disk "$seconds" 1 > "${out_dir}/biolatency.txt" &

	biotop --noclear --rows 100 "$seconds" 1 > "${out_dir}/biotop.txt" &

	cachestat "$seconds" 1 > "${out_dir}/page-cachestat.txt" &

	wait

	journalctl --output=cat _PID="$(pidof -s gitaly)" > "${out_dir}/gitaly.log"

	gitaly_perf_svg="${out_dir}/gitaly-perf.svg"
	perf script --header --input="$gitaly_perf_out" | stackcollapse | flamegraph > "$gitaly_perf_svg" &

	all_perf_svg="${out_dir}/all-perf.svg"
	perf script --header --input="$all_perf_out" | stackcollapse | flamegraph > "$all_perf_svg" &

	wait

	chown -R git:git "$out_dir"
	rm -rf "$perf_tmp_dir"
}

main "$@"
