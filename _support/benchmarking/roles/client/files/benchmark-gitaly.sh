#!/bin/sh

# benchmark-gitaly: Run ghz against a remote Gitaly instance.
#
# Mandatory options:
#   -a <GITALY_ADDR>   : Gitaly address, without port
#   -d <DURATION_SECS> : Duration to run in seconds
#   -o <OUTPUT_DIR>    : Directory to write results to
#   -p <PROTO_FILE>    : Protobuf file containing definition of RPC to test
#   -r <RPC>           : RPC name, e.g. GetBlobs
#   -s <SERVICE>       : RPC service name, e.g. gitaly.BlobService

set -e

usage() {
	echo "Usage: $0 -a <GITALY_ADDR> -d <DURATION_SECS> -g <GIT_REPO> \
-o <OUTPUT_DIR> -p <PROTO_FILE> -r <RPC> -s <SERVICE>"
	exit 1
}

main() {
	while getopts "ha:d:g:o:p:q:r:s:" arg; do
		case "${arg}" in
			a) gitaly_addr="${OPTARG}" ;;
			d) seconds="${OPTARG}" ;;
			g) repo="${OPTARG}" ;;
			o) out_dir="${OPTARG}" ;;
			p) proto="${OPTARG}" ;;
			r) rpc="${OPTARG}" ;;
			s) service="${OPTARG}" ;;
			h|*) usage ;;
		esac
	done

	if [ "${seconds}" -le 0 ] \
		|| [ -z "${gitaly_addr}" ] \
		|| [ -z "${out_dir}" ] \
		|| [ -z "${repo}" ] \
		|| [ -z "${proto}" ] \
		|| [ -z "${rpc}" ] \
		|| [ -z "${service}" ]; then
		usage
	fi

	query_file="/opt/ghz/queries/${rpc}/${repo}.json"

	ghz --insecure --format=json --output="${out_dir}/ghz.json" \
		--proto="/src/gitaly/proto/${proto}" --call="${service}/${rpc}" \
		--concurrency=10 --duration="${seconds}s" --rps=100 \
		--data-file="${query_file}" "${gitaly_addr}:8075"
}

main "$@"
