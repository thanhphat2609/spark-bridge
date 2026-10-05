#!/bin/bash
# =============================================================================
# fetch.sh <apache-relative-path> <sha512> <output-file>
#
# Downloads an Apache release from the first mirror that works, then verifies
# it against the pinned official SHA-512 (so third-party mirrors are safe).
#
#   - resumes broken downloads (-C -) and retries transient errors
#   - gives up on a mirror that stays below ~200 KB/s for 30 s and tries the next
#   - two passes over the mirror list before failing the build
#
# Mirrors come from $APACHE_MIRRORS (space separated), e.g.
#   "https://archive.apache.org/dist https://repo.huaweicloud.com/apache"
# =============================================================================
set -uo pipefail

REL_PATH="$1"
SHA512="$2"
OUT="$3"
MIRRORS="${APACHE_MIRRORS:-https://archive.apache.org/dist}"

for pass in 1 2; do
  for mirror in ${MIRRORS}; do
    url="${mirror%/}/${REL_PATH}"
    echo "[fetch] pass ${pass}: ${url}"
    if curl -fL --connect-timeout 20 \
         --retry 5 --retry-all-errors --retry-delay 3 \
         --speed-limit 204800 --speed-time 30 \
         -C - -o "${OUT}" "${url}"; then
      if echo "${SHA512}  ${OUT}" | sha512sum -c --status; then
        echo "[fetch] OK (sha512 verified): ${REL_PATH}"
        exit 0
      fi
      echo "[fetch] checksum mismatch from ${mirror}, discarding"
      rm -f "${OUT}"
    else
      # Keep the partial file: the next mirror resumes it (checksum verifies the result)
      echo "[fetch] download failed or too slow from ${mirror}, trying next mirror"
    fi
  done
done

echo "[fetch] ERROR: could not download ${REL_PATH} from any mirror" >&2
exit 1
