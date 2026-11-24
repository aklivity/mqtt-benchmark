#!/bin/bash
S=$(cd "$(dirname "$0")" && pwd)
for cfg in "before 0" "after 0" "before 1000000" "after 1000000"; do
  set -- $cfg
  $S/prep2.sh $2 >/dev/null 2>&1
  echo "=== $1 prefill=$2"
  python3 $S/load.py $S/dist/jars$( [ $1 = before ] && echo -before ) $1-$2 8000 60 2>&1 | grep -E "replay|RESULT|port"
done
echo MATRIX_DONE
