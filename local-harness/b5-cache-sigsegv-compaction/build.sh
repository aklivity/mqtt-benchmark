cd /home/user/zilla
bash .github/scripts/build-cache.sh install
which zstd || apt-get install -y zstd
bash .github/scripts/build-cache.sh restore "$HOME/.m2/build-cache" ghcr.io/aklivity/zilla/build-cache develop
./mvnw clean install -Dskip.checks=true -DskipTests -DskipITs
