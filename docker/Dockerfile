FROM ubuntu:24.04 as build-base

ARG BUILDKIT_SBOM_SCAN_STAGE=true

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		build-essential \
		ca-certificates \
		gnupg \
		libncurses5-dev \
		wget

FROM build-base as openssl-builder

ARG BUILDKIT_SBOM_SCAN_STAGE=true

# Default to a PGP keyserver that pgp-happy-eyeballs recognizes, but allow for substitutions locally
ARG PGP_KEYSERVER=keyserver.ubuntu.com
# If you are building this image locally and are getting `gpg: keyserver receive failed: No data` errors,
# run the build with a different PGP_KEYSERVER, e.g. docker build --tag rabbitmq:4.1 --build-arg PGP_KEYSERVER=pgpkeys.eu 4.1/ubuntu
# For context, see https://github.com/docker-library/official-images/issues/4252

ENV OPENSSL_VERSION 3.3.4
ENV OPENSSL_SOURCE_SHA256="8d1a5fc323d3fd351dc05458457fd48f78652d2a498e1d70ffea07b4d0eb3fa8"
# https://www.openssl.org/source/
ENV OPENSSL_PGP_KEY_IDS="0xBA5473A2B0587B07FB27CF2D216094DFD0CB81EF"

ENV OTP_VERSION 27.3.4.3
# TODO add PGP checking when the feature will be added to Erlang/OTP's build system
# https://erlang.org/pipermail/erlang-questions/2019-January/097067.html
ENV OTP_SOURCE_SHA256="597618c75890e8e606033bc8dbf6ec87fdf4e892c10b4912ae3c9a4f01384579"

# install openssl & erlang to a path that isn't auto-checked for libs to prevent accidental use by system packages
ENV ERLANG_INSTALL_PATH_PREFIX /opt/erlang
ENV OPENSSL_INSTALL_PATH_PREFIX /opt/openssl

# Install dependencies required to build Erlang/OTP from source
# https://erlang.org/doc/installation_guide/INSTALL.html
# dpkg-dev: Required to set up host & build type when compiling Erlang/OTP
# gnupg: Required to verify OpenSSL artefacts
# libncurses5-dev: Required for Erlang/OTP new shell & observer_cli - https://github.com/zhongwencool/observer_cli
RUN set -eux; \
	OPENSSL_SOURCE_URL="https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz"; \
	OPENSSL_PATH="/usr/local/src/openssl-$OPENSSL_VERSION"; \
	OPENSSL_CONFIG_DIR="$OPENSSL_INSTALL_PATH_PREFIX/etc/ssl"; \
	\
# Required by the crypto & ssl Erlang/OTP applications
	wget --progress dot:giga --output-document "$OPENSSL_PATH.tar.gz.asc" "$OPENSSL_SOURCE_URL.asc"; \
	wget --progress dot:giga --output-document "$OPENSSL_PATH.tar.gz" "$OPENSSL_SOURCE_URL"; \
	export GNUPGHOME="$(mktemp -d)"; \
	for key in $OPENSSL_PGP_KEY_IDS; do \
		gpg --batch --keyserver "$PGP_KEYSERVER" --recv-keys "$key"; \
	done; \
	gpg --batch --verify "$OPENSSL_PATH.tar.gz.asc" "$OPENSSL_PATH.tar.gz"; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME"; \
	echo "$OPENSSL_SOURCE_SHA256 *$OPENSSL_PATH.tar.gz" | sha256sum --check --strict -; \
	mkdir -p "$OPENSSL_PATH"; \
	tar --extract --file "$OPENSSL_PATH.tar.gz" --directory "$OPENSSL_PATH" --strip-components 1; \
	\
# Configure OpenSSL for compilation
	cd "$OPENSSL_PATH"; \
# without specifying "--libdir", Erlang will fail during "crypto:supports()" looking for a "pthread_atfork" function that doesn't exist (but only on arm32v7/armhf??)
# OpenSSL's "config" script uses a lot of "uname"-based target detection...
	dpkgArch="$(dpkg --print-architecture)"; dpkgArch="${dpkgArch##*-}"; \
# https://deb.debian.org/debian/dists/unstable/main/
	case "$dpkgArch" in \
# https://github.com/openssl/openssl/blob/openssl-3.1.1/Configurations/10-main.conf#L860 (look for "linux-" and "linux64-" keys)
		amd64) opensslMachine='linux-x86_64' ;; \
		arm64) opensslMachine='linux-aarch64' ;; \
# https://github.com/openssl/openssl/blob/openssl-3.1.1/Configurations/10-main.conf#L736-L766
# https://wiki.debian.org/ArchitectureSpecificsMemo#Architecture_baselines
# https://gcc.gnu.org/onlinedocs/gcc/ARM-Options.html
		armhf) opensslMachine='linux-armv4'; opensslExtraConfig='-march=armv7-a+fp' ;; \
		i386) opensslMachine='linux-x86' ;; \
		ppc64el) opensslMachine='linux-ppc64le' ;; \
		riscv64) opensslMachine='linux64-riscv64' ;; \
		s390x) opensslMachine='linux64-s390x' ;; \
		*) echo >&2 "error: unsupported arch: '$apkArch'"; exit 1 ;; \
	esac; \
	MACHINE="$opensslMachine" \
	RELEASE="4.x.y-z" \
	SYSTEM='Linux' \
	BUILD='???' \
	./Configure \
		"$opensslMachine" \
		enable-fips \
		--prefix="$OPENSSL_INSTALL_PATH_PREFIX" \
		--openssldir="$OPENSSL_CONFIG_DIR" \
		--libdir="$OPENSSL_INSTALL_PATH_PREFIX/lib" \
# add -rpath to avoid conflicts between our OpenSSL's "libssl.so" and the libssl package by making sure "$INSTALL_PATH_PREFIX/lib" is searched first (but only for Erlang/OpenSSL to avoid issues with other tools using libssl; https://github.com/docker-library/rabbitmq/issues/364)
		-Wl,-rpath="$OPENSSL_INSTALL_PATH_PREFIX/lib" \
		${opensslExtraConfig:-} \
	; \
# Compile, install OpenSSL, verify that the command-line works & development headers are present
	make -j "$(getconf _NPROCESSORS_ONLN)"; \
	make install_sw install_ssldirs install_fips; \
	ldconfig; \
# use Debian's CA certificates
	rmdir "$OPENSSL_CONFIG_DIR/certs" "$OPENSSL_CONFIG_DIR/private"; \
	ln -sf /etc/ssl/certs /etc/ssl/private "$OPENSSL_CONFIG_DIR"

# smoke test
RUN $OPENSSL_INSTALL_PATH_PREFIX/bin/openssl version

FROM openssl-builder

ARG BUILDKIT_SBOM_SCAN_STAGE=true
ARG DEBIAN_FRONTEND=noninteractive

RUN set -eux;	\
	apt-get install -y --no-install-recommends git cmake zip curl p7zip-full p7zip-rar python3; \
	cd /usr/local/src; \
	curl -fsSO https://elixir-lang.org/install.sh; \
	sh install.sh elixir@1.18.4 otp@$OTP_VERSION; \
	installs_dir=$HOME/.elixir-install/installs; \
	export PATH=$installs_dir/otp/$OTP_VERSION/bin:$PATH; \
	export PATH=$installs_dir/elixir/1.18.4-otp-27/bin:$PATH; \
	git clone -b v4.1.x https://github.com/rabbitmq/rabbitmq-server.git; \
	cd rabbitmq-server/deps; \
	git clone https://github.com/kraanerk/rabbitmq-message-deduplication.git; \
	cd rabbitmq-message-deduplication; \
	make dist
