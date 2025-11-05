#!/usr/bin/env bash

# Common helper for building a user-space Nginx with SSL support.

set -euo pipefail

log_info() {
    echo "[lib_nginx_build] $*" >&2
}

download_or_die() {
    local url="$1"
    local output="$2"
    log_info "Downloading ${url}"
    if ! curl -fSL --retry 3 --retry-delay 2 -o "${output}" "${url}"; then
        log_info "Download failed for ${url}"
        exit 1
    fi
}

ensure_nginx() {
    local prefix="$1"
    local version="${2:-1.26.1}"
    local openssl_version="${OPENSSL_VERSION:-3.2.1}"
    local pcre_version="${PCRE_VERSION:-8.45}"
    local zlib_version="${ZLIB_VERSION:-1.3.1}"
    local build_root="${BUILD_ROOT:-$HOME/.cache/nginx-build}"

    if [ -z "${prefix}" ]; then
        log_info "Prefix must not be empty"
        return 1
    fi

    if [ -x "${prefix}/sbin/nginx" ]; then
        log_info "Reusing existing Nginx at ${prefix}"
        return 0
    fi

    for bin in curl tar make gcc; do
        if ! command -v "${bin}" >/dev/null 2>&1; then
            log_info "Missing required tool: ${bin}"
            return 1
        fi
    done

    mkdir -p "${build_root}"
    mkdir -p "${prefix}"

    local src_dir="${build_root}/src"
    rm -rf "${src_dir}"
    mkdir -p "${src_dir}"

    local nginx_tar="nginx-${version}.tar.gz"
    local openssl_tar="openssl-${openssl_version}.tar.gz"
    local pcre_tar="pcre-${pcre_version}.tar.gz"
    local zlib_tar="zlib-${zlib_version}.tar.gz"

    (
        cd "${src_dir}"

        if [ ! -f "${nginx_tar}" ]; then
            download_or_die "https://nginx.org/download/${nginx_tar}" "${nginx_tar}"
        else
            log_info "Using cached ${nginx_tar}"
        fi
        if [ ! -f "${openssl_tar}" ]; then
            download_or_die "https://www.openssl.org/source/${openssl_tar}" "${openssl_tar}"
        else
            log_info "Using cached ${openssl_tar}"
        fi
        if [ ! -f "${pcre_tar}" ]; then
            download_or_die "https://downloads.sourceforge.net/project/pcre/pcre/${pcre_version}/${pcre_tar}" "${pcre_tar}"
        else
            log_info "Using cached ${pcre_tar}"
        fi
        if [ ! -f "${zlib_tar}" ]; then
            download_or_die "https://zlib.net/${zlib_tar}" "${zlib_tar}"
        else
            log_info "Using cached ${zlib_tar}"
        fi

        tar xf "${nginx_tar}"
        tar xf "${openssl_tar}"
        tar xf "${pcre_tar}"
        tar xf "${zlib_tar}"

        cd "nginx-${version}"
        ./configure \
            --prefix="${prefix}" \
            --with-http_ssl_module \
            --with-http_v2_module \
            --with-http_stub_status_module \
            --with-http_gzip_static_module \
            --with-stream \
            --with-openssl="${src_dir}/openssl-${openssl_version}" \
            --with-pcre="${src_dir}/pcre-${pcre_version}" \
            --with-zlib="${src_dir}/zlib-${zlib_version}" \
            --with-cc-opt='-O2 -fPIC'

        make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)"
        make install
    )

    log_info "Installed Nginx ${version} to ${prefix}"
}
