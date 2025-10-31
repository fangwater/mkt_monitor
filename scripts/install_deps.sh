#!/usr/bin/env bash
set -euo pipefail

# 安装运行 eBPF/BCC 所需的依赖

ensure_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "需要 root 权限，尝试使用 sudo..."
    exec sudo --preserve-env=PATH "$0" "$@"
  fi
}

install_debian() {
  apt-get update
  apt-get install -y \
    bpfcc-tools \
    python3-bpfcc \
    python3-zmq \
    linux-headers-"$(uname -r)" \
    clang \
    llvm \
    make \
    libbpf-dev \
    pkg-config \
    gettext \
    autoconf \
    automake \
    libtool \
    flex \
    bison \
    libelf-dev \
    zlib1g-dev
}

install_fedora() {
  dnf install -y \
    bcc \
    python3-bcc \
    python3-zmq \
    kernel-devel-"$(uname -r)" \
    clang \
    llvm \
    make \
    libbpf \
    pkg-config \
    gettext \
    autoconf \
    automake \
    libtool \
    flex \
    bison \
    elfutils-libelf-devel \
    zlib-devel
}

main() {
  ensure_root "$@"

  if [[ -r /etc/os-release ]]; then
    # shellcheck source=/dev/null
    source /etc/os-release
  else
    echo "无法检测当前发行版，请手动安装 BCC/libbpf 相关依赖。" >&2
    exit 1
  fi

  case "${ID}-${VERSION_ID}" in
    ubuntu-*|debian-*)
      install_debian
      ;;
    fedora-*|centos-8*|rhel-8*|rocky-8*|almalinux-8*)
      install_fedora
      ;;
    *)
      echo "暂未支持的发行版 (${ID}-${VERSION_ID})，请参考官方文档手动安装依赖。" >&2
      exit 2
      ;;
  esac

  echo "依赖安装完成。"
}

main "$@"
