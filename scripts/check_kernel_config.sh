#!/usr/bin/env bash
set -euo pipefail

# 检测是否支持 eBPF 所需的关键内核配置

readonly REQUIRED_FLAGS_Y=(
  CONFIG_BPF
  CONFIG_BPF_SYSCALL
  CONFIG_BPF_JIT
  CONFIG_HAVE_EBPF_JIT
  CONFIG_BPF_EVENTS
  CONFIG_TRACEPOINTS
)

readonly OPTIONAL_FLAGS_Y=(
  CONFIG_KPROBE_EVENTS
  CONFIG_FUNCTION_TRACER
  CONFIG_NET_SCH_SFQ
)

readonly OPTIONAL_FLAGS_M=(
  CONFIG_BPF_STREAM_PARSER
)

detect_config_file() {
  if [[ -r /proc/config.gz ]]; then
    echo "zcat:/proc/config.gz"
    return 0
  fi

  local boot_cfg="/boot/config-$(uname -r)"
  if [[ -r "${boot_cfg}" ]]; then
    echo "cat:${boot_cfg}"
    return 0
  fi

  echo "未找到内核配置文件，可在内核源码目录下执行：zcat /proc/config.gz > .config" >&2
  exit 2
}

read_config_value() {
  local reader="$1"
  local flag="$2"

  if [[ "${reader}" == zcat:* ]]; then
    "${reader%%:*}" "${reader#*:}" | grep -E "^${flag}="
  else
    "${reader%%:*}" "${reader#*:}" | grep -E "^${flag}="
  fi
}

check_flags() {
  local reader="$1"
  local status=0

  echo "== 必需选项 (应为 =y) =="
  for flag in "${REQUIRED_FLAGS_Y[@]}"; do
    if read_config_value "${reader}" "${flag}" | grep -q "=y$"; then
      printf "  [OK]  %s\n" "${flag}"
    else
      printf "  [FAIL]%s (需要 =y)\n" "${flag}"
      status=1
    fi
  done

  echo -e "\n== 建议选项 (建议为 =y) =="
  for flag in "${OPTIONAL_FLAGS_Y[@]}"; do
    if read_config_value "${reader}" "${flag}" | grep -q "=y$"; then
      printf "  [OK]  %s\n" "${flag}"
    else
      printf "  [WARN]%s (建议 =y)\n" "${flag}"
    fi
  done

  echo -e "\n== 可选模块 (建议 =m 或 =y) =="
  for flag in "${OPTIONAL_FLAGS_M[@]}"; do
    if read_config_value "${reader}" "${flag}" | grep -Eq "=(y|m)$"; then
      printf "  [OK]  %s\n" "${flag}"
    else
      printf "  [INFO]%s (如需相关功能可打开)\n" "${flag}"
    fi
  done

  return "${status}"
}

main() {
  local reader
  reader="$(detect_config_file)"

  check_flags "${reader}"
}

main "$@"
