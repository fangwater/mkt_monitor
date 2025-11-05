#!/usr/bin/env bash

# 部署脚本（XDP 网卡监控）
# cfg 位于仓库根目录，文件名为 xdp_cfg.yaml

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SSH_TIMEOUT=10

# shellcheck disable=SC2034 # 作为示例保留 primary/secondary
SERVERS=(
    # "103.90.136.194:primary"
    # "103.90.136.195:secondary"
    # "103.90.136.196:primary"
    # "103.90.136.197:secondary"
    "178.173.241.34:primary"
    "178.173.241.35:secondary"
    # "178.173.241.36:primary"
    # "178.173.241.37:secondary"
)

user=el01
exec_dir=/home/"${user}"/xdp_monitor

PYTHON_SCRIPTS=(
    "src/xdp_bandwidth.py"
)

INSTALL_SCRIPT="scripts/install_deps.sh"
LOCAL_CONFIG="src/xdp_cfg.yaml"
REMOTE_CONFIG_NAME="xdp_cfg.yaml"

INSTALL_SCRIPT_PATH="${PROJECT_ROOT}/${INSTALL_SCRIPT}"
LOCAL_CONFIG_PATH="${PROJECT_ROOT}/${LOCAL_CONFIG}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

check_status() {
    if [[ $? -eq 0 ]]; then
        log "✅ $1 成功"
    else
        log "❌ $1 失败"
        exit 1
    fi
}

parse_server_config() {
    local config="$1"
    local ip="${config%:*}"
    local role="${config#*:}"
    echo "$ip $role"
}

ensure_local_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        log "❌ 本地文件不存在: $path"
        exit 1
    fi
}

log "检查本地待部署文件..."
for script in "${PYTHON_SCRIPTS[@]}"; do
    ensure_local_file "${PROJECT_ROOT}/${script}"
done
ensure_local_file "$INSTALL_SCRIPT_PATH"
ensure_local_file "$LOCAL_CONFIG_PATH"
log "✅ 本地文件检查完成"

log "检查所有服务器的SSH连接..."
for server_config in "${SERVERS[@]}"; do
    [[ "$server_config" =~ ^[[:space:]]*# ]] && continue

    read -r ip role <<<"$(parse_server_config "$server_config")"

    log "检查服务器 $ip ($role)..."
    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$user@$ip" "echo 'SSH连接成功'" >/dev/null 2>&1
    check_status "SSH连接到 $ip"

    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$user@$ip" "if [ ! -d '$exec_dir' ]; then mkdir -p '$exec_dir'; fi"
    check_status "检查目录在 $ip"
done

log "开始部署文件..."
for server_config in "${SERVERS[@]}"; do
    [[ "$server_config" =~ ^[[:space:]]*# ]] && continue

    read -r ip role <<<"$(parse_server_config "$server_config")"
    log "部署到服务器 $ip ($role)..."

    for script in "${PYTHON_SCRIPTS[@]}"; do
        local_script="${PROJECT_ROOT}/${script}"
        remote_name=$(basename "$script")
        scp -o ConnectTimeout="$SSH_TIMEOUT" "$local_script" "$user@$ip:$exec_dir/$remote_name"
        check_status "复制 Python 脚本 $remote_name 到 $ip"
    done

    scp -o ConnectTimeout="$SSH_TIMEOUT" "$INSTALL_SCRIPT_PATH" "$user@$ip:$exec_dir/$(basename "$INSTALL_SCRIPT")"
    check_status "复制依赖安装脚本到 $ip"

    ssh -o ConnectTimeout="$SSH_TIMEOUT" "$user@$ip" "chmod +x '$exec_dir/$(basename "$INSTALL_SCRIPT")'"
    check_status "设置 $ip 上的依赖脚本权限"

    scp -o ConnectTimeout="$SSH_TIMEOUT" "$LOCAL_CONFIG_PATH" "$user@$ip:$exec_dir/$REMOTE_CONFIG_NAME"
    check_status "复制配置文件到 $ip"

    log "服务器 $ip ($role) 部署完成！"
done

log "所有服务器部署完成！"
