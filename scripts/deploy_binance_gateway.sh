#!/usr/bin/env bash

# Build and configure a user-space Nginx gateway that load-balances Binance proxy nodes.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/lib_nginx_build.sh"

INSTALL_PREFIX="${INSTALL_PREFIX:-$HOME/nginx/binance-gateway}"
SPOT_PORT="${SPOT_PORT:-19080}"
FUTURES_PORT="${FUTURES_PORT:-19081}"
SPOT_NODE_PORT="${SPOT_NODE_PORT:-9080}"
FUTURES_NODE_PORT="${FUTURES_NODE_PORT:-9081}"
DEFAULT_RESOLVERS="1.1.1.1 8.8.8.8"
RESOLVERS="${RESOLVERS:-$DEFAULT_RESOLVERS}"
NGINX_VERSION="${NGINX_VERSION:-1.26.1}"
ENABLE_LOCAL_NODE="${ENABLE_LOCAL_NODE:-false}"

# Adjust the backend list to match the deployed proxy nodes.
BACKENDS_DEFAULT=(
    "192.168.1.194:9080"
    "192.168.1.195:9080"
    "192.168.1.196:9080"
    "192.168.1.197:9080"
    "192.168.1.198:9080"
)

declare -a RAW_SPOT_BACKENDS=()
declare -a BACKEND_SPOT_SERVERS=()
declare -a BACKEND_FUTURES_SERVERS=()
declare -a BACKEND_HOSTS=()

if [ -n "${BACKENDS_SPOT:-}" ]; then
    IFS=',' read -r -a RAW_SPOT_BACKENDS <<< "${BACKENDS_SPOT}"
elif [ -n "${BACKENDS:-}" ]; then
    IFS=',' read -r -a RAW_SPOT_BACKENDS <<< "${BACKENDS}"
else
    RAW_SPOT_BACKENDS=("${BACKENDS_DEFAULT[@]}")
fi

for backend in "${RAW_SPOT_BACKENDS[@]}"; do
    backend="${backend//[[:space:]]/}"
    [ -z "${backend}" ] && continue
    host="${backend}"
    spot_port="${SPOT_NODE_PORT}"
    if [[ "${backend}" == *:* ]]; then
        host="${backend%%:*}"
        provided_port="${backend##*:}"
        if [ -n "${provided_port}" ]; then
            spot_port="${provided_port}"
        fi
    fi
    BACKEND_HOSTS+=("${host}")
    BACKEND_SPOT_SERVERS+=("${host}:${spot_port}")
done

if [ -n "${BACKENDS_FUTURES:-}" ]; then
    IFS=',' read -r -a RAW_FUTURES_BACKENDS <<< "${BACKENDS_FUTURES}"
    for backend in "${RAW_FUTURES_BACKENDS[@]}"; do
        backend="${backend//[[:space:]]/}"
        [ -z "${backend}" ] && continue
        host="${backend}"
        futures_port="${FUTURES_NODE_PORT}"
        if [[ "${backend}" == *:* ]]; then
            host="${backend%%:*}"
            provided_port="${backend##*:}"
            if [ -n "${provided_port}" ]; then
                futures_port="${provided_port}"
            fi
        fi
        BACKEND_FUTURES_SERVERS+=("${host}:${futures_port}")
    done
else
    for host in "${BACKEND_HOSTS[@]}"; do
        BACKEND_FUTURES_SERVERS+=("${host}:${FUTURES_NODE_PORT}")
    done
fi

if [[ "${ENABLE_LOCAL_NODE}" == "true" || "${ENABLE_LOCAL_NODE}" == "True" ]]; then
    LOCAL_NODE_PREFIX="${INSTALL_PREFIX}/local-node"
    ensure_nginx "${LOCAL_NODE_PREFIX}" "${NGINX_VERSION}"

    mkdir -p "${LOCAL_NODE_PREFIX}/conf"
    mkdir -p "${LOCAL_NODE_PREFIX}/logs"
    mkdir -p "${LOCAL_NODE_PREFIX}/temp/client" "${LOCAL_NODE_PREFIX}/temp/proxy"

    cat > "${LOCAL_NODE_PREFIX}/conf/nginx.conf" <<EOF
worker_processes auto;
error_log  logs/error.log info;
pid        logs/nginx.pid;

events {
    worker_connections 1024;
}

http {
    include       mime.types;
    default_type  application/octet-stream;

    sendfile        on;
    tcp_nopush      on;
    tcp_nodelay     on;
    keepalive_timeout  65;
    types_hash_max_size 2048;

    resolver ${RESOLVERS} valid=300s ipv6=off;
    resolver_timeout 5s;

    log_format main '
        \$remote_addr - \$remote_user [\$time_local] "\$request" '
        '\$status \$body_bytes_sent "\$http_referer" '
        '"\$http_user_agent" "\$http_x_forwarded_for"';
    access_log  logs/access.log  main;

    client_body_temp_path temp/client;
    proxy_temp_path       temp/proxy;

    server {
        listen       ${SPOT_NODE_PORT};
        listen       [::]:${SPOT_NODE_PORT};
        server_name  _;

        keepalive_timeout 60s;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header X-Request-ID \$request_id;

        location / {
            proxy_pass https://data-api.binance.vision\$request_uri;
            proxy_set_header Host data-api.binance.vision;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Host \$host;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_ssl_server_name on;
            proxy_ssl_name data-api.binance.vision;
            proxy_ssl_protocols TLSv1.2 TLSv1.3;
            proxy_read_timeout 30s;
            proxy_connect_timeout 5s;
        }

        location = /healthz {
            return 204;
        }
    }

    server {
        listen       ${FUTURES_NODE_PORT};
        listen       [::]:${FUTURES_NODE_PORT};
        server_name  _;

        keepalive_timeout 60s;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header X-Request-ID \$request_id;

        location / {
            proxy_pass https://fapi.binance.com\$request_uri;
            proxy_set_header Host fapi.binance.com;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Host \$host;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_ssl_server_name on;
            proxy_ssl_name fapi.binance.com;
            proxy_ssl_protocols TLSv1.2 TLSv1.3;
            proxy_read_timeout 30s;
            proxy_connect_timeout 5s;
        }

        location = /healthz {
            return 204;
        }
    }
}
EOF

    BACKEND_SPOT_SERVERS+=("127.0.0.1:${SPOT_NODE_PORT}")
    BACKEND_FUTURES_SERVERS+=("127.0.0.1:${FUTURES_NODE_PORT}")
fi

if [ "${#BACKEND_SPOT_SERVERS[@]}" -eq 0 ]; then
    echo "[deploy_binance_gateway] No spot backend servers defined" >&2
    exit 1
fi

if [ "${#BACKEND_FUTURES_SERVERS[@]}" -eq 0 ]; then
    echo "[deploy_binance_gateway] No futures backend servers defined" >&2
    exit 1
fi

echo "[deploy_binance_gateway] Target prefix: ${INSTALL_PREFIX}"
ensure_nginx "${INSTALL_PREFIX}" "${NGINX_VERSION}"

mkdir -p "${INSTALL_PREFIX}/conf"
mkdir -p "${INSTALL_PREFIX}/logs"
mkdir -p "${INSTALL_PREFIX}/temp/client" "${INSTALL_PREFIX}/temp/proxy"

CONFIG_FILE="${INSTALL_PREFIX}/conf/nginx.conf"

{
    cat <<EOF
worker_processes auto;
error_log  logs/error.log info;
pid        logs/nginx.pid;

events {
    worker_connections 1024;
}

http {
    include       mime.types;
    default_type  application/octet-stream;

    sendfile        on;
    tcp_nopush      on;
    tcp_nodelay     on;
    keepalive_timeout  65;
    types_hash_max_size 2048;

    log_format main '
        \$remote_addr - \$remote_user [\$time_local] "\$request" '
        '\$status \$body_bytes_sent "\$http_referer" '
        '"\$http_user_agent" "\$http_x_forwarded_for"';
    access_log  logs/access.log  main;

    upstream binance_spot_nodes {
        zone binance_spot_nodes 128k;
EOF

    for backend in "${BACKEND_SPOT_SERVERS[@]}"; do
        echo "        server ${backend} max_fails=3 fail_timeout=15s;"
    done

    cat <<'EOF'
        keepalive 64;
    }

EOF

    cat <<'EOF'
    upstream binance_futures_nodes {
        zone binance_futures_nodes 128k;
EOF

    for backend in "${BACKEND_FUTURES_SERVERS[@]}"; do
        echo "        server ${backend} max_fails=3 fail_timeout=15s;"
    done

    cat <<'EOF'
        keepalive 64;
    }

EOF

    cat <<EOF
    resolver ${RESOLVERS} valid=300s ipv6=off;
    resolver_timeout 5s;

    client_body_temp_path temp/client;
    proxy_temp_path       temp/proxy;

    server {
        listen       ${SPOT_PORT};
        listen       [::]:${SPOT_PORT};
        server_name  _;

        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header X-Request-ID \$request_id;

        location = /healthz {
            return 204;
        }

        location / {
            proxy_pass http://binance_spot_nodes;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Host \$host;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 30s;
            proxy_connect_timeout 5s;
        }

    }

    server {
        listen       ${FUTURES_PORT};
        listen       [::]:${FUTURES_PORT};
        server_name  _;

        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header X-Request-ID \$request_id;

        location = /healthz {
            return 204;
        }

        location /futures/data/globalLongShortAccountRatio {
            proxy_pass http://binance_futures_nodes;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Host \$host;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 30s;
            proxy_connect_timeout 5s;
        }

        location / {
            proxy_pass http://binance_futures_nodes;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Host \$host;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 30s;
            proxy_connect_timeout 5s;
        }

    }
}
EOF
} > "${CONFIG_FILE}"

echo "[deploy_binance_gateway] Nginx configured at ${INSTALL_PREFIX}"
echo "[deploy_binance_gateway] Launch with: ${INSTALL_PREFIX}/sbin/nginx -p ${INSTALL_PREFIX} -c conf/nginx.conf"
echo "[deploy_binance_gateway] Reload with: ${INSTALL_PREFIX}/sbin/nginx -p ${INSTALL_PREFIX} -s reload"
