#!/usr/bin/env bash

# Build and configure a user-space Nginx that proxies Binance endpoints.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/lib_nginx_build.sh"

INSTALL_PREFIX="${INSTALL_PREFIX:-$HOME/nginx/binance-node}"
SPOT_PORT="${SPOT_PORT:-9080}"
FUTURES_PORT="${FUTURES_PORT:-9081}"
DEFAULT_RESOLVERS="1.1.1.1 8.8.8.8"
RESOLVERS="${RESOLVERS:-$DEFAULT_RESOLVERS}"
NGINX_VERSION="${NGINX_VERSION:-1.26.1}"

echo "[deploy_binance_node] Target prefix: ${INSTALL_PREFIX}"
ensure_nginx "${INSTALL_PREFIX}" "${NGINX_VERSION}"

mkdir -p "${INSTALL_PREFIX}/conf"
mkdir -p "${INSTALL_PREFIX}/logs"
mkdir -p "${INSTALL_PREFIX}/temp/client" "${INSTALL_PREFIX}/temp/proxy"

cat > "${INSTALL_PREFIX}/conf/nginx.conf" <<EOF
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
        listen       ${SPOT_PORT};
        listen       [::]:${SPOT_PORT};
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
        listen       ${FUTURES_PORT};
        listen       [::]:${FUTURES_PORT};
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

echo "[deploy_binance_node] Nginx configured at ${INSTALL_PREFIX}"
echo "[deploy_binance_node] Spot port: ${SPOT_PORT}, Futures port: ${FUTURES_PORT}"
echo "[deploy_binance_node] Launch with: ${INSTALL_PREFIX}/sbin/nginx -p ${INSTALL_PREFIX} -c conf/nginx.conf"
echo "[deploy_binance_node] Reload with: ${INSTALL_PREFIX}/sbin/nginx -p ${INSTALL_PREFIX} -s reload"
