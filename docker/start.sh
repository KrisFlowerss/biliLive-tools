#!/bin/sh

set -u

BACKEND_PID=""
CADDY_PID=""
UNHEALTHY_MARKER="${BILILIVE_TOOLS_UNHEALTHY_MARKER:-/tmp/bililive-tools-unhealthy}"

cleanup() {
    if [ -n "$BACKEND_PID" ] && kill -0 "$BACKEND_PID" 2>/dev/null; then
        kill "$BACKEND_PID" 2>/dev/null || true
    fi
    if [ -n "$CADDY_PID" ] && kill -0 "$CADDY_PID" 2>/dev/null; then
        kill "$CADDY_PID" 2>/dev/null || true
    fi
}

trap 'cleanup; exit 143' INT TERM

rm -f "$UNHEALTHY_MARKER"

# 启动后端服务
echo "Starting backend service..."
cd /app/backend
node index.cjs server --config config.json &

BACKEND_PID=$!

# 等待后端启动
echo "Waiting for backend to start..."
sleep 3

# 检查后端是否成功启动
if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    echo "Backend failed to start"
    exit 1
fi

echo "Backend started successfully (PID: $BACKEND_PID)"

# 启动 Caddy
echo "Starting Caddy server..."
caddy run --config /etc/caddy/Caddyfile &
CADDY_PID=$!

while true; do
    if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
        wait "$BACKEND_PID"
        BACKEND_STATUS=$?
        echo "Backend exited with status $BACKEND_STATUS, stopping container..."
        cleanup
        exit "$BACKEND_STATUS"
    fi

    if ! kill -0 "$CADDY_PID" 2>/dev/null; then
        wait "$CADDY_PID"
        CADDY_STATUS=$?
        echo "Caddy exited with status $CADDY_STATUS, stopping backend..."
        cleanup
        exit "$CADDY_STATUS"
    fi

    sleep 2
done
