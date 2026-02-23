#!/bin/bash

echo "=== GPS Service Restart Script ==="

# 1. Остановка gpsd
echo "[1] Stopping gpsd..."
sudo systemctl stop gpsd.socket 2>/dev/null
sudo systemctl stop gpsd 2>/dev/null

# 2. Убиваем возможные зависшие процессы
echo "[2] Killing leftover gpsd processes..."
sudo killall -9 gpsd 2>/dev/null

# 3. Проверка GPS устройства
echo "[3] Detecting GPS device..."
GPS_DEVICE=$(ls /dev/ttyUSB* /dev/ttyACM* 2>/dev/null | head -n 1)

if [ -z "$GPS_DEVICE" ]; then
    echo "⚠ GPS device not found in /dev/"
else
    echo "GPS device detected: $GPS_DEVICE"
fi

# 4. Перезапуск bluetooth (если используется)
echo "[4] Restarting Bluetooth service..."
sudo systemctl restart bluetooth 2>/dev/null

# 5. Запуск gpsd
echo "[5] Starting gpsd..."
if [ -n "$GPS_DEVICE" ]; then
    sudo gpsd $GPS_DEVICE -F /var/run/gpsd.sock
else
    sudo systemctl start gpsd.socket
    sudo systemctl start gpsd
fi

sleep 2

# 6. Проверка статуса
echo "[6] Service status:"
sudo systemctl status gpsd --no-pager
echo
echo "=== Done ==="