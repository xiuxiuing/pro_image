#!/usr/bin/env bash
# 在「云服务器」上执行（Ubuntu 22.04/24.04 示例）。需要 sudo。
# 用法：sudo bash deploy/install-server.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/pro_image}"
DOMAIN="${DOMAIN:-}"
PG_DB="${PG_DB:-pro_image}"
PG_USER="${PG_USER:-pro_image}"
PG_PASSWORD="${PG_PASSWORD:-change-me}"

echo "==> 依赖：git python3-venv nginx redis postgresql certbot"
apt-get update -y
apt-get install -y git python3 python3-venv python3-pip nginx redis-server postgresql postgresql-client certbot python3-certbot-nginx

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "请将仓库克隆到 $APP_DIR，例如："
  echo "  sudo git clone <你的仓库URL> $APP_DIR"
  exit 1
fi

cd "$APP_DIR"
python3 -m venv venv
# shellcheck source=/dev/null
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r deploy/requirements-deploy.txt

chown -R www-data:www-data "$APP_DIR"

echo "==> PostgreSQL: 创建数据库和用户（如已存在则跳过）"
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='${PG_USER}'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE USER ${PG_USER} WITH PASSWORD '${PG_PASSWORD}'"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='${PG_DB}'" | grep -q 1 || \
  sudo -u postgres createdb -O "${PG_USER}" "${PG_DB}"
sudo -u postgres psql -c "ALTER DATABASE ${PG_DB} OWNER TO ${PG_USER}"

echo "==> systemd: 复制 Web/Worker service 到 /etc/systemd/system/ 并 daemon-reload"
cp -f "$APP_DIR/deploy/pro-image.service" /etc/systemd/system/pro-image.service
cp -f "$APP_DIR/deploy/pro-image-worker.service" /etc/systemd/system/pro-image-worker.service
DATABASE_URL_VALUE="postgresql+psycopg://${PG_USER}:${PG_PASSWORD}@127.0.0.1:5432/${PG_DB}"
sed -i "s#^Environment=\"DATABASE_URL=.*#Environment=\"DATABASE_URL=${DATABASE_URL_VALUE}\"#g" /etc/systemd/system/pro-image.service
sed -i "s#^Environment=\"DATABASE_URL=.*#Environment=\"DATABASE_URL=${DATABASE_URL_VALUE}\"#g" /etc/systemd/system/pro-image-worker.service
systemctl daemon-reload
systemctl enable pro-image
systemctl enable pro-image-worker
systemctl restart pro-image
systemctl restart pro-image-worker

echo "==> nginx"
cp -f "$APP_DIR/deploy/nginx-site.conf.example" /etc/nginx/sites-available/pro-image
if [[ -n "$DOMAIN" ]]; then
  sed -i "s/your-domain.example.com/$DOMAIN/g" /etc/nginx/sites-available/pro-image
fi
ln -sf /etc/nginx/sites-available/pro-image /etc/nginx/sites-enabled/pro-image
nginx -t && systemctl reload nginx

echo "==> 若已设置 DOMAIN 环境变量且 DNS 已指向本机，可签发证书："
echo "    sudo certbot --nginx -d $DOMAIN"
echo ""
echo "未设置 DOMAIN 时：编辑 /etc/nginx/sites-available/pro-image 中的 server_name，"
echo "然后: sudo certbot --nginx -d 你的域名"
echo ""
systemctl --no-pager status pro-image || true
systemctl --no-pager status pro-image-worker || true
