#!/usr/bin/env bash
# 在「云服务器」上执行（Ubuntu 22.04/24.04 示例）。需要 sudo。
# 用法：sudo bash deploy/install-server.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/pro_image}"
DOMAIN="${DOMAIN:-}"

echo "==> 依赖：git python3-venv nginx certbot python3-certbot-nginx"
apt-get update -y
apt-get install -y git python3 python3-venv python3-pip nginx certbot python3-certbot-nginx

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

echo "==> systemd: 复制 deploy/pro-image.service 到 /etc/systemd/system/ 并 daemon-reload"
cp -f "$APP_DIR/deploy/pro-image.service" /etc/systemd/system/pro-image.service
systemctl daemon-reload
systemctl enable pro-image
systemctl restart pro-image

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
