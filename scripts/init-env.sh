#!/usr/bin/env bash
# Create .env from .env.example with freshly generated local secrets. Safe to re-run (won't overwrite).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  echo ".env already exists — leaving it untouched."
  exit 0
fi

fernet=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')
jwt=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
ui_password=$(python3 -c 'import secrets; print(secrets.token_urlsafe(12))')

sed -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" \
    -e "s|^FERNET_KEY=.*|FERNET_KEY=${fernet}|" \
    -e "s|^AIRFLOW__API_AUTH__JWT_SECRET=.*|AIRFLOW__API_AUTH__JWT_SECRET=${jwt}|" \
    -e "s|^_AIRFLOW_WWW_USER_PASSWORD=.*|_AIRFLOW_WWW_USER_PASSWORD=${ui_password}|" \
    .env.example > .env
chmod 600 .env
echo "Created .env. Airflow UI login: airflow / ${ui_password}"
echo "Next: fill in the connection values for each source in .env."
