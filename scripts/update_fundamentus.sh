#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

if [[ -f .venv/bin/activate ]]; then
  # Use the local virtualenv when the script is executed directly by cron.
  source .venv/bin/activate
fi

python -m scripts.get_fundamentus_data

python -m scripts.performance

git add .
git diff --cached --quiet && exit 0

git commit -m "Dados da Fundamentus atualizados"
git push
