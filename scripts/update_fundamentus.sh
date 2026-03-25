#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
LOG_FILE="${LOG_DIR}/fundamentus.log"

cd "${REPO_ROOT}"

if [[ -f "${REPO_ROOT}/http_cache.sqlite" ]]; then
  rm "${REPO_ROOT}/http_cache.sqlite"
fi

mkdir -p "${LOG_DIR}"

timestamp_output() {
  while IFS= read -r line || [[ -n "${line}" ]]; do
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S%z')" "${line}"
  done
}

exec > >(timestamp_output >> "${LOG_FILE}") 2>&1

# Cron usually runs with a minimal PATH, so include common user-local bin paths.
export PATH="${HOME}/.local/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"

echo "Iniciando atualizacao da Fundamentus"

supports_project_dependencies() {
  local candidate="$1"
  "${candidate}" -c "import fundamentus, numpy, pandas, pyarrow, yfinance" >/dev/null 2>&1
}

PYTHON_BIN=""

if [[ -x .venv/bin/python ]] && supports_project_dependencies "${REPO_ROOT}/.venv/bin/python"; then
  PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
fi

if [[ -z "${PYTHON_BIN}" ]] && command -v poetry >/dev/null 2>&1; then
  POETRY_ENV_PATH="$(poetry env info --path 2>/dev/null || true)"
  if [[ -n "${POETRY_ENV_PATH}" ]] && [[ -x "${POETRY_ENV_PATH}/bin/python" ]] && supports_project_dependencies "${POETRY_ENV_PATH}/bin/python"; then
    PYTHON_BIN="${POETRY_ENV_PATH}/bin/python"
  fi
fi

if [[ -z "${PYTHON_BIN}" ]] && command -v python3 >/dev/null 2>&1 && supports_project_dependencies "$(command -v python3)"; then
  PYTHON_BIN="$(command -v python3)"
fi

if [[ -z "${PYTHON_BIN}" ]] && command -v python >/dev/null 2>&1 && supports_project_dependencies "$(command -v python)"; then
  PYTHON_BIN="$(command -v python)"
fi

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "Nenhum interpretador Python com as dependencias do projeto foi encontrado." >&2
  exit 1
fi

"${PYTHON_BIN}" -c "import sys; print(f'Python selecionado: {sys.executable}')"

"${PYTHON_BIN}" -m scripts.get_fundamentus_data

"${PYTHON_BIN}" -m scripts.update_market_benchmarks

"${PYTHON_BIN}" -m scripts.performance

"${PYTHON_BIN}" -m scripts.build_performance_history_parquet --include-working-tree

if [[ -f "${REPO_ROOT}/http_cache.sqlite" ]]; then
  rm "${REPO_ROOT}/http_cache.sqlite"
fi

# git add .
# git diff --cached --quiet && exit 0

# git commit -m "Dados da Fundamentus atualizados"
# git push

echo "Atualizacao concluida"
echo "######################################################################"
