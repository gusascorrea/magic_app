from app.services.reallocation_service import build_live_reallocation_analysis


COMPARISON_OPTIONS = {
    "q1_cagr_q1_drawdown": {
        "label": "Q1 CAGR / Q1 drawdown",
        "description": "Favorece configurações com boa consistência no retorno e menor queda no quartil inferior.",
        "sort_by": [
            "razao_q1_cagr_q1_drawdown",
            "cagr_q1",
            "drawdown_q1",
            "vitorias_por_periodo",
        ],
        "ascending": [False, False, False, False],
    },
    "q1_cagr_drawdown_max": {
        "label": "Q1 CAGR / drawdown máximo",
        "description": "Prioriza CAGR mais resiliente, mas penaliza pela pior queda observada.",
        "sort_by": [
            "razao_q1_cagr_drawdown_pior",
            "cagr_q1",
            "drawdown_pior",
            "vitorias_por_periodo",
        ],
        "ascending": [False, False, False, False],
    },
    "q1_retorno_q1_drawdown": {
        "label": "Q1 retorno / Q1 drawdown",
        "description": "Compara o retorno total em cenários mais fracos contra o drawdown típico do quartil inferior.",
        "sort_by": [
            "razao_q1_retorno_q1_drawdown",
            "retorno_q1",
            "drawdown_q1",
            "vitorias_por_periodo",
        ],
        "ascending": [False, False, False, False],
    },
    "q1_retorno_drawdown_max": {
        "label": "Q1 retorno / drawdown máximo",
        "description": "Destaca configurações que mantêm retorno mesmo quando penalizadas pela pior queda histórica.",
        "sort_by": [
            "razao_q1_retorno_drawdown_pior",
            "retorno_q1",
            "drawdown_pior",
            "vitorias_por_periodo",
        ],
        "ascending": [False, False, False, False],
    },
    "cagr_medio": {
        "label": "CAGR médio",
        "description": "Ordena pela taxa anual composta média entre todas as datas iniciais testadas.",
        "sort_by": ["cagr_medio", "cagr_q1", "drawdown_q1", "vitorias_por_periodo"],
        "ascending": [False, False, False, False],
    },
    "retorno_medio": {
        "label": "Retorno médio",
        "description": "Ordena pelo retorno total médio acumulado entre as janelas testadas.",
        "sort_by": [
            "retorno_medio",
            "retorno_q1",
            "drawdown_q1",
            "vitorias_por_periodo",
        ],
        "ascending": [False, False, False, False],
    },
    "drawdown_medio": {
        "label": "Drawdown médio",
        "description": "Favorece as menores quedas médias; quanto mais próximo de zero, melhor.",
        "sort_by": ["drawdown_medio", "drawdown_q1", "cagr_q1", "vitorias_por_periodo"],
        "ascending": [False, False, False, False],
    },
}

DEFAULT_COMPARISON_KEY = "q1_cagr_q1_drawdown"


def get_live_study_comparison_options():
    return COMPARISON_OPTIONS


def get_live_study_view_model(comparison_key=DEFAULT_COMPARISON_KEY):
    analysis = build_live_reallocation_analysis()
    configuration_summary = analysis["configuration_summary"]
    comparison = COMPARISON_OPTIONS.get(
        comparison_key, COMPARISON_OPTIONS[DEFAULT_COMPARISON_KEY]
    )
    sorted_configuration_summary = configuration_summary.sort_values(
        comparison["sort_by"],
        ascending=comparison["ascending"],
    ).reset_index(drop=True)
    worst_configuration_summary = configuration_summary.sort_values(
        comparison["sort_by"],
        ascending=[not value for value in comparison["ascending"]],
    ).reset_index(drop=True)
    best_configuration = sorted_configuration_summary.iloc[0]
    worst_configuration = worst_configuration_summary.iloc[0]

    return {
        "analysis": analysis,
        "comparison_key": comparison_key,
        "comparison": comparison,
        "configuration_summary": sorted_configuration_summary,
        "best_configuration": best_configuration,
        "worst_configuration": worst_configuration,
    }
