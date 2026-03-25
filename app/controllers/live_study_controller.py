from app.services.reallocation_service import build_live_reallocation_analysis


def get_live_study_view_model():
    analysis = build_live_reallocation_analysis()
    configuration_summary = analysis["configuration_summary"]
    best_configuration = configuration_summary.iloc[0]
    worst_configuration = configuration_summary.sort_values(
        [
            "razao_q1_cagr_q1_drawdown",
            "cagr_q1",
            "drawdown_q1",
            "vitorias_por_periodo",
        ],
        ascending=[True, True, True, True],
    ).iloc[0]

    return {
        "analysis": analysis,
        "best_configuration": best_configuration,
        "worst_configuration": worst_configuration,
    }
