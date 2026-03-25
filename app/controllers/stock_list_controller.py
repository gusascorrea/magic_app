from app.services.stock_selector_service import build_investment_table


def get_investment_table(strategy, portfolio_size, total_investment, minimum_volume):
    return build_investment_table(
        strategy=strategy,
        portfolio_size=portfolio_size,
        total_investment=total_investment,
        minimum_volume=minimum_volume,
    )
