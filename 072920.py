from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data.morningstar import Fundamentals
from quantopian.pipeline.filters.morningstar import Q500US
from quantopian.pipeline import CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing
import pandas as pd

from quantopian.pipeline.filters import QTradableStocksUS


class momentum_factor(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 20

    def compute(self, today, assets, out, close):
        out[:] = close[-1] / close[0]


def initialize(context):
    schedule_function(rebalance,
                      date_rules.month_end(),
                      time_rules.market_close())

    # Create our dynamic stock selector.
    pipe = make_pipeline()
    attach_pipeline(make_pipeline(), 'my_pipeline')
    factor1 = momentum_factor()
    pipe.add(factor1, 'factor_1')

    ## Record tracking variables at the end of each day.
    schedule_function(
        record_vars,
        date_rules.every_day(),
        time_rules.market_close(),
    )


def make_pipeline():
    # Base Universe.
    base_universe = QTradableStocksUS()

    # Revenue Growth.
    revenue_growth = morningstar.operation_ratios.revenue_growth.latest
    revenue = revenue_growth > 0.3
    market_value = morningstar.valuation.market_cap.latest

    mv = morningstar.valuation.market_cap.latest.top(20, mask=revenue)

    # mv = market_value.top(20,mask=base_universe)

    # Setting filters.
    securities_of_high_growth = (mv)

    # Return pipe:
    return Pipeline(
        columns={
            'Revenue Growth': revenue_growth,
            'Market Value': market_value,
            'Top Growth Company': mv,
            'price': USEquityPricing.close.latest,
        },
        screen=(securities_of_high_growth & base_universe)
    )


def before_trading_start(context, data):
    """
    Setting long positions.
    """
    context.output = pipeline_output('my_pipeline')

    # List of short positions.
    context.longs = context.output[
        context.output['Top Growth Company']
    ].index

    # Gathering weights from assign_weights().
    context.long_weight = assign_weights(context)


def getWeight(df, stock):
    weight = pd.Series(
        (1 / df['price'].sum()
         ) * df.loc[stock, 'price'],
        index=[stock]
    )
    return weight


def assign_weights(context):
    """
    Assign weights to securities that we want to order.
    """
    df = pd.DataFrame(
        data=context.output,
        columns=[
            'price',
        ],
        index=context.longs
    )
    # Set long weights.
    weights = pd.Series()
    for stock in df.index.tolist():
        weight = getWeight(df, stock)
        weights = weights.append(weight)

    return weights


def rebalance(context, data):
    """
    Rebalance weights per monthly while exiting all positions at the start of every rebalance.
    """
    df = pd.DataFrame(
        context.output,
        columns=[
            "Revenue Growth",
            "Market Value",
            "Top Growth Company",
            "price",
        ]
    ).sort_values(["Market Value",
                   "Revenue Growth"],
                  ascending=False)
    print("df=\n{}".format(df))
    log.info(context.long_weight)

    # Exit positions not in either long or shorts.
    for security in context.portfolio.positions:
        if security not in context.longs and data.can_trade(security):
            order_target_percent(security, 0)

    # Set long position orders.
    for security in context.longs:
        if data.can_trade(security):
            order_target_percent(security, context.long_weight[security])

    record(
        leverage=context.account.leverage,
        positions_len=len(context.portfolio.positions),
        cash=context.portfolio.cash,
        positions=context.portfolio.positions_value,
    )


def record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    record(
        leverage=context.account.leverage,
        positions_len=len(context.portfolio.positions),
        cash=context.portfolio.cash,
        positions=context.portfolio.positions_value,
    )
    # weight_objective = opt.TargetWeights(context.long_weight)
    # # Execute the order_optimal_portfolio method with above objective and constraint
    # order_optimal_portfolio(objective = weight_objective, constraints = [])