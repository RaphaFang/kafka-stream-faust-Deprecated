import faust
from datetime import datetime, timedelta

class StockData(faust.Record):
    symbol: str
    type: str
    start: str
    end: str
    current_time: str
    last_data_time: str
    real_data_count: int
    filled_data_count: int
    real_or_filled: str
    vwap_price_per_sec: float
    size_per_sec: int
    volume_till_now: float
    yesterday_price: float
    price_change_percentage: float

class AggregatedData(faust.Record):
    symbol: str
    type: str
    MA_type: str
    start: str
    end: str
    current_time: str
    sma_value: float
    sum_of_vwap: float
    count_of_vwap: int
    window_data_count: int
    real_data_count: int
    filled_data_count: int

app = faust.App('stock-ma-data-aggregator',
                broker='kafka://10.0.1.138:9092',
                web_port=6066,
                concurrency=2
            )
topic = app.topic('kafka_per_sec_data', value_type=StockData)
windowed_table = app.Table(
    'windowed_stock_table',
    default=lambda: AggregatedData(
        symbol='', type='', MA_type='', start='', end='',
        current_time='', sma_value=0.0, sum_of_vwap=0.0, 
        count_of_vwap=0, window_data_count=0, 
        real_data_count=0, filled_data_count=0
    )
).hopping(size=5, step=1)

aggregated_topic = app.topic('kafka_MA_data_aggregated', value_type=AggregatedData)
@app.agent(topic)
async def process(stream):
    async for stock_data in stream.group_by(lambda x: f"{x.symbol}", name="grouped_by_symbol"):
        key = (stock_data.symbol,)
        existing = windowed_table[key].value()

        window_start = datetime.fromisoformat(stock_data.current_time)
        window_end = datetime.fromisoformat(stock_data.current_time) + timedelta(seconds=5)


        if existing is None:
            sum_vwap = stock_data.vwap_price_per_sec if stock_data.size_per_sec != 0 else 0
            count_vwap = 1 if stock_data.size_per_sec != 0 else 0

            windowed_table[key] = AggregatedData(
                symbol=stock_data.symbol,
                type=stock_data.type,
                MA_type='5_MA_data',

                start=window_start.isoformat(),
                end=window_end.isoformat(), 
                current_time=datetime.utcnow().isoformat(),

                sma_value=0.0,
                sum_of_vwap=sum_vwap,
                count_of_vwap=count_vwap,
                window_data_count=1,

                real_data_count=1 if stock_data.real_or_filled == 'real' else 0,
                filled_data_count=1 if stock_data.real_or_filled != 'real' else 0
            )
        else:
            new_sum_vwap = existing.sum_of_vwap + (stock_data.vwap_price_per_sec if stock_data.size_per_sec != 0 else 0)
            new_count_vwap = existing.count_of_vwap + (1 if stock_data.size_per_sec != 0 else 0)
            new_window_data_count = existing.window_data_count + 1
            new_sma_value = new_sum_vwap / new_count_vwap if new_count_vwap != 0 else 0

            windowed_table[key] = AggregatedData(
                symbol=stock_data.symbol,
                type=stock_data.type,
                MA_type='5_MA_data',

                start=existing.start,
                end=window_end.isoformat(),
                current_time=datetime.utcnow().isoformat(),

                sma_value=new_sma_value,
                sum_of_vwap=new_sum_vwap,
                count_of_vwap=new_count_vwap,
                window_data_count=new_window_data_count,

                real_data_count=existing.real_data_count + stock_data.real_data_count,
                filled_data_count=existing.filled_data_count + stock_data.filled_data_count
            )

        # if window_end.isoformat() == datetime.utcnow().isoformat():
        if windowed_table[key].value().window_data_count >= 5:
            aggregated_data = windowed_table[key].value()
            if aggregated_data:
                await aggregated_topic.send(value=aggregated_data)
                print("data sended")

if __name__ == '__main__':
    app.main()


# --------------------------------------------------------------------------------------------------------
# import faust
# from datetime import datetime
# import pytz
# class StockData(faust.Record):
#     symbol: str
#     type: str
#     MA_type: str
#     start: str
#     end: str
#     current_time: str
#     sma_value: float
#     sum_of_vwap: float
#     count_of_vwap: int
#     window_data_count: int
#     real_data_count: int
#     filled_data_count: int

# class AggregatedData(faust.Record):
#     symbol: str
#     type: str
#     MA_type: str
#     start: str
#     end: str
#     current_time: str
#     sma_value: float
#     sum_of_vwap: float
#     count_of_vwap: int
#     window_data_count: int
#     real_data_count: int
#     filled_data_count: int

# app = faust.App('stock-ma-data-aggregator',
#                 broker='kafka://10.0.1.138:9092',
#                 web_port=6066,
#                 concurrency=2
#             )
# topic = app.topic('kafka_per_sec_data', value_type=StockData)
# windowed_table = app.Table(
#     'windowed_stock_table',
#     default=lambda: AggregatedData(
#         symbol='', type='', MA_type='', start='', end='',
#         current_time='', sma_value=0.0, sum_of_vwap=0.0, 
#         count_of_vwap=0, window_data_count=0, 
#         real_data_count=0, filled_data_count=0
#     ),    # partitions=
#     ).tumbling(size=35) # , step=30

# aggregated_topic = app.topic('kafka_MA_data_aggregated', value_type=AggregatedData)

# @app.agent(topic)
# async def process(stream):
#     async for stock_data in stream.group_by(lambda x: f"{x.symbol}_{x.MA_type}", name="grouped_by_symbol_MA"):
#         # 這步驟是建立鍵質
#         key = (stock_data.symbol, stock_data.type, stock_data.MA_type, stock_data.start, stock_data.end)
#         existing = windowed_table[key].value()  ## 這是之前存在的資料
        
#         if existing is None: ## 如果是空的。把stock_data拉進去放
#             windowed_table[key] = AggregatedData(
#                 symbol=stock_data.symbol,
#                 type=stock_data.type,
#                 MA_type=stock_data.MA_type,
#                 start=stock_data.start,
#                 end=stock_data.end,
#                 current_time=stock_data.current_time,
#                 sma_value=stock_data.sma_value,
#                 sum_of_vwap=stock_data.sum_of_vwap,
#                 count_of_vwap=stock_data.count_of_vwap,
#                 window_data_count=stock_data.window_data_count,
#                 real_data_count=stock_data.real_data_count,
#                 filled_data_count=stock_data.filled_data_count
#             )
#         else: ## 如果有。把stock_data加進去放
#             new_sum_of_vwap = existing.sum_of_vwap + stock_data.sum_of_vwap
#             new_count_of_vwap = existing.count_of_vwap + stock_data.count_of_vwap
#             new_sma_value = new_sum_of_vwap / new_count_of_vwap if new_count_of_vwap != 0 else 0

#             windowed_table[key] = AggregatedData(
#                 symbol=stock_data.symbol,
#                 type=stock_data.type,
#                 MA_type=stock_data.MA_type,
#                 start=stock_data.start,
#                 end=stock_data.end,
#                 current_time=stock_data.current_time,
#                 sma_value=new_sma_value,
#                 sum_of_vwap=new_sum_of_vwap,
#                 count_of_vwap=new_count_of_vwap,
#                 window_data_count=existing.window_data_count + stock_data.window_data_count,
#                 real_data_count=existing.real_data_count + stock_data.real_data_count,
#                 filled_data_count=existing.filled_data_count + stock_data.filled_data_count
#             )
#         aggregated_data = windowed_table[key].value()
#         if aggregated_data:
#             await aggregated_topic.send(value=aggregated_data)
#             print("data sended")

# if __name__ == '__main__':
#     app.main()
