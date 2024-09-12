import faust
from datetime import datetime
import pytz
class StockData(faust.Record):
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
topic = app.topic('kafka_MA_data', value_type=StockData)
windowed_table = app.Table(
    'windowed_stock_table',
    default=lambda: AggregatedData(
        symbol='', type='', MA_type='', start='', end='',
        current_time='', sma_value=0.0, sum_of_vwap=0.0, 
        count_of_vwap=0, window_data_count=0, 
        real_data_count=0, filled_data_count=0
    ),    # partitions=
    ).tumbling(size=60)  # , step=30

aggregated_topic = app.topic('kafka_MA_data_aggregated', value_type=AggregatedData)

@app.agent(topic)
async def process(stream):
    async for stock_data in stream.group_by(lambda x: f"{x.symbol}_{x.MA_type}", name="grouped_by_symbol_MA"):
        # 這步驟是建立鍵質
        key = (stock_data.symbol, stock_data.type, stock_data.MA_type, stock_data.start, stock_data.end)
        existing = windowed_table[key].value()  ## 這是之前存在的資料
        
        if existing is None: ## 如果是空的。把stock_data拉進去放
            windowed_table[key] = AggregatedData(
                symbol=stock_data.symbol,
                type=stock_data.type,
                MA_type=stock_data.MA_type,
                start=stock_data.start,
                end=stock_data.end,
                current_time=stock_data.current_time,
                sma_value=stock_data.sma_value,
                sum_of_vwap=stock_data.sum_of_vwap,
                count_of_vwap=stock_data.count_of_vwap,
                window_data_count=stock_data.window_data_count,
                real_data_count=stock_data.real_data_count,
                filled_data_count=stock_data.filled_data_count
            )
        else: ## 如果有。把stock_data加進去放
            new_sum_of_vwap = existing.sum_of_vwap + stock_data.sum_of_vwap
            new_count_of_vwap = existing.count_of_vwap + stock_data.count_of_vwap
            new_sma_value = new_sum_of_vwap / new_count_of_vwap if new_count_of_vwap != 0 else 0

            windowed_table[key] = AggregatedData(
                symbol=stock_data.symbol,
                type=stock_data.type,
                MA_type=stock_data.MA_type,
                start=stock_data.start,
                end=stock_data.end,
                current_time=stock_data.current_time,
                sma_value=new_sma_value,
                sum_of_vwap=new_sum_of_vwap,
                count_of_vwap=new_count_of_vwap,
                window_data_count=existing.window_data_count + stock_data.window_data_count,
                real_data_count=existing.real_data_count + stock_data.real_data_count,
                filled_data_count=existing.filled_data_count + stock_data.filled_data_count
            )
        aggregated_data = windowed_table[key].value()
        if aggregated_data:
            await aggregated_topic.send(value=aggregated_data)
            print("data sended")

if __name__ == '__main__':
    app.main()
