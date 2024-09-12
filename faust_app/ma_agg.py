import faust
from datetime import datetime

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
aggregated_topic = app.topic('kafka_MA_data_aggregated', value_type=AggregatedData)

windowed_table = app.Table(
    'windowed_stock_table',
    default=AggregatedData,
    partitions=
).hopping(size=60, step=30) 

@app.agent(topic)
async def process(stream):
    # async for stock_data in stream.group_by(lambda x: f"{x.symbol}_{x.MA_type}"):
    async for stock_data in stream.group_by(lambda x: f"{x.symbol}_{x.MA_type}",name="grouped_by_symbol_MA"):
        key = (stock_data.symbol, stock_data.type, stock_data.MA_type, stock_data.start, stock_data.end)
        if key not in windowed_table:
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
        else:
            existing = windowed_table[key]

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
        
        # if windowed_table[key].current_window_expires < datetime.utcnow().timestamp():
        #     await aggregated_topic.send(value=windowed_table[key]) ## 這邊的會是WindowSet物件
            
        if windowed_table[key].now() < datetime.utcnow().timestamp():
            await aggregated_topic.send(value=windowed_table[key])


if __name__ == '__main__':
    app.main()
