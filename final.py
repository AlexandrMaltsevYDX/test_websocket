from abc import ABC, abstractmethod
import asyncio
import json
import websockets
import time
from pydantic import dataclasses
from typing import Any, Deque
import collections
import pandas as pd


def int_format(digital):
    return "{:.2f}".format(float(digital))


bitfinex_currency_pair = {
    "btcusdt": "tBTCUSD",
}

binance_currency_pair = {
    "btcusdt": "btcusdt",
}


@dataclasses.dataclass
class KlineData:
    platform: str
    event_time: int
    kline_start_time: int
    kline_close_time: int
    open: float
    close: float
    high: float
    low: float
    volume: float
    is_closed: bool
    vwap: Any
    rsi: Any

    def __str__(self):
        return (
            f"platform={self.platform}, "
            f"event_time={self.event_time}, "
            f"kline_start_time={self.kline_start_time}, "
            f"kline_close_time={self.kline_close_time}, "
            f"open={self.open}, close={self.close}, "
            f"high={self.high}, low={self.low}, "
            f"volume={self.volume}, is_closed={self.is_closed}, "
            f"vwap={self.vwap}, rsi={self.rsi}, "
        )


class Provider(ABC):
    def __init__(self):
        pass

    @property
    @abstractmethod
    def get_platform_name(self):
        return self._platform

    @property
    @abstractmethod
    def socket(self):
        pass

    @abstractmethod
    async def _parse(self):
        pass

    @abstractmethod
    async def _on_open(self):
        pass

    @abstractmethod
    async def _on_message(self):
        pass

    async def _on_error(self, error):
        print(error)

    async def _on_close(self, message="the end"):
        print(message)

    async def run_stream(self):
        try:
            async with websockets.connect(self.socket) as ws:
                await self._on_open(ws)
                while True:
                    message = await ws.recv()
                    await self._on_message(message)
        except Exception as error:
            await self._on_error(error)

        finally:
            await self._on_close()


class Kline(ABC):
    def __init__(self, symbol: str, timeframe: str):
        self._symbol = symbol
        self._timeframe = timeframe

    @property
    def symbol(self):
        return self._symbol

    @property
    def timeframe(self):
        return self._timeframe


class BitfinexKlineProvider(Provider, Kline):
    def __init__(self, symbol, timeframe):
        super(Provider).__init__()
        Kline.__init__(self, symbol, timeframe)

        self._VWAP = {
            "volume_x_typical_price": 0,
            "volume": 0,
        }

    def vwap(self, volume: int, high: int, open: int, close: int):
        typical_price = (high + open + close) / 3
        volume_x_typical_price = volume * typical_price

        self._VWAP["volume_x_typical_price"] += volume_x_typical_price
        self._VWAP["volume"] += volume

        return self._VWAP["volume_x_typical_price"] / self._VWAP["volume"]

    @property
    def get_platform_name(self):
        return "bitfinex"

    @property
    def socket(self):
        return "wss://api-pub.bitfinex.com/ws/2/"

    async def _on_open(self, ws):
        pair_value = bitfinex_currency_pair[self.symbol]
        key = f"trade:{self.timeframe}:{pair_value}"
        send_message = {
            "event": "subscribe",
            "channel": "candles",
            "key": key,
        }
        await ws.send(json.dumps(send_message))

    def _parse(
        self,
        data: dict,
        event_time_microseconds: int,
    ) -> KlineData:
        if event_time_microseconds > (data[1][0] + 59999):
            return KlineData(
                platform=self.get_platform_name,
                event_time=event_time_microseconds,
                kline_start_time=data[1][0],
                kline_close_time=data[1][0] + 59999,
                open=int_format(data[1][1]),
                close=int_format(data[1][2]),
                high=int_format(data[1][3]),
                low=int_format(data[1][4]),
                volume=int_format(data[1][5]),
                is_closed=event_time_microseconds > (data[1][0] + 59999),
                vwap=self.vwap(
                    volume=data[1][5],
                    high=data[1][3],
                    open=data[1][1],
                    close=data[1][2],
                ),
                rsi=None,
            )
        return False

    async def _on_message(self, message):
        event_time_microseconds = int(time.time() * 1e3)
        data = json.loads(message)
        # print(message)
        if (
            isinstance(data, dict)
            or data[1] == "hb"
            or len(data) != 2
            or len(data[1]) > 6
        ):
            return

        parsed_data = self._parse(data, event_time_microseconds)
        if parsed_data:
            print(parsed_data)


class BinanceKlineProvider(Provider, Kline):
    def __init__(self, symbol, timeframe, length=14):
        super(Provider).__init__()
        Kline.__init__(self, symbol, timeframe)
        self.length = length

        self._window: Deque[Any] = collections.deque(
            [
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ],
            maxlen=length,
        )

    def rsi(self, close: int, period: int) -> float:
        self._window.append(close)
        # Преобразование очереди в Series
        series_data = pd.Series(self._window, dtype=float)

        # Вычисление изменений цен
        price_change = series_data.diff(1)

        # Вычисление средних приростов и средних убытков
        positive_change = price_change.apply(lambda x: x if x > 0 else 0)
        negative_change = -price_change.apply(lambda x: x if x < 0 else 0)
        average_gain = positive_change.rolling(window=period).mean()
        average_loss = negative_change.rolling(window=period).mean()

        # Вычисление RS и RSI серии
        rs = average_gain / average_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi.iloc[-1]

    @property
    def get_platform_name(self):
        return "binance"

    @property
    def socket(self):
        return (
            "wss://stream.binance.com:9443/ws/"
            f"{binance_currency_pair[self.symbol]}@kline_{self.timeframe}"
        )

    async def _on_open(self, ws):
        pass

    def _parse(
        self,
        data: dict,
        event_time_microseconds: int,
    ) -> KlineData:
        # print(data)
        kline_data = data["k"]
        if kline_data["x"]:
            return KlineData(
                platform=self.get_platform_name,
                event_time=event_time_microseconds,
                kline_start_time=kline_data["t"],
                kline_close_time=kline_data["T"],
                open=int_format(kline_data["o"]),
                close=int_format(kline_data["c"]),
                high=int_format(kline_data["h"]),
                low=int_format(kline_data["l"]),
                volume=int_format(kline_data["v"]),
                is_closed=kline_data["x"],
                vwap=None,
                rsi=self.rsi(close=kline_data["c"], period=self.length),
            )
        return False

    async def _on_message(self, message):
        data = json.loads(message)
        event_time_microseconds = data["E"]
        # print(message)
        if not ("e" in data and data["e"] == "kline"):
            return

        parsed_data = self._parse(data, event_time_microseconds)
        if parsed_data:
            print(parsed_data)


async def main():
    binance_provider = BinanceKlineProvider("btcusdt", "5m")
    bitfinex_provider = BitfinexKlineProvider("btcusdt", "1m")

    tasks = [
        asyncio.create_task(binance_provider.run_stream()),
        asyncio.create_task(bitfinex_provider.run_stream()),
    ]

    try:
        await asyncio.gather(*tasks)
    except Exception:
        print("KeyboardInterrupt received. Stopping all providers.")


if __name__ == "__main__":
    asyncio.run(main())
