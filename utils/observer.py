from typing import List
from datetime import datetime
from abc import ABC, abstractmethod
from cache.cache import cache_latest_data
from influxdb.influx import influxdb_write_json_data

class Observer(ABC):
    @abstractmethod
    def save(self, datas: List[dict], **kwargs) -> None:
        pass

class AbstractPublisher(ABC):

    @abstractmethod
    def attach(self, observer: Observer) -> None:
        pass

    @abstractmethod
    def detach(self, observer: Observer) -> None:
        pass

    @abstractmethod
    def notify(self, datas: List[dict], **kwargs) -> None:
        pass


class Publisher(AbstractPublisher):
    _observers: List[Observer] = []
    def __init__(self) -> None:
        self._observers = []

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self, datas: List[dict], **kwargs) -> None:
        for observer in self._observers:
            observer.save(self, datas, **kwargs)

class InhandCacheObserver(Observer):
    def save(self, datas: List[dict], **kwargs) -> None:
        for data in datas:
            key_cache = f'{data["tags"]["mac_address"]}-{data["tags"]["slave"]}-{data["tags"]["register"]}'
            value = {
                "raw_data": data["fields"]["raw_data"],
                "timestamp": datetime.utcnow().strftime("%d/%m/%Y, %H:%M:%S")
            }
            cache_latest_data(key_cache, value)

class CacheObserver(Observer):
    def save(self, datas: List[dict], **kwargs) -> None:
        for data in datas:
            key_cache = data["tags"]["mac_address"]
            value = {
                "raw_data": data["fields"],
                "timestamp": datetime.utcnow().strftime("%d/%m/%Y, %H:%M:%S")
            }
            cache_latest_data(key_cache, value)

class InfluxObserver(Observer):
    def save(self, datas: List[dict], **kwargs) -> None:
        influxdb_write_json_data(datas, **kwargs)