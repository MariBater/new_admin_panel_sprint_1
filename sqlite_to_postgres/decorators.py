import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до
    граничного времени ожидания (border_sleep_time).

    Формула:
        t = start_sleep_time * (factor ** n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """
    # Convert parameters to numeric types immediately.
    _start_sleep_time = float(start_sleep_time)
    _factor = float(factor)
    _border_sleep_time = float(border_sleep_time)

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in {func.__name__}: {e}. Retrying...")
                    sleep_time = _start_sleep_time * (_factor ** n)
                    current_sleep_time = min(sleep_time, _border_sleep_time)
                    logger.info(f"Next retry in {current_sleep_time:.2f} seconds.")
                    time.sleep(current_sleep_time)
                    n += 1
        return inner
    return func_wrapper