import random

from functools import wraps
from time import sleep


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def generate_numbers(target):
    while True:
        value = random.randint(1, 11)
        target.send(value)
        sleep(0.1)


@coroutine
def double_odd(target):
    while value := (yield):
        if value % 2 != 0:
            value = value ** 2
        target.send(value)


@coroutine
def halve_even(target):
    while value := (yield):
        if value % 2 == 0:
            value = value // 2
        target.send(value)


@coroutine
def print_sum():
    buf = []
    while value := (yield):
        buf.append(value)
        if len(buf) == 10:
            print(sum(buf))
            buf.clear()


printer_sink = print_sum()
even_filter = halve_even(printer_sink)
odd_filter = double_odd(even_filter)
generate_numbers(odd_filter)
