from pytest import raises

from rhubarb.backends.base import BaseBackend


def test_methods_not_implemented():
    with raises(
        TypeError,
        match="Can't instantiate abstract class BaseBackend with abstract methods __init__, connect, disconnect, next_event, publish, subscribe, unsubscribe",
    ):
        BaseBackend()
