from pytest import fixture

from rhubarb.event import Event


@fixture
def event():
    return Event("test-channel", "mock data")


class TestEvent:
    def test_event_creation(self):
        event = Event("test-channel", "mock data")
        assert event.channel == "test-channel"
        assert event.message == "mock data"

    def test_event_comparison_equal(self):
        event_1 = Event("test-channel", "mock data")
        event_2 = Event("test-channel", "mock data")
        assert event_1 == event_2
        assert event_1 is not event_2

    def test_event_comparison_not_equal(self):
        event_1 = Event("test-channel", "mock data")
        event_2 = Event("test-channel", "mock data 1")
        assert event_1 != event_2
        assert event_1 is not event_2

    def test_event_repr(self, event):
        assert event.__repr__() == "Event('test-channel', 'mock data')"
