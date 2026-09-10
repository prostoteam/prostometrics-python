from __future__ import annotations

from prostometrics._ring_buffer import RingBuffer


def test_refuses_new_items_instead_of_evicting_old_ones():
    buffer: RingBuffer[int] = RingBuffer(2)
    assert buffer.push(1) is True
    assert buffer.push(2) is True
    assert buffer.push(3) is False
    assert [buffer.shift(), buffer.shift(), buffer.shift()] == [1, 2, None]


def test_length_and_clear():
    buffer: RingBuffer[int] = RingBuffer(4)
    for item in range(3):
        buffer.push(item)
    assert len(buffer) == 3
    buffer.clear()
    assert len(buffer) == 0
    assert buffer.shift() is None
