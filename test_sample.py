def add(a, b):
    return a + b


def sub(a, b):
    return a - b


def div(a, b):
    return a / b


def mul(a, b):
    return a * b


def test_add():
    assert add(2, 2) == 4


def test_sub():
    assert sub(2, 2) == 0


def test_div():
    assert div(2, 2) == 1


def test_mul():
    assert mul(2, 2) == 4
